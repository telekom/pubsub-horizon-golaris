package golaris

import (
	"context"
	"encoding/gob"
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/auth"
	"golaris/config"
	"golaris/health"
	"golaris/utils"
	"net/http"
	"time"
)

func init() {
	gob.Register(health.HealthCheck{})
}

func performHealthCheck(deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	// Specify HTTP method based on the subscription configuration
	httpMethod := "HEAD"
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = "GET"
	}

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)
	healthCheckData := buildHealthCheckData(subscription, httpMethod)

	// Attempt to acquire a lock for the health check key
	ctx := deps.HealthCache.NewLockContext(context.Background())
	if acquired, _ := deps.HealthCache.TryLockWithTimeout(ctx, healthCheckKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for key %s, skipping health check", healthCheckKey)
		return
	}

	// Ensure that the lock is released when the function is ended
	defer func() {
		if err := deps.HealthCache.Unlock(ctx, healthCheckKey); err != nil {
			log.Error().Err(err).Msgf("Error unlocking key %s", healthCheckKey)
		}
		log.Debug().Msgf("Successfully unlocked key %s", healthCheckKey)
	}()

	//Check if there is already a HealthCheck entry for the HealthCheckKey
	if shouldSkipHealthCheck(ctx, deps, healthCheckKey) {
		return
	}

	checkConsumerHealth(ctx, deps, cbMessage, healthCheckData, healthCheckKey)

	log.Info().Msgf("Successfully updated health check for key %s", healthCheckKey)
	return
}

func checkConsumerHealth(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckData health.HealthCheck, healthCheckKey string) {
	log.Debug().Msg("Checking consumer health")

	url, clientId, clientSecret := config.Current.Security.Url, config.Current.Security.ClientId, config.Current.Security.ClientSecret
	token, err := auth.RetrieveToken(url, clientId, clientSecret)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return
	}

	resp, err := executeHealthRequestWithToken(healthCheckData.CallbackUrl, healthCheckData.Method, token)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to perform http-request for callback-url %s", healthCheckData.CallbackUrl)
		return
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", healthCheckData.CallbackUrl, resp.StatusCode)

	if utils.Contains(config.Current.SuccessfulResponseCodes, resp.StatusCode) {
		handleSuccessfulHealthCheck(ctx, deps, cbMessage, healthCheckKey, healthCheckData, resp)
	} else {
		handleFailedHealthCheck(ctx, deps, cbMessage, healthCheckKey, healthCheckData, resp)
	}
}

func executeHealthRequestWithToken(callbackUrl string, httpMethod string, token string) (*http.Response, error) {
	log.Debug().Msgf("Performing health request for calllback-url %s with http-method %s", callbackUrl, httpMethod)

	request, err := http.NewRequest(httpMethod, callbackUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create request for URL %s: %v", callbackUrl, err)
	}

	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	//request.Header.Add("Accept", "application/stream+json")

	response, err := auth.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Failed to perform %s request to %s: %v", httpMethod, callbackUrl, err)
	}

	return response, nil
}

func republishPendingEvents(deps utils.Dependencies, subscriptionId string) {
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	//Get Waiting events from database pageable!
	pageable := options.Find().SetLimit(config.Current.RepublishingBatchSize).SetSort(bson.D{{Key: "timestamp", Value: 1}})
	// ToDo Iterate over pages
	dbMessages, err := deps.MongoConn.FindWaitingMessages(time.Now().UTC(), pageable, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
	}

	for _, dbMessage := range dbMessages {
		log.Debug().Msgf("Republishing message for subscription %s: %v", subscriptionId, dbMessage)

		if dbMessage.Coordinates == nil {
			log.Error().Msgf("Coordinates in message for subscription %s are nil: %v", subscriptionId, dbMessage)
			continue
		}

		kafkaMessage, err := deps.KafkaHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
		if err != nil {
			log.Warn().Msgf("Error while fetching message from kafka for subscription %s", subscriptionId)
			continue
		}
		err = deps.KafkaHandler.RepublishMessage(kafkaMessage)
		if err != nil {
			log.Warn().Msgf("Error while republishing message for subscription %s", subscriptionId)
		}
		log.Debug().Msgf("Successfully republished message for subscription %s", subscriptionId)
	}
}

func closeCircuitBreaker(deps utils.Dependencies, subscriptionId string) {
	if err := deps.CbCache.Delete(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId); err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, subscriptionId)
		return
	}
}

func buildHealthCheckData(subscription *resource.SubscriptionResource, httpMethod string) health.HealthCheck {
	return health.HealthCheck{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
		CheckingFor: subscription.Spec.Subscription.SubscriptionId,
	}
}

func shouldSkipHealthCheck(ctx context.Context, deps utils.Dependencies, healthCheckKey string) bool {
	existingHealthCheckData, err := deps.HealthCache.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving health check for key %s", healthCheckKey)
		return true
	}

	if existingHealthCheckData != nil {
		lastCheckedTime := existingHealthCheckData.(health.HealthCheck).LastChecked
		duration := time.Since(lastCheckedTime)
		if duration.Seconds() < config.Current.RequestCooldownTime.Seconds() {
			log.Debug().Msgf("Skipping health check for key %s due to cooldown", healthCheckKey)
			return true
		}
	}
	return false
}

func handleSuccessfulHealthCheck(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckKey string, healthCheckData health.HealthCheck, resp *http.Response) {
	cbMessage.Status = enum.CircuitBreakerStatusRepublishing
	cbMessage.LastModified = time.Now().UTC()

	if err := deps.CbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error putting CircuitBreakerMessage to cache for subscription %s", cbMessage.SubscriptionId)
		return
	}
	log.Debug().Msgf("Updated CircuitBreaker with id %s to status rebublishing", cbMessage.SubscriptionId)
	updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)

	go func() {
		republishPendingEvents(deps, healthCheckData.CheckingFor)
		// ToDo Check whether there are still  waiting messages in the db for the subscriptionId
		// ToDo When to increment the republishing counter?
		closeCircuitBreaker(deps, cbMessage.SubscriptionId)
	}()
}

func handleFailedHealthCheck(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckKey string, healthCheckData health.HealthCheck, resp *http.Response) {
	cbMessage.Status = enum.CircuitBreakerStatusOpen
	cbMessage.LastModified = time.Now().UTC()

	if err := deps.CbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error putting CircuitBreakerMessage to cache for subscription %s", cbMessage.SubscriptionId)
		return
	}
	log.Debug().Msgf("Updated CircuitBreaker with id %s to status rebublishing", cbMessage.SubscriptionId)
	updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)
}

func updateHealthCheck(ctx context.Context, deps utils.Dependencies, healthCheckKey string, healthCheckData health.HealthCheck, statusCode int) {
	healthCheckData.LastedCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := deps.HealthCache.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update health check for key %s", healthCheckKey)
	}
}
