package health_check

import (
	"context"
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/message"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/internal/auth"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
	"golaris/internal/utils"
	"net/http"
	"time"
)

func checkConsumerHealth(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckData HealthCheck, healthCheckKey string) {
	log.Debug().Msg("Checking consumer health")

	url, clientId, clientSecret := config.Current.Security.Url, config.Current.Security.ClientId, config.Current.Security.ClientSecret
	token, err := auth.RetrieveToken(url, clientId, clientSecret)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return
	}

	resp, err := executeHealthRequestWithToken(healthCheckData.CallbackUrl, healthCheckData.Method, token)
	if err != nil {
		// Todo Check error handling while timeout
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

	// Todo Add missing headers
	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Add("Accept", "application/stream+json")

	response, err := auth.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Failed to perform %s request to %s: %v", httpMethod, callbackUrl, err)
	}

	return response, nil
}

func handleSuccessfulHealthCheck(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckKey string, healthCheckData HealthCheck, resp *http.Response) {
	// Todo Method for update CBMessage and delete handleSuccessfulHealthCheck
	cbMessage.Status = enum.CircuitBreakerStatusRepublishing
	cbMessage.LastModified = time.Now().UTC()

	if err := deps.CbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error putting CircuitBreakerMessage to cache for subscription %s", cbMessage.SubscriptionId)
		return
	}
	log.Debug().Msgf("Updated CircuitBreaker with id %s to status rebublishing", cbMessage.SubscriptionId)

	updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)
	go republish.RepublishPendingEvents(deps, healthCheckData.CheckingFor)

	// ToDo Check whether there are still  waiting messages in the db for the subscriptionId?
	// ToDo When to increment the republishing counter?
	circuit_breaker.CloseCircuitBreaker(deps, cbMessage.SubscriptionId)
}

func handleFailedHealthCheck(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckKey string, healthCheckData HealthCheck, resp *http.Response) {
	cbMessage.Status = enum.CircuitBreakerStatusOpen
	cbMessage.LastModified = time.Now().UTC()

	if err := deps.CbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error putting CircuitBreakerMessage to cache for subscription %s", cbMessage.SubscriptionId)
		return
	}
	log.Debug().Msgf("Updated CircuitBreaker with id %s to status rebublishing", cbMessage.SubscriptionId)
	updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)
}
