// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package health_check

import (
	"context"
	"encoding/gob"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/internal/auth"
	"golaris/internal/cache"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
	"golaris/internal/utils"
	"net/http"
	"strings"
	"time"
)

func init() {
	gob.Register(HealthCheck{})
}

func PerformHealthCheck(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	// Specify HTTP method based on the subscription configuration
	httpMethod := "HEAD"
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = "GET"
	}

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)
	healthCheckData := buildHealthCheckData(subscription, httpMethod)

	// Attempt to acquire a lock for the health check key
	ctx := cache.HealthChecks.NewLockContext(context.Background())
	if acquired, _ := cache.HealthChecks.TryLockWithTimeout(ctx, healthCheckKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for key %s, skipping health check", healthCheckKey)
		return
	}

	// Ensure that the lock is released when the function is ended
	defer func() {
		if err := cache.HealthChecks.Unlock(ctx, healthCheckKey); err != nil {
			log.Error().Err(err).Msgf("Error unlocking key %s", healthCheckKey)
		}
		log.Debug().Msgf("Successfully unlocked key %s", healthCheckKey)
	}()

	// Attempt to get an republishingCache entry for the subscriptionId
	entry, err := cache.RepublishingCache.Get(ctx, cbMessage.SubscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting entry from RepublishingCache for subscriptionId %s", cbMessage.SubscriptionId)
	}

	// If there is an entry, force delete and increase republishingCount
	if entry != nil {
		log.Info().Msgf("RepublishingCache entry found for subscriptionId %s", cbMessage.SubscriptionId)
		// ForceDelete eventual existing RepublishingCache entry for the subscriptionId
		republish.ForceDelete(cbMessage.SubscriptionId, ctx)
		// Increase the republishing count for the subscription by 1
		circuit_breaker.IncreaseRepublishingCount(cbMessage.SubscriptionId)
	}

	//Check if there is already a HealthCheck entry for the HealthCheckKey
	if shouldSkipHealthCheck(ctx, healthCheckKey) {
		return
	}

	checkConsumerHealth(ctx, cbMessage, healthCheckData, healthCheckKey, subscription)

	log.Info().Msgf("Successfully updated health check for key %s", healthCheckKey)
	return
}

func buildHealthCheckData(subscription *resource.SubscriptionResource, httpMethod string) HealthCheck {
	return HealthCheck{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
	}
}

func updateHealthCheck(ctx context.Context, healthCheckKey string, healthCheckData HealthCheck, statusCode int) {
	healthCheckData.LastedCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := cache.HealthChecks.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update health check for key %s", healthCheckKey)
	}
}

func shouldSkipHealthCheck(ctx context.Context, healthCheckKey string) bool {
	existingHealthCheckData, err := cache.HealthChecks.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving health check for key %s", healthCheckKey)
		return true
	}

	if existingHealthCheckData != nil {
		lastCheckedTime := existingHealthCheckData.(HealthCheck).LastChecked
		duration := time.Since(lastCheckedTime)
		if duration.Seconds() < config.Current.HealthCheck.CoolDownTime.Seconds() {
			log.Debug().Msgf("Skipping health check for key %s due to cooldown", healthCheckKey)
			return true
		}
	}
	return false
}

func checkConsumerHealth(ctx context.Context, cbMessage message.CircuitBreakerMessage, healthCheckData HealthCheck, healthCheckKey string, subscription *resource.SubscriptionResource) {
	log.Debug().Msg("Checking consumer health")

	//Todo Take several virtual environments into account
	clientSecret := strings.Split(config.Current.Security.ClientSecret, "=")
	issuerUrl := strings.ReplaceAll(config.Current.Security.Url, "<realm>", clientSecret[0])

	token, err := auth.RetrieveToken(issuerUrl, config.Current.Security.ClientId, clientSecret[1])
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return
	}

	resp, err := executeHealthRequestWithToken(healthCheckData.CallbackUrl, healthCheckData.Method, subscription, token)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to perform http-request for callback-url %s", healthCheckData.CallbackUrl)
		return
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", healthCheckData.CallbackUrl, resp.StatusCode)
	updateHealthCheck(ctx, healthCheckKey, healthCheckData, resp.StatusCode)

	if utils.Contains(config.Current.HealthCheck.SuccessfulResponseCodes, resp.StatusCode) {
		go republish.RepublishPendingEvents(subscription.Spec.Subscription.SubscriptionId)

		// ToDo Check whether there are still  waiting messages in the db for the subscriptionId?
		circuit_breaker.CloseCircuitBreaker(cbMessage.SubscriptionId)
	}
}

func executeHealthRequestWithToken(callbackUrl string, httpMethod string, subscription *resource.SubscriptionResource, token string) (*http.Response, error) {
	log.Debug().Msgf("Performing health request for calllback-url %s with http-method %s", callbackUrl, httpMethod)

	request, err := http.NewRequest(httpMethod, callbackUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create request for URL %s: %v", callbackUrl, err)
	}

	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Add("x-pubsub-publisher-id", subscription.Spec.Subscription.PublisherId)
	request.Header.Add("x-pubsub-subscriber-id", subscription.Spec.Subscription.SubscriberId)

	response, err := auth.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Failed to perform %s request to %s: %v", httpMethod, callbackUrl, err)
	}

	return response, nil
}
