// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuit_breaker

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/health_check"
	"golaris/internal/republish"
	"golaris/internal/utils"
	"time"
)

func HandleOpenCircuitBreaker(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	// Specify HTTP method based on the subscription configuration
	httpMethod := "HEAD"
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = "GET"
	}

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)
	ctx := cache.HealthChecks.NewLockContext(context.Background())

	healthCheckData, err := cache.HealthChecks.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving health check for key %s", healthCheckKey)
	}

	if healthCheckData == nil {
		healthCheckData = health_check.BuildHealthCheckData(subscription, httpMethod)
		log.Debug().Msgf("Creating new health check for key %s", healthCheckKey)
	}

	// Attempt to acquire a lock for the health check key
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
	republishingEntry, err := cache.RepublishingCache.Get(ctx, cbMessage.SubscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting entry from RepublishingCache for subscriptionId %s", cbMessage.SubscriptionId)
	}

	// If there is an entry, force delete and increase republishingCount
	if republishingEntry != nil {
		log.Info().Msgf("RepublishingCache entry found for subscriptionId %s", cbMessage.SubscriptionId)
		// ForceDelete eventual existing RepublishingCache entry for the subscriptionId
		republish.ForceDelete(cbMessage.SubscriptionId, ctx)
		// Increase the republishing count for the subscription by 1
		updatedCbMessage, err := IncreaseRepublishingCount(cbMessage.SubscriptionId)
		cbMessage = *updatedCbMessage
		if err != nil {
			log.Error().Err(err).Msgf("Error while increasing republishing count for subscription %s", cbMessage.SubscriptionId)
			return
		}
	}

	castedHealthCheckData := healthCheckData.(health_check.HealthCheck)

	// Check if circuit breaker is in cool down
	healthStatusCode := castedHealthCheckData.LastedCheckedStatus
	if health_check.InCoolDown(castedHealthCheckData) == false {
		// Perform health check and update health check data
		resp, err := health_check.CheckConsumerHealth(castedHealthCheckData, subscription)
		if err != nil {
			log.Debug().Msgf("Error while checking consumer health for key %s", healthCheckKey)
			return
		}
		health_check.UpdateHealthCheck(ctx, healthCheckKey, castedHealthCheckData, resp.StatusCode)
		healthStatusCode = resp.StatusCode
	}

	// Create republishing cache entry if last health check was successful
	if utils.Contains(config.Current.HealthCheck.SuccessfulResponseCodes, healthStatusCode) {
		republishingCacheEntry := republish.RepublishingCache{SubscriptionId: cbMessage.SubscriptionId, RepublishingUpTo: time.Time{}}
		err := cache.RepublishingCache.Set(ctx, cbMessage.SubscriptionId, republishingCacheEntry)
		if err != nil {
			log.Error().Msgf("Error while creating republishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
			return
		}
	}

	CloseCircuitBreaker(cbMessage)

	log.Info().Msgf("Successfully proccessed open circuit breaker entry for subscriptionId %s", cbMessage.SubscriptionId)
	return
}

func CloseCircuitBreaker(cbMessage message.CircuitBreakerMessage) {
	cbMessage.LastModified = time.Now()
	// ToDo Enhance horizon2go library with status closed
	cbMessage.Status = "CLOSED"
	err := cache.CircuitBreakers.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, cbMessage.SubscriptionId)
		return
	}
}

// IncreaseRepublishingCount increments the republishing count for a given subscription.
// The function first retrieves the CircuitBreaker message associated with the subscriptionId from the cache.
// If the CircuitBreaker message is successfully retrieved, the republishing count of the message is incremented.
// The updated CircuitBreaker message is then put back into the cache.
// If any error occurs during these operations, it is logged and the function returns immediately.
//
// Parameters:
// subscriptionId: The ID of the subscription for which the republishing count is to be incremented.
//
// Returns:
// This function does not return a value.
func IncreaseRepublishingCount(subscriptionId string) (*message.CircuitBreakerMessage, error) {
	cbMessage, err := cache.CircuitBreakers.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return &message.CircuitBreakerMessage{}, err
	}

	cbMessage.RepublishingCount++
	if err := cache.CircuitBreakers.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, *cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker message for subscription %s", subscriptionId)
		return &message.CircuitBreakerMessage{}, err
	}

	log.Debug().Msgf("Successfully increased RepublishingCount to %d for subscription %s", cbMessage.RepublishingCount, subscriptionId)
	return cbMessage, nil
}
