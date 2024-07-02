// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuit_breaker

import (
	"context"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/health_check"
	"golaris/internal/republish"
	"golaris/internal/utils"
	"time"
)

// HandleOpenCircuitBreaker handles the process when a circuit breaker is open.
// The function takes two parameters:
// - cbMessage: a CircuitBreakerMessage instance containing the details of the circuit breaker.
// - subscription: a pointer to a SubscriptionResource instance.
// The function performs several operations including specifying the HTTP method based on the subscription configuration,
// retrieving and updating health check data, performing a health check if not in cool down, and creating a republishing cache entry if the last health check was successful.
// It also handles the process of closing the circuit breaker once all operations are successfully completed.
func HandleOpenCircuitBreaker(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	hcData := prepareHealthCheck(subscription)

	if hcData.IsAcquired == false {
		log.Debug().Msgf("Could not acquire lock for health check key %s, skipping entry", hcData.HealthCheckKey)
		return
	}

	// Ensure that the lock is released when the function is ended
	defer func() {
		if err := cache.HealthCheckCache.Unlock(hcData.Ctx, hcData.HealthCheckKey); err != nil {
			log.Error().Err(err).Msgf("Error unlocking key %s", hcData.HealthCheckKey)
		}
		log.Debug().Msgf("Successfully unlocked key %s", hcData.HealthCheckKey)
	}()

	cbMessage, err := deleteRepubEntryAndIncreaseRepubCount(cbMessage, hcData)
	if err != nil {
		log.Error().Err(err).Msgf("Error while deleting republishing entry and increasing republishing count for subscriptionId %s", cbMessage.SubscriptionId)
		return
	}

	// Check if circuit breaker is in cool down
	if health_check.IsHealthCheckInCoolDown(hcData.HealthCheckEntry) == false {
		// Perform health check and update health check data
		err := health_check.CheckConsumerHealth(hcData, subscription)
		if err != nil {
			log.Debug().Msgf("Error while checking consumer health for key %s", hcData.HealthCheckKey)
			return
		}
	}

	// Create republishing cache entry if last health check was successful
	if utils.Contains(config.Current.HealthCheck.SuccessfulResponseCodes, hcData.HealthCheckEntry.LastCheckedStatus) {
		republishingCacheEntry := republish.RepublishingCache{SubscriptionId: cbMessage.SubscriptionId, RepublishingUpTo: time.Time{}}
		err := cache.RepublishingCache.Set(hcData.Ctx, cbMessage.SubscriptionId, republishingCacheEntry)
		if err != nil {
			log.Error().Msgf("Error while creating republishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
			return
		}
	}

	closeCircuitBreaker(cbMessage)

	log.Debug().Msgf("Successfully processed open circuit breaker entry for subscriptionId %s", cbMessage.SubscriptionId)
	return
}

// deleteRepubEntryAndIncreaseRepubCount deletes a republishing cache entry and increases the republishing count for a given subscription.
func deleteRepubEntryAndIncreaseRepubCount(cbMessage message.CircuitBreakerMessage, hcData *health_check.PreparedHealthCheckData) (message.CircuitBreakerMessage, error) {
	// Attempt to get an republishingCache entry for the subscriptionId
	republishingEntry, err := cache.RepublishingCache.Get(hcData.Ctx, cbMessage.SubscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting entry from RepublishingCache for subscriptionId %s", cbMessage.SubscriptionId)
	}

	// If there is an entry, force delete and increase republishingCount
	if republishingEntry != nil {
		log.Debug().Msgf("RepublishingCache entry found for subscriptionId %s", cbMessage.SubscriptionId)
		// ForceDelete eventual existing RepublishingCache entry for the subscriptionId
		republish.ForceDelete(cbMessage.SubscriptionId, hcData.Ctx)
		// Increase the republishing count for the subscription by 1
		updatedCbMessage, err := IncreaseRepublishingCount(cbMessage.SubscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while increasing republishing count for subscription %s", cbMessage.SubscriptionId)
			return message.CircuitBreakerMessage{}, err
		}
		cbMessage = *updatedCbMessage
	}
	return cbMessage, nil
}

// prepareHealthCheck tries to get an entry from the HealthCheckCache. If no entry exists it creates a new one. The entry then gets locked.
// It returns a PreparedHealthCheckData struct containing the context, health check key, health check entry, and a boolean indicating if the lock was acquired.
func prepareHealthCheck(subscription *resource.SubscriptionResource) *health_check.PreparedHealthCheckData {
	httpMethod := getHttpMethod(subscription)

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)

	ctx := cache.HealthCheckCache.NewLockContext(context.Background())

	// Get the health check entry for the healthCacheKey
	healthCheckEntry, err := cache.HealthCheckCache.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving health check entry for key %s", healthCheckKey)
	}

	// If no entry exists, create a new one
	if healthCheckEntry == nil {
		healthCheckEntry = health_check.CreateHealthCheckEntry(subscription, httpMethod)
		log.Debug().Msgf("Creating new health check entry for key %s", healthCheckKey)
	}

	// Attempt to acquire a lock for the health check key
	isAcquired, _ := cache.HealthCheckCache.TryLockWithTimeout(ctx, healthCheckKey, 10*time.Millisecond)

	castedHealthCheckEntry := healthCheckEntry.(health_check.HealthCheck)
	return &health_check.PreparedHealthCheckData{Ctx: ctx, HealthCheckKey: healthCheckKey, HealthCheckEntry: castedHealthCheckEntry, IsAcquired: isAcquired}
}

// getHttpMethod specifies the HTTP method based on the subscription configuration
func getHttpMethod(subscription *resource.SubscriptionResource) string {
	httpMethod := "HEAD"
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = "GET"
	}
	return httpMethod
}

// closeCircuitBreaker sets the circuit breaker status to CLOSED for a given subscription.
func closeCircuitBreaker(cbMessage message.CircuitBreakerMessage) {
	cbMessage.LastModified = time.Now()
	// ToDo Enhance horizon2go library with status closed
	cbMessage.Status = "CLOSED"
	err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, cbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, cbMessage.SubscriptionId)
		return
	}
}

// IncreaseRepublishingCount increments the republishing count for a given subscription by 1.
func IncreaseRepublishingCount(subscriptionId string) (*message.CircuitBreakerMessage, error) {
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return &message.CircuitBreakerMessage{}, err
	}

	cbMessage.RepublishingCount++
	if err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, *cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker message for subscription %s", subscriptionId)
		return &message.CircuitBreakerMessage{}, err
	}

	log.Debug().Msgf("Successfully increased RepublishingCount to %d for subscription %s", cbMessage.RepublishingCount, subscriptionId)
	return cbMessage, nil
}
