// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuitbreaker

import (
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/types"
	"math"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/healthcheck"
	"pubsub-horizon-golaris/internal/republish"
	"slices"
	"time"
)

var (
	healthCheckFunc = healthcheck.CheckConsumerHealth
)

// HandleOpenCircuitBreaker handles the process when a circuit breaker is open.
// The function takes two parameters:
// - cbMessage: a CircuitBreakerMessage instance containing the details of the circuit breaker.
// - subscription: a pointer to a SubscriptionResource instance.
// The function performs several operations including specifying the HTTP method based on the subscription configuration,
// retrieving and updating health check data, performing a health check if not in cool down, and creating a republishing cache entry if the last health check was successful.
// It also handles the process of closing the circuit breaker once all operations are successfully completed.
func HandleOpenCircuitBreaker(cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	hcData, err := healthcheck.PrepareHealthCheck(subscription)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to create new HealthCheck cache entry for subscriptionId %s", subscription.Spec.Subscription.SubscriptionId)
		return
	}

	if hcData.IsAcquired == false {
		log.Debug().Msgf("Could not acquire lock for HealthCheck cache entry, skipping entry for subscriptionId %s", hcData.HealthCheckKey)
		return
	}
	log.Debug().Msgf("Successfully locked HealthCheck cache entry with key %s", hcData.HealthCheckKey)

	// Ensure that the lock is released when the function is ended
	defer func() {
		if hcData.IsAcquired == true {
			if err := cache.HealthCheckCache.Unlock(hcData.Ctx, hcData.HealthCheckKey); err != nil {
				log.Error().Err(err).Msgf("Error unlocking HealthCheck cache entry with key %s", hcData.HealthCheckKey)
			}
			log.Debug().Msgf("Successfully unlocked HealthCheck cache entry with key %s", hcData.HealthCheckKey)
		}
	}()

	// Check and handle if circuit breaker is in a loop and update cb message
	checkAndHandleCircuitBreakerLoop(&cbMessage)

	cbMessage, err = deleteRepubEntryAndIncreaseRepubCount(cbMessage, hcData)
	if err != nil {
		log.Error().Err(err).Msgf("Error while deleting Republishing cache entry and increasing republishing count for subscriptionId %s", cbMessage.SubscriptionId)
		return
	}

	// Check if circuit breaker is in cool down
	if healthcheck.IsHealthCheckInCoolDown(hcData.HealthCheckEntry) == false {
		// Perform health check and update health check data
		err := healthCheckFunc(hcData, subscription)
		if err != nil {
			log.Debug().Msgf("HealthCheck failed for key %s", hcData.HealthCheckKey)
			return
		}
	} else {
		log.Debug().Msgf("HealthCheck is in cooldown for key %s", hcData.HealthCheckKey)
	}

	// Create republishing cache entry if last health check was successful
	if slices.Contains(config.Current.HealthCheck.SuccessfulResponseCodes, hcData.HealthCheckEntry.LastCheckedStatus) {
		// Calculate exponential backoff for republishing based on circuit breaker loop counter
		postponedUntil := time.Now().Add(+calculateExponentialBackoff(cbMessage))
		log.Debug().Msgf("backoff: %v", calculateExponentialBackoff(cbMessage))
		republishingCacheEntry := republish.RepublishingCache{SubscriptionId: cbMessage.SubscriptionId, RepublishingUpTo: time.Now(), PostponedUntil: postponedUntil}
		err := cache.RepublishingCache.Set(hcData.Ctx, cbMessage.SubscriptionId, republishingCacheEntry)
		if err != nil {
			log.Error().Err(err).Msgf("Error while creating RepublishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
			return
		}
		log.Debug().Msgf("Successfully created RepublishingCache entry for subscriptionId %s: %v", cbMessage.SubscriptionId, republishingCacheEntry)
		CloseCircuitBreaker(&cbMessage)
	}

	log.Debug().Msgf("Successfully processed open CircuitBreaker entry for subscriptionId %s", cbMessage.SubscriptionId)
	return
}

// checkAndHandleCircuitBreakerLoop evaluates the circuit breaker's last opened timestamp against the configured loop detection period.
// If the circuit breaker was last opened within the loop detection period, it increments the circuit breaker counter to indicate
// a potential loop scenario. If the last opened timestamp is outside the loop detection period, it resets the counter, assuming
// normal operation. The function updates the circuit breaker's last opened timestamp and republishing count in the cache.
func checkAndHandleCircuitBreakerLoop(cbMessage *message.CircuitBreakerMessage) {
	loopDetectionPeriod := config.Current.CircuitBreaker.OpenCbLoopDetectionPeriod

	//Todo Check if LoopCounter is kept in comet when opening a circuit breaker again

	// If circuit breaker last opened timestamp is within loop detection period, increase loop counter
	if cbMessage.LastOpened != nil && time.Since(cbMessage.LastOpened.ToTime()).Seconds() < loopDetectionPeriod.Seconds() {
		log.Debug().Msgf("Detected circuit breaker loop. Increasing loop counter for subscription %s: %d", cbMessage.SubscriptionId, cbMessage.LoopCounter+1)
		cbMessage.LoopCounter++
	} else {
		// If outside the loop detection period, reset loop  counter
		log.Debug().Msgf("No circuit breaker loop. Resetting loop counter for subscription %s", cbMessage.SubscriptionId)
		cbMessage.LoopCounter = 0
	}
	cbMessage.LastModified = types.NewTimestamp(time.Now().UTC())

	err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, *cbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker for subscription %s with loop detection result: %v", cbMessage.SubscriptionId, err)
		return
	}
}

// deleteRepubEntryAndIncreaseRepubCount deletes a republishing cache entry and increases the republishing count for a given subscription.
func deleteRepubEntryAndIncreaseRepubCount(cbMessage message.CircuitBreakerMessage, hcData *healthcheck.PreparedHealthCheckData) (message.CircuitBreakerMessage, error) {
	// Attempt to get an republishingCache entry for the subscriptionId
	republishingEntry, err := cache.RepublishingCache.Get(hcData.Ctx, cbMessage.SubscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting RepublishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
	}

	// If there is an entry, force delete and increase LoopCounter
	if republishingEntry != nil {
		log.Debug().Msgf("RepublishingCache entry found for subscriptionId %s", cbMessage.SubscriptionId)
		// ForceDelete eventual existing RepublishingCache entry for the subscriptionId
		republish.ForceDelete(hcData.Ctx, cbMessage.SubscriptionId)
		// Increase the republishing count for the subscription by 1
		updatedCbMessage, err := IncreaseLoopCounter(cbMessage.SubscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while increasing republishing count for subscription %s", cbMessage.SubscriptionId)
			return message.CircuitBreakerMessage{}, err
		}
		cbMessage = *updatedCbMessage
	}
	return cbMessage, nil
}

// CloseCircuitBreaker sets the circuit breaker status to CLOSED for a given subscription.
func CloseCircuitBreaker(cbMessage *message.CircuitBreakerMessage) {
	cbMessage.LastModified = types.NewTimestamp(time.Now().UTC())
	cbMessage.Status = enum.CircuitBreakerStatusClosed
	err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, *cbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing CircuitBreaker for subscription %s", err, cbMessage.SubscriptionId)
		return
	}
	log.Info().Msgf("Successfully closed circuit breaker for subscription %s with status %s", cbMessage.SubscriptionId, cbMessage.Status)
}

// IncreaseLoopCounter increments the loop counter for a given subscription by 1.
func IncreaseLoopCounter(subscriptionId string) (*message.CircuitBreakerMessage, error) {
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return nil, err
	}

	cbMessage.LastOpened = types.NewTimestamp(time.Now().UTC())
	cbMessage.LoopCounter++
	if err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, *cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker message for subscription %s", subscriptionId)
		return nil, err
	}

	log.Debug().Msgf("Successfully increased LoopCounter to %d for subscription %s", cbMessage.LoopCounter, subscriptionId)
	return cbMessage, nil
}

// calculateExponentialBackoff calculates the exponential backoff duration based on the loop counter.
func calculateExponentialBackoff(cbMessage message.CircuitBreakerMessage) time.Duration {
	exponentialBackoffBase := config.Current.CircuitBreaker.ExponentialBackoffBase
	exponentialBackoffMax := config.Current.CircuitBreaker.ExponentialBackoffMax

	// Return 0 for exponential backoff if circuit breaker is opened for the first time
	if cbMessage.LoopCounter <= 1 {
		return 0
	}

	// Calculate the exponential backoff based on republishing count. If the circuit breaker counter is 2 it is the first retry
	exponentialBackoff := exponentialBackoffBase * time.Duration(math.Pow(2, float64(cbMessage.LoopCounter-1)))

	// Limit the exponential backoff to the max backoff
	if exponentialBackoff > exponentialBackoffMax {
		exponentialBackoff = exponentialBackoffMax
	}
	return exponentialBackoff
}
