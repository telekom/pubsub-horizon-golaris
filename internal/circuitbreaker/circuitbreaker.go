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
		log.Error().Err(err).Msgf("Failed to create new HealthCheckCacheEntry for subscriptionId %s", subscription.Spec.Subscription.SubscriptionId)
		return
	}

	if hcData.IsAcquired == false {
		log.Debug().Msgf("Could not acquire lock for HealthCheckCacheEntry, skipping entry for subscriptionId %s", hcData.HealthCheckKey)
		return
	}
	log.Debug().Msgf("Successfully locked HealthCheckCacheEntry with key %s", hcData.HealthCheckKey)

	// Ensure that the lock is released when the function is ended
	defer func() {
		if hcData.IsAcquired == true {
			if err := cache.HealthCheckCache.Unlock(hcData.Ctx, hcData.HealthCheckKey); err != nil {
				log.Error().Err(err).Msgf("Error unlocking HealthCheckCacheEntry with key %s", hcData.HealthCheckKey)
			}
			log.Debug().Msgf("Successfully unlocked HealthCheckCacheEntry with key %s", hcData.HealthCheckKey)
		}
	}()

	err = forceDeleteRepublishingEntry(cbMessage, hcData)
	if err != nil {
		log.Error().Err(err).Msgf("Error while deleting Republishing cache entry for subscriptionId %s", cbMessage.SubscriptionId)
		return
	}

	// Check if circuit breaker is in cool down
	if healthcheck.IsHealthCheckInCoolDown(hcData.HealthCheckEntry) == false {
		// Perform health check and update health check data
		err := healthCheckFunc(hcData, subscription)
		if err != nil {
			log.Debug().Msgf("HealthCheck failed for key %s", hcData.HealthCheckKey)

			// I have observed the case where events were set to DELIVERING.
			// Then the DeliveryType was changed to SSE. However, the events landed on WAITING.
			// HealthCheck was performed, but the CallbackUrl was already missing because deliveryType was set to SSE.
			if subscription.Spec.Subscription.Callback == "" || subscription.Spec.Subscription.DeliveryType == "sse" || subscription.Spec.Subscription.DeliveryType == "server_sent_event" {
				err = cache.RepublishingCache.Set(hcData.Ctx, subscription.Spec.Subscription.SubscriptionId, republish.RepublishingCacheEntry{SubscriptionId: subscription.Spec.Subscription.SubscriptionId})
				if err != nil {
					log.Error().Err(err).Msgf("Error while creating RepublishingCache entry for subscriptionId %s", subscription.Spec.Subscription.SubscriptionId)
					return
				}
				CloseCircuitBreaker(&cbMessage)
			}
			return
		}
	} else {
		log.Debug().Msgf("HealthCheck is in cooldown for key %s", hcData.HealthCheckKey)
	}

	// Create republishing cache entry if last health check was successful
	if slices.Contains(config.Current.HealthCheck.SuccessfulResponseCodes, hcData.HealthCheckEntry.LastCheckedStatus) {
		// Check if circuit breaker is in a loop and update loop counter of cb message or reset it
		err = checkForCircuitBreakerLoop(&cbMessage)
		if err != nil {
			log.Error().Err(err).Msgf("Error handling circuit breaker loop for subscriptionId %s", cbMessage.SubscriptionId)
		}

		// Calculate exponential backoff for new republishing cache entry based on updated circuit breaker loop counter
		exponentialBackoff := calculateExponentialBackoff(cbMessage)
		log.Debug().Msgf("Calculated exponential backoff for circuit breaker with subscriptionId %s: %v", cbMessage.SubscriptionId, exponentialBackoff)

		// Create republishing cache entry
		republishingCacheEntry := republish.RepublishingCacheEntry{SubscriptionId: cbMessage.SubscriptionId, RepublishingUpTo: time.Now(), PostponedUntil: time.Now().Add(+exponentialBackoff)}

		log.Info().Msgf("postponedUntil for subscriptionId %s set to %v", republishingCacheEntry.SubscriptionId, republishingCacheEntry.PostponedUntil)

		err := cache.RepublishingCache.Set(hcData.Ctx, cbMessage.SubscriptionId, republishingCacheEntry)
		if err != nil {
			log.Error().Err(err).Msgf("Error while creating RepublishingCacheEntry entry for subscriptionId %s", cbMessage.SubscriptionId)
			return
		}
		log.Debug().Msgf("Successfully created RepublishingCacheEntry entry for subscriptionId %s: %+v", cbMessage.SubscriptionId, republishingCacheEntry)
		CloseCircuitBreaker(&cbMessage)
	}

	log.Debug().Msgf("Successfully processed open CircuitBreaker entry for subscriptionId %s", cbMessage.SubscriptionId)
	return
}

// checkForCircuitBreakerLoop evaluates the circuit breaker's last opened timestamp against the configured loop detection period.
// If the circuit breaker was last opened within the loop detection period, it increments the circuit breaker counter to indicate
// a potential loop scenario. If the last opened timestamp is outside the loop detection period, it resets the counter, assuming
// normal operation. The function also updates the circuit breaker's last opened timestamp for the next loop detection to the last
// modified timestamp (as the current circuit breaker was opened then)
func checkForCircuitBreakerLoop(cbMessage *message.CircuitBreakerMessage) error {
	loopDetectionPeriod := config.Current.CircuitBreaker.OpenLoopDetectionPeriod

	// If circuit breaker last opened timestamp is within loop detection period, increase loop counter
	if cbMessage.LastOpened != nil && time.Since(cbMessage.LastOpened.ToTime()).Seconds() < loopDetectionPeriod.Seconds() {
		cbMessage.LoopCounter++
		log.Debug().Msgf("Circuit breaker opened within loop detection period. Increased loop counter for subscription %s: %d", cbMessage.SubscriptionId, cbMessage.LoopCounter)
	} else {
		// If outside the loop detection period, reset loop  counter
		cbMessage.LoopCounter = 0
		log.Debug().Msgf("Circuit breaker opened outside loop detection period. Reseted loop counter for subscription %s: %v", cbMessage.SubscriptionId, cbMessage.LoopCounter)
	}
	cbMessage.LastModified = types.NewTimestamp(time.Now().UTC())

	err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, cbMessage.SubscriptionId, *cbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker for subscription %s with loop detection result: %v", cbMessage.SubscriptionId, err)
		return err
	}
	return nil
}

// Attempt to get an RepublishingCacheEntry for the subscriptionId
// forceDeleteRepublishingEntry forces the deletion of a republishing cache entry.
func forceDeleteRepublishingEntry(cbMessage message.CircuitBreakerMessage, hcData *healthcheck.PreparedHealthCheckData) error {
	// Attempt to get an RepublishingCacheEntry for the subscriptionId
	republishingEntry, err := cache.RepublishingCache.Get(hcData.Ctx, cbMessage.SubscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting RepublishingCacheEntry for subscriptionId %s", cbMessage.SubscriptionId)
		return err
	}

	// If there is an entry, force delete
	if republishingEntry != nil {
		republishCacheEntry, ok := republishingEntry.(republish.RepublishingCacheEntry)
		if !ok {
			log.Error().Msgf("Error casting republishing entry for subscriptionId %s", cbMessage.SubscriptionId)
			return err
		}
		if republishCacheEntry.SubscriptionChange != true {
			log.Debug().Msgf("RepublishingCacheEntry found for subscriptionId %s", cbMessage.SubscriptionId)
			republish.ForceDelete(hcData.Ctx, cbMessage.SubscriptionId)
		}
	}
	return nil
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

// calculateExponentialBackoff calculates the exponential backoff duration based on the loop counter.
func calculateExponentialBackoff(cbMessage message.CircuitBreakerMessage) time.Duration {
	exponentialBackoffBase := config.Current.CircuitBreaker.ExponentialBackoffBase
	exponentialBackoffMax := config.Current.CircuitBreaker.ExponentialBackoffMax

	// Return 0 for exponential backoff if circuit breaker is opened for the first time.
	// If the circuit breaker counter is 1 it is newly opened, because the counter had  already been  incremented immediately before
	if cbMessage.LoopCounter <= 1 {
		return 0
	}

	// Calculate the exponential backoff based on republishing count.
	// If the circuit breaker counter is 2 it is the first retry, because the counter had  already been  incremented immediately before
	exponentialBackoff := exponentialBackoffBase * time.Duration(math.Pow(2, float64(cbMessage.LoopCounter-1)))

	// Limit the exponential backoff to the max backoff
	if exponentialBackoff > exponentialBackoffMax {
		exponentialBackoff = exponentialBackoffMax
	}

	log.Info().Msgf("Calculating exponential backoff, subscriptionId %s, loopCounter %d, exponentialBackoffBase %v, exponentialBackoffMax %v, exponentialBackoff %v", cbMessage.SubscriptionId, cbMessage.LoopCounter, exponentialBackoffBase, exponentialBackoffMax, exponentialBackoff)

	return exponentialBackoff
}
