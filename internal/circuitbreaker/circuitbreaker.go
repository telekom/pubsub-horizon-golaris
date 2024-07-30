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
				err = cache.RepublishingCache.Set(hcData.Ctx, subscription.Spec.Subscription.SubscriptionId, republish.RepublishingCache{SubscriptionId: subscription.Spec.Subscription.SubscriptionId})
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
		republishingCacheEntry := republish.RepublishingCache{SubscriptionId: cbMessage.SubscriptionId, RepublishingUpTo: time.Now()}
		err := cache.RepublishingCache.Set(hcData.Ctx, cbMessage.SubscriptionId, republishingCacheEntry)
		if err != nil {
			log.Error().Err(err).Msgf("Error while creating RepublishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
			return
		}
		log.Debug().Msgf("Successfully created RepublishingCache entry for subscriptionId %s", cbMessage.SubscriptionId)
		CloseCircuitBreaker(&cbMessage)
	}

	log.Debug().Msgf("Successfully processed open CircuitBreaker entry for subscriptionId %s", cbMessage.SubscriptionId)
	return
}

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
		republishCacheEntry, ok := republishingEntry.(republish.RepublishingCache)
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
