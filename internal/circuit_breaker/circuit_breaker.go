// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuit_breaker

import (
	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/config"
)

// TODO lock/unlock the circuit breaker entry?
// TODO set the status of the circuit breaker to closed instead of deleting the entry
func CloseCircuitBreaker(subscriptionId string) {
	if err := cache.CircuitBreakers.Delete(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId); err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, subscriptionId)
		return
	}
}

// TODO lock/unlock the circuit breaker entry?
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
func IncreaseRepublishingCount(subscriptionId string) {
	cbMessage, err := cache.CircuitBreakers.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return
	}

	cbMessage.RepublishingCount++
	if err := cache.CircuitBreakers.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, *cbMessage); err != nil {
		log.Error().Err(err).Msgf("Error while updating CircuitBreaker message for subscription %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully increased RepublishingCount to %d for subscription %s", cbMessage.RepublishingCount, subscriptionId)
}
