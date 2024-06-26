// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package circuit_breaker

import (
	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/config"
)

func CloseCircuitBreaker(subscriptionId string) {
	if err := cache.CircuitBreakers.Delete(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId); err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, subscriptionId)
		return
	}
}
