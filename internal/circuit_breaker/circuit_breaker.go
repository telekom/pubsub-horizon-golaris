package circuit_breaker

import (
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
	"golaris/internal/utils"
)

func CloseCircuitBreaker(deps utils.Dependencies, subscriptionId string) {
	if err := deps.CbCache.Delete(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId); err != nil {
		log.Error().Err(err).Msgf("Error: %v while closing circuit breaker for subscription %s", err, subscriptionId)
		return
	}
}
