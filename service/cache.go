package service

import (
	"eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"golaris/health_check"
	"golaris/utils"
)

func configureCaches(config hazelcast.Config) (utils.Dependencies, error) {
	var err error

	deps.SubCache, err = cache.NewCache[resource.SubscriptionResource](config)
	if err != nil {
		return deps, fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	deps.CbCache, err = cache.NewCache[message.CircuitBreakerMessage](config)
	if err != nil {
		return deps, fmt.Errorf("error initializing CircuitBreaker health cache: %v", err)
	}

	deps.HealthCache, err = health_check.NewHealthCheckCache(config)
	if err != nil {
		return deps, fmt.Errorf("error initializing HealthCheck cache: %v", err)
	}

	return deps, nil
}
