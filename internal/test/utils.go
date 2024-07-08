//go:build testing

package test

import (
	"context"
	"golaris/internal/cache"
	"golaris/internal/config"
	"os"
)

func EnvOrDefault(name string, fallback string) string {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	return value
}

func ClearCaches() {
	cbMap, _ := cache.CircuitBreakerCache.GetMap(config.Current.Hazelcast.Caches.CircuitBreakerCache)
	cbMap.Clear(context.Background())

	subscriptionMap, _ := cache.SubscriptionCache.GetMap(config.Current.Hazelcast.Caches.SubscriptionCache)
	subscriptionMap.Clear(context.Background())

	cache.RepublishingCache.Clear(context.Background())
	cache.HealthCheckCache.Clear(context.Background())
}
