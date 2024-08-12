// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"context"
	"os"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
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
	err := cbMap.Clear(context.Background())
	if err != nil {
		return
	}

	subscriptionMap, _ := cache.SubscriptionCache.GetMap(config.Current.Hazelcast.Caches.SubscriptionCache)
	err = subscriptionMap.Clear(context.Background())
	if err != nil {
		return
	}

	err = cache.RepublishingCache.Clear(context.Background())
	if err != nil {
		return
	}

	err = cache.HealthCheckCache.Clear(context.Background())
	if err != nil {
		return
	}
}
