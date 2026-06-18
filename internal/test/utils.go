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

	"github.com/telekom/pubsub-horizon-go/message"
)

func EnvOrDefault(name, fallback string) string {
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

func GenerateStatusMessages(topic string, startPartition int32, startOffset int64, count int) []message.StatusMessage {
	msgs := make([]message.StatusMessage, count)
	for i := range count {
		p := startPartition
		o := startOffset + int64(i)

		msgs[i] = message.StatusMessage{
			Topic: topic,
			Coordinates: &message.Coordinates{
				Partition: &p,
				Offset:    &o,
			},
		}
	}
	return msgs
}
