// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"pubsub-horizon-golaris/internal/config"
	"time"
)

func BuildTestConfig() config.Configuration {
	return config.Configuration{
		LogLevel: "debug",
		Port:     8080,
		CircuitBreaker: config.CircuitBreaker{
			OpenCheckInterval:       30 * time.Second,
			OpenLoopDetectionPeriod: 300 * time.Second,
			ExponentialBackoffMax:   60 * time.Minute,
			ExponentialBackoffBase:  1000 * time.Millisecond,
		},
		HealthCheck: config.HealthCheck{
			SuccessfulResponseCodes: []int{200, 201, 202},
			CoolDownTime:            30 * time.Second,
		},
		Republishing: config.Republishing{
			CheckInterval: 10 * time.Second,
			BatchSize:     100,
		},
		Hazelcast: config.Hazelcast{
			ServiceDNS:  EnvOrDefault("HAZELCAST_HOST", "localhost"),
			ClusterName: "dev",
			Caches: config.Caches{
				SubscriptionCache:   "subCache",
				CircuitBreakerCache: "cbCache",
				HealthCheckCache:    "hcCache",
				RepublishingCache:   "repCache",
			},
		},
		Kafka: config.Kafka{
			Brokers: []string{"broker1:9092", "broker2:9092"},
			Topics:  []string{"testTopic"},
		},
		Mongo: config.Mongo{
			Url:        "mongodb://localhost:27017",
			Database:   "mydatabase",
			Collection: "mycollection",
			BulkSize:   10,
		},
		Security: config.Security{
			Enabled:      true,
			Url:          "https://security.local",
			ClientId:     "my-client-id",
			ClientSecret: []string{"default=my-client-secret"},
		},
		Tracing: config.Tracing{
			CollectorEndpoint: "http://tracing.local/collect",
			DebugEnabled:      false,
			Enabled:           true,
		},
	}
}
