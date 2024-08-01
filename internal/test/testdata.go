// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

//go:build testing

package test

import (
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/types"
	"pubsub-horizon-golaris/internal/config"
	"time"
)

func NewTestCbMessage(testSubscriptionId string) message.CircuitBreakerMessage {
	testCircuitBreakerMessage := message.CircuitBreakerMessage{
		SubscriptionId: testSubscriptionId,
		Status:         enum.CircuitBreakerStatusOpen,
		LastModified:   types.NewTimestamp(time.Now().UTC()),
	}
	return testCircuitBreakerMessage
}

func NewTestSubscriptionResource(testSubscriptionId string, testCallbackUrl string, testEnvironment string) *resource.SubscriptionResource {
	testSubscriptionResource := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriptionId:        testSubscriptionId,
				Callback:              testCallbackUrl,
				EnforceGetHealthCheck: false,
			},
			Environment: testEnvironment,
		},
	}
	return testSubscriptionResource
}

func BuildTestConfig() config.Configuration {

	return config.Configuration{
		LogLevel: "debug",
		Port:     8080,
		CircuitBreaker: config.CircuitBreaker{
			OpenCbCheckInterval: 30 * time.Second,
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
			CustomLoggerEnabled: false,
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
