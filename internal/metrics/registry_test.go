// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"fmt"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/test"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
)

func resetMetrics() {
	registry = prometheus.NewRegistry()
	openCircuitBreakers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "open_circuit_breakers",
		Help:      "The amount of open circuit-breakers.",
		Namespace: namespace,
	}, []string{"subscriptionId", "subscriberId", "eventType", "environment"})
	registry.MustRegister(openCircuitBreakers)
}

func TestPopulateFromCache_OnlyOpenCircuitBreakers(t *testing.T) {
	resetMetrics()
	config.Current = test.BuildTestConfig()
	config.Current.Metrics.Enabled = true

	cbCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = cbCache

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	openCB := message.CircuitBreakerMessage{
		SubscriptionId: "open-sub-id",
		Status:         enum.CircuitBreakerStatusOpen,
		EventType:      "test.event.v1",
		Environment:    "integration",
	}
	cbCache.On("GetQuery", config.Current.Hazelcast.Caches.CircuitBreakerCache, mock.MatchedBy(func(p interface{}) bool {
		return fmt.Sprintf("%v", p) == fmt.Sprintf("%v", predicate.Equal("status", string(enum.CircuitBreakerStatusOpen)))
	})).
		Return([]message.CircuitBreakerMessage{openCB}, nil)

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriberId: "test-subscriber",
			},
		},
	}
	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "open-sub-id").Return(subscription, nil)

	PopulateFromCache()

	// Verify the open CB was recorded with correct subscriberId
	metrics, _ := registry.Gather()
	foundOpen := false
	foundClosed := false
	for _, mf := range metrics {
		if mf.GetName() == "golaris_open_circuit_breakers" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "subscriptionId" && label.GetValue() == "open-sub-id" {
						foundOpen = true
						assert.Equal(t, float64(1), m.GetGauge().GetValue(), "OPEN CB should have value 1")
					}
					if label.GetName() == "subscriptionId" && label.GetValue() == "closed-sub-id" {
						foundClosed = true
					}
				}
			}
		}
	}
	assert.True(t, foundOpen, "open-sub-id metric should be present")
	assert.False(t, foundClosed, "closed-sub-id should not be loaded (only OPEN CBs are populated)")
}

func TestPopulateFromCache_OrphanedCBGetsUnknownSubscriberId(t *testing.T) {
	resetMetrics()
	config.Current = test.BuildTestConfig()
	config.Current.Metrics.Enabled = true

	cbCache := new(test.CircuitBreakerMockCache)
	cache.CircuitBreakerCache = cbCache

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	orphanedCB := message.CircuitBreakerMessage{
		SubscriptionId: "orphaned-sub-id",
		Status:         enum.CircuitBreakerStatusClosed,
		EventType:      "test.event.v1",
		Environment:    "playground",
	}

	cbCache.On("GetQuery", config.Current.Hazelcast.Caches.CircuitBreakerCache, mock.MatchedBy(func(p interface{}) bool {
		return fmt.Sprintf("%v", p) == fmt.Sprintf("%v", predicate.Equal("status", string(enum.CircuitBreakerStatusOpen)))
	})).Return([]message.CircuitBreakerMessage{orphanedCB}, nil)

	// Subscription doesn't exist (deleted)
	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "orphaned-sub-id").Return((*resource.SubscriptionResource)(nil), nil)

	PopulateFromCache()

	// The orphaned CB gets subscriberId="unknown"
	metrics, _ := registry.Gather()
	for _, mf := range metrics {
		if mf.GetName() == "golaris_open_circuit_breakers" {
			for _, m := range mf.GetMetric() {
				for _, label := range m.GetLabel() {
					if label.GetName() == "subscriberId" {
						assert.Equal(t, "unknown", label.GetValue(), "orphaned CB should get subscriberId=unknown")
					}
				}
			}
		}
	}
}

func TestLookupSubscriberId_ReturnsSubscriberId(t *testing.T) {
	config.Current = test.BuildTestConfig()

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				SubscriberId: "my-subscriber",
			},
		},
	}
	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "sub-123").Return(subscription, nil)

	result := lookupSubscriberId("sub-123")
	assert.Equal(t, "my-subscriber", result)
}

func TestLookupSubscriberId_ReturnsUnknownWhenNotFound(t *testing.T) {
	config.Current = test.BuildTestConfig()

	subCache := new(test.SubscriptionMockCache)
	cache.SubscriptionCache = subCache

	subCache.On("Get", config.Current.Hazelcast.Caches.SubscriptionCache, "missing-sub").Return((*resource.SubscriptionResource)(nil), nil)

	result := lookupSubscriberId("missing-sub")
	assert.Equal(t, "unknown", result)
}
