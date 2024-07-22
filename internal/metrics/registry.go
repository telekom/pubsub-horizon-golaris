// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/utils"
)

var (
	openCircuitBreakers *prometheus.GaugeVec

	registry *prometheus.Registry
)

const namespace = "golaris"

func init() {
	openCircuitBreakers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name:      "open_circuit_breakers",
		Help:      "The amount of open circuit-breakers.",
		Namespace: namespace,
	}, []string{"subscriptionId", "eventType"})

	registry = prometheus.NewRegistry()
	registry.MustRegister(openCircuitBreakers)
}

func recordCircuitBreaker(subscriptionId string, eventType string, open bool) {
	if config.Current.Metrics.Enabled {
		var value = float64(utils.IfThenElse(open, 1, 0))
		openCircuitBreakers.With(map[string]string{
			"subscriptionId": subscriptionId,
			"eventType":      eventType,
		}).Set(value)
	}
}

func PopulateFromCache() {
	var cbc = cache.CircuitBreakerCache
	circuitBreakers, err := cbc.GetQuery("circuit-breakers", predicate.True())
	if err != nil {
		log.Warn().Err(err).Msg("could initialize metrics from circuit-breakers map")
	}

	for _, circuitBreaker := range circuitBreakers {
		var open = circuitBreaker.Status == enum.CircuitBreakerStatusOpen
		recordCircuitBreaker(circuitBreaker.SubscriptionId, circuitBreaker.EventType, open)
	}
}

func ListenForChanges() {
	var cbc = cache.CircuitBreakerCache
	var listener = new(CircuitBreakerListener)
	if err := cbc.AddListener("circuit-breakers", listener); err != nil {
		log.Warn().Err(err).Msg("could not register listener for circuit-breakers map")
	}
}
