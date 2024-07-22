// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
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
	}, []string{"subscriptionId"})

	registry = prometheus.NewRegistry()
	registry.MustRegister(openCircuitBreakers)
}

func recordCircuitBreaker(subscriptionId string, open bool) {
	if config.Current.Metrics.Enabled {
		var value = float64(utils.IfThenElse(open, 1, 0))
		openCircuitBreakers.With(map[string]string{
			"subscriptionId": subscriptionId,
		}).Set(value)
	}
}

func ListenForChanges() {
	var cbc = cache.CircuitBreakerCache
	var listener = new(CircuitBreakerListener)
	if err := cbc.AddListener("circuit-breakers", listener); err != nil {
		log.Warn().Err(err).Msg("could not register listener for circuit-breakers map")
	}
}
