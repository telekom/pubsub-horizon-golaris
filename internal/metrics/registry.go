// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	OpenCircuitBreakers *prometheus.CounterVec

	registry *prometheus.Registry
)

const namespace = "scheduler"

func init() {
	OpenCircuitBreakers = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name:      "open_circuit_breakers",
		Help:      "The amount of open circuit-breakers.",
		Namespace: namespace,
	},
		[]string{"subscriptionId"})

	registry = prometheus.NewRegistry()
	registry.MustRegister(OpenCircuitBreakers)
}
