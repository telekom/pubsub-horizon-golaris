// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
)

type CircuitBreakerListener struct{}

func (c *CircuitBreakerListener) OnAdd(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage) {
	// Nothing to do!
}

func (c *CircuitBreakerListener) OnUpdate(event *hazelcast.EntryNotified, obj message.CircuitBreakerMessage, oldObj message.CircuitBreakerMessage) {
	var open = obj.Status == enum.CircuitBreakerStatusOpen
	var subscriptionId = event.Key.(string)
	recordCircuitBreaker(subscriptionId, open)
}

func (c *CircuitBreakerListener) OnDelete(event *hazelcast.EntryNotified) {
	var subscriptionId = event.Key.(string)
	openCircuitBreakers.Delete(prometheus.Labels{
		"subscriptionId": subscriptionId,
	})
}

func (c *CircuitBreakerListener) OnError(event *hazelcast.EntryNotified, err error) {
	log.Warn().Err(err).Msg("could not listen for circuitbreaker changes")
}
