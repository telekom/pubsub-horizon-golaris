// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package test

import (
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"golaris/internal/cache"
	"golaris/internal/config"
	"time"
)

// TODO remove after initial development is done
func CreateMockedCircuitBreakerMessages(numberMessages int) []message.CircuitBreakerMessage {
	messages := make([]message.CircuitBreakerMessage, 0, numberMessages)

	counter := 0
	for {
		log.Info().Msgf("Creating mocked circuit breaker message %d", counter)

		subscriptionId := config.Current.MockCbSubscriptionId

		circuitBreakerMessage := message.CircuitBreakerMessage{
			SubscriptionId:    subscriptionId,
			LastModified:      time.Now().Add(-48 * time.Hour),
			OriginMessageId:   "originMessageId",
			Status:            enum.CircuitBreakerStatusOpen,
			LastRepublished:   time.Now(),
			RepublishingCount: 0,
		}

		err := cache.CircuitBreakerCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, circuitBreakerMessage)
		if err != nil {
			log.Error().Err(err).Msg("Could not create mocked circuit breaker messages")
		}

		counter++
		if counter == numberMessages && numberMessages != 0 {
			break
		}
		time.Sleep(60 * time.Second)

	}
	return messages
}
