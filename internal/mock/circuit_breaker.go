// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mock

import (
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/rs/zerolog/log"
	"golaris/internal/cache"
	"golaris/internal/config"
	"time"
)

func CreateMockedCircuitBreakerMessages(numberMessages int) []message.CircuitBreakerMessage {
	messages := make([]message.CircuitBreakerMessage, 0, numberMessages)

	for i := 1; i <= numberMessages; i++ {
		log.Info().Msgf("Creating mocked circuit breaker message %d", i)

		subscriptionId := config.Current.MockCbSubscriptionId

		circuitBreakerMessage := message.CircuitBreakerMessage{
			SubscriptionId:    subscriptionId,
			LastModified:      time.Now().Add(-48 * time.Hour),
			OriginMessageId:   "originMessageId",
			Status:            enum.CircuitBreakerStatusOpen,
			LastRepublished:   time.Now(),
			RepublishingCount: 0,
		}

		err := cache.CircuitBreakers.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, circuitBreakerMessage)
		if err != nil {
			log.Error().Err(err).Msg("Could not create mocked circuit breaker messages")
		}

		time.Sleep(60 * time.Second)

	}
	return messages
}
