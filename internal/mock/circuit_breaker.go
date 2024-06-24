package mock

import (
	"eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
	"time"
)

func CreateMockedCircuitBreakerMessages(cbCache *cache.Cache[message.CircuitBreakerMessage], numberMessages int) []message.CircuitBreakerMessage {
	messages := make([]message.CircuitBreakerMessage, 0, numberMessages)

	for i := 1; i <= numberMessages; i++ {

		subscriptionId := ""

		circuitBreakerMessage := message.CircuitBreakerMessage{
			SubscriptionId:    subscriptionId,
			LastModified:      time.Now().Add(-48 * time.Hour),
			OriginMessageId:   "originMessageId",
			Status:            enum.CircuitBreakerStatusOpen,
			LastRepublished:   time.Now(),
			RepublishingCount: 0,
		}

		err := cbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId, circuitBreakerMessage)
		if err != nil {
			log.Error().Err(err).Msg("Could not create mocked circuit breaker messages")
		}

	}
	return messages
}
