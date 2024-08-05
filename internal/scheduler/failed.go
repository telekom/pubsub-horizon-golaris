package scheduler

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"
)

func checkFailedEvents() {
	var ctx = cache.FailedHandler.NewLockContext(context.Background())

	if acquired, _ := cache.FailedHandler.TryLockWithTimeout(ctx, cache.FailedLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for FailedHandler entry: %s", cache.FailedLockKey)
		return
	}

	defer func() {
		if err := cache.FailedHandler.Unlock(ctx, cache.FailedLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking FailedHandler")
		}
	}()

	batchSize := config.Current.Republishing.BatchSize

	var dbMessages []message.StatusMessage
	var err error

	for {
		var lastCursor any
		dbMessages, _, err = mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription from db")
			return
		}

		if len(dbMessages) == 0 {
			return
		}

		for _, dbMessage := range dbMessages {
			subscriptionId := dbMessage.SubscriptionId

			subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
			if err != nil {
				log.Printf("Error while fetching republishing entry for subscriptionId %s: %v", subscriptionId, err)
				return
			}

			if subscription != nil {
				if subscription.Spec.Subscription.DeliveryType == "sse" || subscription.Spec.Subscription.DeliveryType == "server_sent_event" {
					var newDeliveryType = "SERVER_SENT_EVENT"

					if dbMessage.Coordinates == nil {
						log.Printf("Coordinates in message for subscriptionId %s are nil: %v", subscriptionId, dbMessage)
						return
					}

					kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage)
					if err != nil {
						log.Printf("Error while fetching message from kafka for subscriptionId %s: %v", subscriptionId, err)
						return
					}

					var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
					var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

					traceCtx.StartSpan("republish failed message")
					traceCtx.SetAttribute("component", "Horizon Golaris")
					traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
					traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
					traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
					traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))

					err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, newDeliveryType, "", true)
					if err != nil {
						log.Printf("Error while republishing message for subscriptionId %s: %v", subscriptionId, err)
						return
					}
					log.Printf("Successfully republished message for subscriptionId %s", subscriptionId)

				}
			}
		}

		if len(dbMessages) < int(batchSize) {
			break
		}
	}
}
