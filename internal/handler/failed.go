package handler

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"
)

func CheckFailedEvents() {
	var acquired = false
	var err error

	failedLockKey := "FailedHandlerLock"
	ctx := cache.FailedHandler.NewLockContext(context.Background())

	failedHandlerEntry, err := cache.FailedHandler.Get(ctx, failedLockKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving FailedHandler entry for key %s", failedLockKey)
		return
	}

	if failedHandlerEntry == nil {
		failedHandlerEntry = NewHandlerEntry(failedLockKey)
		err = cache.FailedHandler.Set(ctx, failedLockKey, failedHandlerEntry)
		if err != nil {
			log.Error().Err(err).Msgf("Error setting FailedHandler entry for key %s", failedLockKey)
			return
		}

		return
	}

	if acquired, _ = cache.FailedHandler.TryLockWithTimeout(ctx, failedLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for FailedHandler, skipping checkFailedEvents")
		return
	}

	defer func() {
		if acquired {
			if err = cache.FailedHandler.Unlock(ctx, failedLockKey); err != nil {
				log.Error().Err(err).Msg("Error unlocking FailedHandler")
			}
		}
	}()

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	opts := options.Find().SetLimit(batchSize).SetSkip(page * batchSize).SetSort(bson.D{{Key: "timestamp", Value: 1}})

	dbMessages, err := mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), opts)
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

			if len(dbMessages) < int(batchSize) {
				break
			}
			page++
		}
	}
}
