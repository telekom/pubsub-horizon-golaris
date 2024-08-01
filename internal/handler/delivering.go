package handler

import (
	"context"
	"encoding/gob"
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

func init() {
	gob.Register(HandlerEntry{})
}

func CheckDeliveringEvents() {
	var acquired = false
	var err error

	deliveringLockKey := "DeliveringHandlerLock"
	ctx := cache.DeliveringHandler.NewLockContext(context.Background())

	deliveringHandlerEntry, err := cache.DeliveringHandler.Get(ctx, deliveringLockKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving DeliveringHandler entry for key %s", deliveringLockKey)
		return
	}

	if deliveringHandlerEntry == nil {
		deliveringHandlerEntry = NewHandlerEntry(deliveringLockKey)
		err = cache.DeliveringHandler.Set(ctx, deliveringLockKey, deliveringHandlerEntry)
		if err != nil {
			log.Error().Err(err).Msgf("Error setting DeliveringHandler entry for key %s", deliveringLockKey)
			return
		}

		return
	}

	if acquired, _ = cache.DeliveringHandler.TryLockWithTimeout(ctx, deliveringLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for DeliveringHandler, skipping checkDeliveringEvents")
		return
	}

	defer func() {
		if acquired {
			if err = cache.DeliveringHandler.Unlock(ctx, deliveringLockKey); err != nil {
				log.Error().Err(err).Msg("Error unlocking DeliveringHandler")
			}
		}
	}()

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	opts := options.Find().SetLimit(batchSize).SetSkip(page * batchSize).SetSort(bson.D{{Key: "timestamp", Value: 1}})

	deliveringStatesOffsetMins := config.Current.Republishing.DeliveringStatesOffsetMins
	upperThresholdTimestamp := time.Now().Add(-deliveringStatesOffsetMins * time.Minute)

	dbMessages, err := mongo.CurrentConnection.FindDeliveringMessagesByDeliveryType(upperThresholdTimestamp, opts)
	if err != nil {
		log.Error().Msgf("Error while fetching DELIVERING messages from MongoDb: %v", err)
		return
	}

	if len(dbMessages) == 0 {
		return
	}

	for _, dbMessage := range dbMessages {

		if dbMessage.Coordinates == nil {
			log.Printf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)
			return
		}

		kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage)
		if err != nil {
			log.Printf("Error while fetching message from kafka for subscriptionId %s: %v", dbMessage.SubscriptionId, err)
			return
		}

		var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
		var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

		traceCtx.StartSpan("republish delivering message")
		traceCtx.SetAttribute("component", "Horizon Golaris")
		traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
		traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
		traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
		traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))

		err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, "", "", false)
		if err != nil {
			log.Printf("Error while republishing message for subscriptionId %s: %v", dbMessage.SubscriptionId, err)
			return
		}
		log.Printf("Successfully republished message for subscriptionId %s", dbMessage.SubscriptionId)

		if len(dbMessages) < int(batchSize) {
			break
		}
		page++
	}
}
