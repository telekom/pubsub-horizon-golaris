package scheduler

import (
	"context"
	"encoding/gob"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"
)

func init() {
	gob.Register(HandlerEntry{})
	gob.Register(DeliveringEntry{})
}

type DeliveringEntry struct {
	Entry string `json:"entry"`
}

func NewDeliveringEntry(lockKey string) DeliveringEntry {
	return DeliveringEntry{
		Entry: lockKey,
	}
}

func checkDeliveringEvents() {
	ctx := cache.DeliveringHandler.NewLockContext(context.Background())

	if acquired, _ := cache.DeliveringHandler.TryLockWithTimeout(ctx, cache.DeliveringLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for DeliveringHandler entry: %s", cache.DeliveringLockKey)
		return
	}

	defer func() {
		if err := cache.DeliveringHandler.Unlock(ctx, cache.DeliveringLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking DeliveringHandler")
		}
		log.Info().Msg("Lock released for DeliveringHandler")
	}()

	entry, err := cache.DeliveringHandler.Get(ctx, cache.DeliveringLockKey)
	if err != nil {
		log.Error().Err(err).Msg("Error while fetching DeliveringHandler entry")
		return
	}

	if entry == nil {
		newEntry := NewDeliveringEntry(cache.DeliveringLockKey)
		err = cache.DeliveringHandler.Set(ctx, cache.DeliveringLockKey, newEntry)
		if err != nil {
			log.Error().Err(err).Msg("Error while setting DeliveringHandler entry")
			return
		}
	}

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	deliveringStatesOffsetMins := config.Current.Republishing.DeliveringStatesOffsetMins
	upperThresholdTimestamp := time.Now().Add(-deliveringStatesOffsetMins * time.Minute)
	var lastCursor any

	dbMessages, lastCursor, err := mongo.CurrentConnection.FindDeliveringMessagesByDeliveryType(upperThresholdTimestamp, lastCursor)
	if err != nil {
		log.Error().Msgf("Error while fetching DELIVERING messages from MongoDb: %v", err)
		return
	}
	log.Debug().Msgf("Found %d DELIVERING messages in MongoDb", len(dbMessages))

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
