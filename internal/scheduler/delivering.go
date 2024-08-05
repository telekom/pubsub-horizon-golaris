package scheduler

import (
	"context"
	"encoding/gob"
	"github.com/rs/zerolog/log"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
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
	lockCtx := cache.DeliveringHandler.NewLockContext(context.Background())

	if err := cache.DeliveringHandler.Lock(lockCtx, config.Current.Handler.Delivering); err != nil {
		log.Error().Err(err).Msgf("Error while locking DeliveringHandler entry: %v", err)
		return
	}
	log.Info().Msg("Lock acquired for DeliveringHandler")

	defer func() {
		if err := cache.DeliveringHandler.Unlock(lockCtx, config.Current.Handler.Delivering); err != nil {
			log.Error().Err(err).Msg("Error while unlocking DeliveringHandler")
		}
	}()

	value, err := cache.DeliveringHandler.Get(lockCtx, config.Current.Handler.Delivering)
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching DeliveringHandler entry: %v", err)
		return
	}

	if value == nil {
		if err = cache.DeliveringHandler.Set(lockCtx, config.Current.Handler.Delivering, NewDeliveringEntry(config.Current.Handler.Delivering)); err != nil {
			log.Error().Err(err).Msgf("Error while setting DeliveringHandler entry: %v", err)
			return
		}
	}

}

//func checkDeliveringEvents() {
//	var acquired = false
//	lockKey := cache.DeliveringLockKey
//
//	ctx := cache.DeliveringHandler.NewLockContext(context.Background())
//
//	if acquired, _ = cache.DeliveringHandler.TryLockWithTimeout(ctx, config.Current.Handler.Delivering, 10*time.Millisecond); !acquired {
//		log.Debug().Msgf("Could not acquire lock for DeliveringHandler entry: %s", lockKey)
//		return
//	}
//
//	defer func() {
//		if acquired {
//			if err := cache.DeliveringHandler.Unlock(ctx, config.Current.Handler.Delivering); err != nil {
//				log.Error().Err(err).Msg("Error unlocking DeliveringHandler")
//			}
//			log.Info().Msg("Lock released for DeliveringHandler")
//		}
//	}()
//
//	batchSize := config.Current.Republishing.BatchSize
//	page := int64(0)
//
//	deliveringStatesOffsetMins := config.Current.Republishing.DeliveringStatesOffsetMins
//	upperThresholdTimestamp := time.Now().Add(-deliveringStatesOffsetMins * time.Minute)
//	var lastCursor any
//
//	dbMessages, lastCursor, err := mongo.CurrentConnection.FindDeliveringMessagesByDeliveryType(upperThresholdTimestamp, lastCursor)
//	if err != nil {
//		log.Error().Msgf("Error while fetching DELIVERING messages from MongoDb: %v", err)
//		return
//	}
//	log.Debug().Msgf("Found %d DELIVERING messages in MongoDb", len(dbMessages))
//
//	if len(dbMessages) == 0 {
//		return
//	}
//
//	for _, dbMessage := range dbMessages {
//
//		if dbMessage.Coordinates == nil {
//			log.Printf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)
//			return
//		}
//
//		kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage)
//		if err != nil {
//			log.Printf("Error while fetching message from kafka for subscriptionId %s: %v", dbMessage.SubscriptionId, err)
//			return
//		}
//
//		var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
//		var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)
//
//		traceCtx.StartSpan("republish delivering message")
//		traceCtx.SetAttribute("component", "Horizon Golaris")
//		traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
//		traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
//		traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
//		traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))
//
//		err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, "", "", false)
//		if err != nil {
//			log.Printf("Error while republishing message for subscriptionId %s: %v", dbMessage.SubscriptionId, err)
//			return
//		}
//		log.Printf("Successfully republished message for subscriptionId %s", dbMessage.SubscriptionId)
//
//		if len(dbMessages) < int(batchSize) {
//			break
//		}
//		page++
//	}
//}
