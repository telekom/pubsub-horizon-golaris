// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"
)

func CheckDeliveringEvents() {
	log.Debug().Msgf("Republish messages in state DELIVERING")

	var ctx = cache.HandlerCache.NewLockContext(context.Background())

	if acquired, _ := cache.HandlerCache.TryLockWithTimeout(ctx, cache.DeliveringLockKey, 100*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for DeliveringHandler entry: %s", cache.DeliveringLockKey)
		return
	}

	defer func() {
		if err := cache.HandlerCache.Unlock(ctx, cache.DeliveringLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking DeliveringHandler")
		}
	}()

	batchSize := config.Current.Republishing.BatchSize

	upperThresholdTimestamp := time.Now().Add(-config.Current.Republishing.DeliveringStatesOffset)

	picker, err := kafka.NewPicker()
	if err != nil {
		log.Error().Err(err).Msg("Could not initialize picker for handling events in state DELIVERING")
		return
	}
	defer picker.Close()

	for {
		var lastCursor any

		dbMessages, lastCursor, err := mongo.CurrentConnection.FindDeliveringMessagesByDeliveryType(upperThresholdTimestamp, lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching DELIVERING messages from MongoDb")
			return
		}

		if len(dbMessages) == 0 {
			return
		}
		log.Debug().Msgf("Found %d DELIVERING messages in MongoDb", len(dbMessages))

		for _, dbMessage := range dbMessages {

			if dbMessage.Coordinates == nil {
				log.Warn().Msgf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)
				return
			}

			message, err := picker.Pick(&dbMessage)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching message from kafka for subscriptionId %s", dbMessage.SubscriptionId)
				return
			}

			var b3Ctx = tracing.WithB3FromMessage(context.Background(), message)
			var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

			traceCtx.StartSpan("republish delivering message")
			traceCtx.SetAttribute("component", "Horizon Golaris")
			traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
			traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
			traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
			traceCtx.SetAttribute("uuid", string(message.Key))

			err = kafka.CurrentHandler.RepublishMessage(traceCtx, message, "", "", false)
			if err != nil {
				log.Error().Err(err).Msgf("Error while republishing message for subscriptionId %s", dbMessage.SubscriptionId)
				return
			}
			log.Debug().Msgf("Successfully republished message in state DELIVERING for subscriptionId %s", dbMessage.SubscriptionId)
		}

		if len(dbMessages) < int(batchSize) {
			break
		}
	}
}
