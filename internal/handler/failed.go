// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

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

func CheckFailedEvents() {
	log.Info().Msgf("Republish messages in state FAILED")

	var ctx = cache.HandlerCache.NewLockContext(context.Background())

	if acquired, _ := cache.HandlerCache.TryLockWithTimeout(ctx, cache.FailedLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for FailedHandler entry: %s", cache.FailedLockKey)
		return
	}

	defer func() {
		if err := cache.HandlerCache.Unlock(ctx, cache.FailedLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking FailedHandler")
		}
	}()

	batchSize := config.Current.Republishing.BatchSize

	var dbMessages []message.StatusMessage
	var err error

	picker, err := kafka.NewPicker()
	if err != nil {
		log.Error().Err(err).Msg("Could not initialize picker for handling events in state FAILED")
		return
	}
	defer picker.Close()

	for {
		var lastCursor any
		dbMessages, _, err = mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching FAILED messages from MongoDb")
			return
		}

		if len(dbMessages) == 0 {
			return
		}

		for _, dbMessage := range dbMessages {
			subscriptionId := dbMessage.SubscriptionId

			subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching republishing entry for subscriptionId %s", subscriptionId)
				return
			}

			if subscription != nil {
				if subscription.Spec.Subscription.DeliveryType == "sse" || subscription.Spec.Subscription.DeliveryType == "server_sent_event" {
					var newDeliveryType = "SERVER_SENT_EVENT"

					if dbMessage.Coordinates == nil {
						log.Warn().Msgf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)
						return
					}

					kafkaMessage, err := picker.Pick(&dbMessage)
					if err != nil {
						log.Error().Err(err).Msgf("Error while fetching message from kafka for subscriptionId %s", subscriptionId)
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
						log.Error().Err(err).Msgf("Error while republishing message for subscriptionId %s", subscriptionId)
						return
					}
					log.Debug().Msgf("Successfully republished message in state FAILED for subscriptionId %s", subscriptionId)

				}
			}
		}

		if len(dbMessages) < int(batchSize) {
			break
		}
	}
}
