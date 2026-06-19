// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"errors"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/tracing"
)

// errAbort signals that batch processing should stop without logging an additional error.
var errAbort = errors.New("abort processing")

func CheckFailedEvents() {
	log.Debug().Msgf("FailedHandler: Republish messages in state FAILED")

	ctx := cache.HandlerCache.NewLockContext(context.Background())

	if acquired, err := cache.HandlerCache.TryLockWithTimeout(ctx, cache.FailedLockKey, 100*time.Millisecond); err != nil {
		log.Error().Err(err).Msgf("Error acquiring lock for FailedHandler entry: %s", cache.FailedLockKey)
		return
	} else if !acquired {
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

	var lastTimestamp any

	for {
		var newTimestamp any
		dbMessages, newTimestamp, err = mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), lastTimestamp)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching FAILED messages from MongoDb")
			return
		}
		lastTimestamp = newTimestamp

		if len(dbMessages) == 0 {
			return
		}

		for _, dbMessage := range dbMessages {
			if err := republishFailedMessage(&dbMessage, picker); err != nil {
				return
			}
		}

		if len(dbMessages) < int(batchSize) {
			break
		}
	}
}

func republishFailedMessage(dbMessage *message.StatusMessage, picker kafka.MessagePicker) error {
	subscriptionId := dbMessage.SubscriptionId

	subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching republishing entry for subscriptionId %s", subscriptionId)
		return err
	}

	if subscription == nil {
		return nil
	}

	if !isSSEDeliveryType(subscription.Spec.Subscription.DeliveryType) {
		return nil
	}

	if dbMessage.Coordinates == nil {
		log.Warn().Msgf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)
		return errAbort
	}

	kafkaMessage, err := picker.Pick(dbMessage)
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching message from kafka for subscriptionId %s", subscriptionId)
		return err
	}

	traceCtx := startRepublishTrace(kafkaMessage, dbMessage)

	err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, "SERVER_SENT_EVENT", "", true)
	if err != nil {
		log.Error().Err(err).Msgf("Error while republishing message for subscriptionId %s", subscriptionId)
		return err
	}

	log.Debug().Msgf("Successfully republished message in state FAILED for subscriptionId %s", subscriptionId)
	return nil
}

func isSSEDeliveryType(deliveryType enum.DeliveryType) bool {
	return deliveryType == "sse" || deliveryType == "server_sent_event"
}

func startRepublishTrace(kafkaMessage *sarama.ConsumerMessage, dbMessage *message.StatusMessage) *tracing.TraceContext {
	b3Ctx := tracing.WithB3FromMessage(context.Background(), kafkaMessage)
	traceCtx := tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

	traceCtx.StartSpan("republish failed message")
	traceCtx.SetAttribute("component", "Horizon Golaris")
	traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
	traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
	traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
	traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))

	return traceCtx
}
