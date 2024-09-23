// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/rs/zerolog/log"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/republish"
	"time"
)

func CheckDeliveringEvents() {
	log.Info().Msgf("Republish messages in state DELIVERING")

	var ctx = cache.DeliveringHandler.NewLockContext(context.Background())

	if acquired, _ := cache.DeliveringHandler.TryLockWithTimeout(ctx, cache.DeliveringLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for DeliveringHandler entry: %s", cache.DeliveringLockKey)
		return
	}
	defer func() {
		if err := cache.DeliveringHandler.Unlock(ctx, cache.DeliveringLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking DeliveringHandler")
		}
	}()

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
			log.Error().Err(err).Msgf("Error while fetching DELIVERING messages from database")

			return
		}

		if len(dbMessages) == 0 {
			return
		}

		log.Debug().Msgf("Found %d DELIVERING messages in database", len(dbMessages))

		for _, dbMessage := range dbMessages {
			if err := republish.RepublishEvent(picker, &dbMessage, nil); err != nil {
				log.Error().Err(err).Msgf("Error while republishing message for subscriptionId %s", dbMessage.SubscriptionId)

				continue
			}

			log.Debug().Msgf("Successfully republished message in state DELIVERING for subscriptionId %s", dbMessage.SubscriptionId)
		}

		if len(dbMessages) < int(config.Current.Republishing.BatchSize) {
			break
		}
	}
}
