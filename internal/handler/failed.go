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

func CheckFailedEvents() {
	log.Info().Msgf("Republish messages in state FAILED")

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

	picker, err := kafka.NewPicker()
	if err != nil {
		log.Error().Err(err).Msg("Could not initialize picker for handling events in state FAILED")

		return
	}
	defer picker.Close()

	for {
		var lastCursor any

		dbMessages, _, err := mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription from database")
			return
		}

		if len(dbMessages) == 0 {
			return
		}

		log.Debug().Msgf("Found %d FAILED messages in database", len(dbMessages))

		for _, dbMessage := range dbMessages {
			subscriptionId := dbMessage.SubscriptionId

			subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching republishing entry for subscriptionId %s", subscriptionId)
				return
			}

			if subscription != nil && (subscription.Spec.Subscription.DeliveryType == "sse" || subscription.Spec.Subscription.DeliveryType == "server_sent_event") {
				if err := republish.RepublishEvent(picker, &dbMessage, subscription); err != nil {
					log.Error().Err(err).Msgf("Error while republishing message for subscriptionId %s", dbMessage.SubscriptionId)

					continue
				}

				log.Debug().Msgf("Successfully republished message in state FAILED for subscriptionId %s", dbMessage.SubscriptionId)
			}
		}

		if len(dbMessages) < int(config.Current.Republishing.BatchSize) {
			break
		}
	}
}
