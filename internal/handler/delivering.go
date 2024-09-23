// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package handler

import (
	"context"
	"github.com/rs/zerolog/log"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/republish"
	"time"
)

func CheckDeliveringEvents() {
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

	for {
		var lastCursor any

		dbMessages, lastCursor, err := mongo.CurrentConnection.FindDeliveringMessagesByDeliveryType(upperThresholdTimestamp, lastCursor)
		if err != nil {
			log.Error().Msgf("Error while fetching DELIVERING messages from MongoDb: %v", err)
			return
		}

		if len(dbMessages) == 0 {
			return
		}

		log.Debug().Msgf("Found %d DELIVERING messages in MongoDb", len(dbMessages))

		for _, dbMessage := range dbMessages {
			subscriptionId := dbMessage.SubscriptionId

			subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
			if err != nil {
				log.Printf("Error while fetching republishing entry for subscriptionId %s: %v", subscriptionId, err)

				return
			}

			republish.HandleRepublishingEntry(subscription)

		}

		if len(dbMessages) < int(config.Current.Republishing.BatchSize) {
			break
		}
	}
}
