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

func CheckFailedEvents() {
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

	for {
		var lastCursor any

		dbMessages, _, err := mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription from db")
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
