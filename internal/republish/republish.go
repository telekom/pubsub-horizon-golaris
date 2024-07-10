// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"encoding/gob"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mongo"
	"time"
)

func init() {
	gob.Register(RepublishingCache{})
}

func HandleRepublishingEntry(subscription *resource.SubscriptionResource) {
	ctx := cache.RepublishingCache.NewLockContext(context.Background())

	subscriptionId := subscription.Spec.Subscription.SubscriptionId
	republishingEntry, err := cache.RepublishingCache.Get(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving republishing cache entry for subscriptionId %s", subscriptionId)
	}

	if republishingEntry == nil {
		log.Debug().Msgf("No republishing entry found for subscriptionId %s", subscriptionId)
		return
	}

	// Attempt to acquire a lock for the health check key
	if acquired, _ := cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for republishing entry with subscriptionId %s, skipping entry", subscriptionId)
		return
	}

	republishCache, ok := republishingEntry.(RepublishingCache)
	if !ok {
		log.Error().Msgf("Error casting republishing entry for subscriptionId %s", subscriptionId)
		return
	}

	RepublishPendingEvents(subscription, republishCache)

	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting republishing entry with subscriptionId %s", subscriptionId)
		return
	}
	log.Debug().Msgf("Successfully proccessed republishing entry with subscriptionId %s", subscriptionId)
}

func RepublishPendingEvents(subscription *resource.SubscriptionResource, repulishEntry RepublishingCache) {
	var subscriptionId = subscription.Spec.Subscription.SubscriptionId

	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	cache.CancelMapMutex.Lock()
	defer cache.CancelMapMutex.Unlock()

	cache.SubscriptionCancelMap[subscriptionId] = false

	// Start a loop to paginate through the events
	for {
		if cache.SubscriptionCancelMap[subscriptionId] {
			log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
			return
		}

		opts := options.Find().
			SetLimit(batchSize).
			// Skip the number of events already processed
			SetSkip(page * batchSize).
			SetSort(bson.D{{Key: "timestamp", Value: 1}})

		var dbMessages []message.StatusMessage
		var err error
		if repulishEntry.OldDeliveryType == "sse" || repulishEntry.OldDeliveryType == "server_sent_event" {
			dbMessages, err = mongo.CurrentConnection.FindProcessedMessagesByDeliveryTypeSSE(time.Now(), opts, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching PROCESSED messages for subscription %s from db", subscriptionId)
			}
		} else {
			dbMessages, err = mongo.CurrentConnection.FindWaitingMessages(time.Now(), opts, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
			}
		}

		log.Info().Msgf("Found %d event states in MongoDb", len(dbMessages))
		log.Debug().Msgf("dbMessages: %v", dbMessages)

		if len(dbMessages) == 0 {
			break
		}

		// Iterate over each message to republish
		for _, dbMessage := range dbMessages {
			if cache.SubscriptionCancelMap[subscriptionId] {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return
			}

			// TODo: Delete callbackUrl
			var newDeliveryType string
			if subscription.Spec.Subscription.DeliveryType != dbMessage.DeliveryType {
				newDeliveryType = string(subscription.Spec.Subscription.DeliveryType)
			}

			var newCallbackUrl string
			if subscription.Spec.Subscription.Callback != dbMessage.Properties["callbackUrl"] {
				newCallbackUrl = subscription.Spec.Subscription.Callback
			}

			log.Debug().Msgf("Republishing message for subscription %s: %v", subscriptionId, dbMessage)

			if dbMessage.Coordinates == nil {
				log.Error().Msgf("Coordinates in message for subscription %s are nil: %v", subscriptionId, dbMessage)
				continue
			}

			kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
			if err != nil {
				log.Warn().Msgf("Error while fetching message from kafka for subscription %s", subscriptionId)
				continue
			}
			err = kafka.CurrentHandler.RepublishMessage(kafkaMessage, newDeliveryType, newCallbackUrl)
			if err != nil {
				log.Warn().Msgf("Error while republishing message for subscription %s", subscriptionId)
			}
			log.Debug().Msgf("Successfully republished message for subscription %s", subscriptionId)
		}

		// If the number of fetched messages is less than the batch size, exit the loop
		if len(dbMessages) < int(batchSize) {
			break
		}

		// Increment the page number for the next iteration
		page++
	}
}

// ForceDelete attempts to forcefully delete a RepublishingCache entry for a given subscriptionId.
// The function first checks if the cache entry is locked. If it is, it attempts to unlock it.
// After ensuring the entry is not locked, it attempts to delete the cache entry.
// If any of these operations (checking lock status, unlocking, deleting) fail, the function logs the error.
// The function takes two parameters:
// - subscriptionId: a string representing the subscriptionId of the cache entry to be deleted.
// - ctx: a context.Context object for managing timeouts and cancellation signals.
// This function does not return a value.
func ForceDelete(subscriptionId string, ctx context.Context) {
	// Check if the entry is locked
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error checking if RepublishingCache entry is locked for subscriptionId %s", subscriptionId)
	}

	// If locked, unlock it
	if isLocked {
		err = cache.RepublishingCache.ForceUnlock(ctx, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error unlocking RepublishingCache entry for subscriptionId %s", subscriptionId)
		}
	}

	// Delete the entry
	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCache entry for subscriptionId %s", subscriptionId)
	}

	log.Debug().Msgf("Successfully deleted RepublishingCache entry for subscriptionId %s", subscriptionId)
	return
}
