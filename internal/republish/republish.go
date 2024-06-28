// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mongo"
	"time"
)

func RepublishPendingEvents(subscriptionId string) {
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	batchSize := int64(config.Current.RepublishingBatchSize)
	page := int64(0)

	// Start a loop to paginate through the events
	for {
		pageable := options.Find().
			SetLimit(batchSize).
			// Skip the number of events already processed
			SetSkip(page * batchSize).
			SetSort(bson.D{{Key: "timestamp", Value: 1}})

		//Get Waiting events from database pageable!
		dbMessages, err := mongo.CurrentConnection.FindWaitingMessages(time.Now(), pageable, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
		}

		log.Info().Msgf("Found %d event states in MongoDb", len(dbMessages))
		log.Debug().Msgf("dbMessages: %v", dbMessages)

		if len(dbMessages) == 0 {
			break
		}

		// Iterate over each message to republish
		for _, dbMessage := range dbMessages {
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
			err = kafka.CurrentHandler.RepublishMessage(kafkaMessage)
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

// ForceDelete attempts to delete an entry from the RepublishingCache for a given subscriptionId.
// The function first tries to retrieve the entry from the cache. If the entry does not exist, the function returns true.
// If the entry exists, the function checks if it is locked. If it is locked, the function attempts to unlock it.
// After ensuring the entry is not locked, the function attempts to delete the entry from the cache.
// If any of the operations (getting, checking lock, unlocking, deleting) fail, the function logs the error and returns false.
// If the entry is successfully deleted, the function logs a success message and returns true.
//
// Parameters:
//   - subscriptionId: The ID of the subscription for which the cache entry should be deleted.
//   - ctx: The context within which the function should operate. This is typically used for timeout and cancellation signals.
//
// Returns:
//   - bool: Returns true if the operation is successful (either the entry did not exist or it was successfully deleted). Returns false if any operation fails.
func ForceDelete(subscriptionId string, ctx context.Context) bool {
	// Attempt to get the entry and check if it's locked
	entry, err := cache.RepublishingCache.Get(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error getting entry from RepublishingCache for subscriptionId %s", subscriptionId)
		return false
	}

	// If there is no entry, nothing to delete
	if entry == nil {
		log.Debug().Msgf("No RepublishingCache entry found for subscriptionId %s", subscriptionId)
		return true
	}

	// Check if the entry is locked
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error checking if RepublishingCache entry is locked for subscriptionId %s", subscriptionId)
		return false
	}

	// If locked, unlock it
	if isLocked {
		err = cache.RepublishingCache.ForceUnlock(ctx, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error unlocking RepublishingCache entry for subscriptionId %s", subscriptionId)
			return false
		}
	}

	// Delete the entry
	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCache entry for subscriptionId %s", subscriptionId)
		return false
	}

	log.Info().Msgf("Successfully deleted RepublishingCache entry for subscriptionId %s", subscriptionId)
	return true
}
