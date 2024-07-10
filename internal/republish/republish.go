// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"encoding/gob"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/cache"
	"golaris/internal/config"
	"time"
)

var mongoHandler MongoHandler
var kafkaHandler KafkaHandler

var republishWaitingEventsFunc = republishWaitingEvents

// register the data type RepublishingCache to gob for encoding and decoding of binary data
func init() {
	gob.Register(RepublishingCache{})
}

// HandleRepublishingEntry manages the republishing process for a given subscription.
// The function takes a SubscriptionResource object as a parameter.
func HandleRepublishingEntry(subscription *resource.SubscriptionResource) {
	var acquired = false
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

	// Attempt to acquire a lock on the republishing entry
	if acquired, _ = cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for republishing entry with subscriptionId %s in time, skipping entry", subscriptionId)
		return
	}

	// Ensure that the lock is released if acquired before when the function is ended
	defer func() {
		if acquired == true {
			err := Unlock(ctx, subscriptionId)
			if err != nil {
				log.Debug().Msgf("Failed to unlock RepublishingCache entry with subscriptionId %s and error %v", subscriptionId, err)
			}
			log.Debug().Msgf("Successfully unlocked RepublishingCache entry with subscriptionId %s", subscriptionId)
		}
	}()

	republishWaitingEventsFunc(subscriptionId)

	// Delete the republishing entry after processing
	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting republishing entry with subscriptionId %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully proccessed republishing entry with subscriptionId %s", subscriptionId)
}

// republishWaitingEvents handles the republishing of pending events for a given subscription.
// The function fetches waiting events from the database and republishes them to Kafka.
// The function takes a subscriptionId as a parameter.
func republishWaitingEvents(subscriptionId string) {
	log.Debug().Msgf("Republishing waiting events for subscription %s", subscriptionId)

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	// Start a loop to paginate through the events
	for {
		opts := options.Find().
			SetLimit(batchSize).
			// Skip the number of events already processed
			SetSkip(page * batchSize).
			SetSort(bson.D{{Key: "timestamp", Value: 1}})

		//Get Waiting events from database pageable!
		dbMessages, err := mongoHandler.FindWaitingMessages(time.Now(), opts, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscriptionId %s from db", subscriptionId)
		}

		log.Debug().Msgf("Found %d messages in MongoDb", len(dbMessages))
		log.Debug().Msgf("dbMessages: %v", dbMessages)

		if len(dbMessages) == 0 {
			break
		}

		// Iterate over each message to republish
		for _, dbMessage := range dbMessages {
			log.Debug().Msgf("Republishing message for subscriptionId %s: %v", subscriptionId, dbMessage)

			if dbMessage.Coordinates == nil {
				log.Error().Msgf("Coordinates in message for subscriptionId %s are nil: %v", subscriptionId, dbMessage)
				continue
			}

			kafkaMessage, err := kafkaHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
			if err != nil {
				log.Warn().Msgf("Error while fetching message from kafka for subscriptionId %s", subscriptionId)
				continue
			}
			err = kafkaHandler.RepublishMessage(kafkaMessage)
			if err != nil {
				log.Warn().Msgf("Error while republishing message for subscriptionId %s", subscriptionId)
			}
			log.Debug().Msgf("Successfully republished message for subscriptionId %s", subscriptionId)
		}

		// If the number of fetched messages is less than the batch size, exit the loop
		// as there are no more messages to fetch
		if len(dbMessages) < int(batchSize) {
			break
		}
		page++
	}
}

// ForceDelete attempts to forcefully delete a RepublishingCache entry for a given subscriptionId.
// The function takes two parameters:
// - subscriptionId: a string representing the subscriptionId of the cache entry to be deleted.
// - ctx: a context.Context object for managing timeouts and cancellation signals.
func ForceDelete(ctx context.Context, subscriptionId string) {
	// Check if the entry is locked
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error checking if RepublishingCache entry is locked for subscriptionId %s", subscriptionId)
	}

	// If locked, unlock it
	if isLocked {
		err = cache.RepublishingCache.ForceUnlock(ctx, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error force-unlocking RepublishingCache entry for subscriptionId %s", subscriptionId)
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

func Unlock(ctx context.Context, subscriptionId string) error {
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		return err
	}
	if isLocked {
		if err := cache.RepublishingCache.Unlock(ctx, subscriptionId); err != nil {
			return err
		}
	}
	return nil
}
