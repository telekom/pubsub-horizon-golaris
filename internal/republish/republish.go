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
	"github.com/telekom/pubsub-horizon-go/tracing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"strings"
	"time"
)

var republishPendingEventsFunc = RepublishPendingEvents

// register the data type RepublishingCacheEntry to gob for encoding and decoding of binary data
func init() {
	gob.Register(RepublishingCacheEntry{})
}

// HandleRepublishingEntry manages the republishing process for a given subscription.
// The function takes a SubscriptionResource object as a parameter.
func HandleRepublishingEntry(subscription *resource.SubscriptionResource) {
	var acquired = false
	ctx := cache.RepublishingCache.NewLockContext(context.Background())
	subscriptionId := subscription.Spec.Subscription.SubscriptionId

	// Get the republishing entry from the cache
	republishingEntry, err := cache.RepublishingCache.Get(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving RepublishingCacheEntry for subscriptionId %s", subscriptionId)
		return
	}

	if republishingEntry == nil {
		log.Debug().Msgf("No RepublishingCacheEntry found for subscriptionId %s", subscriptionId)
		return
	}

	castedRepublishCacheEntry, ok := republishingEntry.(RepublishingCacheEntry)
	if !ok {
		log.Error().Msgf("Error casting republishing entry for subscriptionId %s", subscriptionId)
		return
	}

	// If republishing entry is postponed in the future skip entry
	if (castedRepublishCacheEntry.PostponedUntil != time.Time{}) && time.Now().Before(castedRepublishCacheEntry.PostponedUntil) {
		log.Debug().Msgf("Postponed republishing for subscription %s until %s", subscriptionId, castedRepublishCacheEntry.PostponedUntil)
		return
	}

	// Attempt to acquire a lock on the republishing entry
	if acquired, _ = cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for RepublishingCacheEntry, skipping entry for subscriptionId %s", subscriptionId)
		return
	}
	log.Debug().Msgf("Successfully locked RepublishingCacheEntry with subscriptionId %s", subscriptionId)

	// Ensure that the lock is released if acquired before when the function is ended
	defer func() {
		if acquired {
			err := Unlock(ctx, subscriptionId)
			if err != nil {
				log.Debug().Msgf("Failed to unlock RepublishingCacheEntry with subscriptionId %s and error %v", subscriptionId, err)
			}
			log.Debug().Msgf("Successfully unlocked RepublishingCacheEntry with subscriptionId %s", subscriptionId)
		}
	}()

	republishPendingEventsFunc(subscription, castedRepublishCacheEntry)

	// Delete the republishing entry after processing
	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCacheEntry with subscriptionId %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully processed RepublishingCacheEntry with subscriptionId %s", subscriptionId)
}

// RepublishPendingEvents handles the republishing of pending events for a given subscription.
// The function fetches waiting events from the database and republishes them to Kafka.
// The function takes a subscriptionId as a parameter.
func RepublishPendingEvents(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) {
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
		if republishEntry.OldDeliveryType == "sse" || republishEntry.OldDeliveryType == "server_sent_event" {
			dbMessages, err = mongo.CurrentConnection.FindProcessedMessagesByDeliveryTypeSSE(time.Now(), opts, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching PROCESSED messages for subscription %s from db", subscriptionId)
			}
			log.Debug().Msgf("Found %d PROCESSED messages in MongoDb", len(dbMessages))
		} else {
			dbMessages, err = mongo.CurrentConnection.FindWaitingMessages(time.Now(), opts, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
			}
			log.Debug().Msgf("Found %d WAITING messages in MongoDb", len(dbMessages))
		}

		if len(dbMessages) == 0 {
			break
		}

		// Iterate over each message to republish
		for _, dbMessage := range dbMessages {
			log.Debug().Msgf("Republishing message for subscriptionId %s: %+v", subscriptionId, dbMessage)
			if cache.SubscriptionCancelMap[subscriptionId] {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return
			}

			var newDeliveryType string
			if !strings.EqualFold(string(subscription.Spec.Subscription.DeliveryType), string(dbMessage.DeliveryType)) {
				newDeliveryType = strings.ToUpper(string(subscription.Spec.Subscription.DeliveryType))
			}

			var newCallbackUrl string
			if subscription.Spec.Subscription.Callback != "" && (subscription.Spec.Subscription.Callback != dbMessage.Properties["callbackUrl"]) {
				newCallbackUrl = subscription.Spec.Subscription.Callback
			}

			if dbMessage.Coordinates == nil {
				log.Error().Msgf("Coordinates in message for subscriptionId %s are nil: %+v", subscriptionId, dbMessage)
				continue
			}

			kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
			if err != nil {
				log.Warn().Msgf("Error while fetching message from kafka for subscriptionId %s", subscriptionId)
				continue
			}

			var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
			var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

			traceCtx.StartSpan("republish message")
			traceCtx.SetAttribute("component", "Horizon Golaris")
			traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
			traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
			traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
			traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))

			err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, newDeliveryType, newCallbackUrl)
			if err != nil {
				log.Warn().Msgf("Error while republishing message for subscriptionId %s", subscriptionId)
				traceCtx.CurrentSpan().RecordError(err)
			}
			log.Debug().Msgf("Successfully republished message for subscriptionId %s", subscriptionId)

			traceCtx.EndCurrentSpan()
		}

		// If the number of fetched messages is less than the batch size, exit the loop
		// as there are no more messages to fetch
		if len(dbMessages) < int(batchSize) {
			break
		}
		page++
	}
}

// ForceDelete attempts to forcefully delete a RepublishingCacheEntry for a given subscriptionId.
// The function takes two parameters:
// - subscriptionId: a string representing the subscriptionId of the cache entry to be deleted.
// - ctx: a context.Context object for managing timeouts and cancellation signals.
func ForceDelete(ctx context.Context, subscriptionId string) {
	// Check if the entry is locked
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error checking if RepublishingCacheEntry is locked for subscriptionId %s", subscriptionId)
	}

	// If locked, unlock it
	if isLocked {
		err = cache.RepublishingCache.ForceUnlock(ctx, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error force-unlocking RepublishingCacheEntry for subscriptionId %s", subscriptionId)
		}
	}

	// Delete the entry
	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCacheEntry for subscriptionId %s", subscriptionId)
	}

	log.Debug().Msgf("Successfully deleted RepublishingCacheEntry for subscriptionId %s", subscriptionId)
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
