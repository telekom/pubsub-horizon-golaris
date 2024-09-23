// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/throttling"
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
		err = Unlock(ctx, subscriptionId)
		if err != nil {
			log.Debug().Msgf("Failed to unlock RepublishingCacheEntry with subscriptionId %s and error %v", subscriptionId, err)
		}
		log.Debug().Msgf("Successfully unlocked RepublishingCacheEntry with subscriptionId %s", subscriptionId)
	}()

	republishPendingEventsFunc(subscription, castedRepublishCacheEntry)

	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCacheEntry with subscriptionId %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully processed RepublishingCacheEntry with subscriptionId %s", subscriptionId)
}

func findDBMessages(subscriptionId string, oldDeliveryType string, cursor any) (dbMessages []message.StatusMessage, newCursor any) {
	dbMessages = []message.StatusMessage{}
	var err error

	if oldDeliveryType == "sse" || oldDeliveryType == "server_sent_event" {
		dbMessages, newCursor, err = mongo.CurrentConnection.FindProcessedMessagesByDeliveryTypeSSE(time.Now(), cursor, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching PROCESSED messages for subscription %s from db", subscriptionId)
		}
		log.Debug().Msgf("Found %d PROCESSED messages in MongoDb", len(dbMessages))
	} else {
		dbMessages, newCursor, err = mongo.CurrentConnection.FindWaitingMessages(time.Now(), cursor, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
		}
		log.Debug().Msgf("Found %d WAITING messages in MongoDb", len(dbMessages))
	}

	log.Debug().Msgf("Last cursor: %v", newCursor)

	return
}

func republishEvent(dbMessage message.StatusMessage, subscription *resource.SubscriptionResource) error {
	subscriptionId := subscription.Spec.Subscription.SubscriptionId

	if dbMessage.Coordinates == nil {
		err := fmt.Errorf("Coordinates in message for subscriptionId %s are nil: %+v", subscriptionId, dbMessage)

		return err
	}

	kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage)
	if err != nil {
		err := fmt.Errorf("Error while fetching message from kafka for subscriptionId %s: %v", subscriptionId, err)

		return err
	}

	var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
	var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

	traceCtx.StartSpan("republish message")
	defer traceCtx.EndCurrentSpan()

	traceCtx.SetAttribute("component", "Horizon Golaris")
	traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
	traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
	traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
	traceCtx.SetAttribute("uuid", dbMessage.Uuid)

	var newDeliveryType string
	var newCallbackUrl string

	if !strings.EqualFold(string(subscription.Spec.Subscription.DeliveryType), string(dbMessage.DeliveryType)) {
		newDeliveryType = strings.ToUpper(string(subscription.Spec.Subscription.DeliveryType))
	}

	if subscription.Spec.Subscription.Callback != "" && (subscription.Spec.Subscription.Callback != dbMessage.Properties["callbackUrl"]) {
		newCallbackUrl = subscription.Spec.Subscription.Callback
	}

	err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, newDeliveryType, newCallbackUrl, false)
	if err != nil {
		log.Warn().Msgf("Error while republishing message for subscriptionId %s: %v", subscriptionId, err)
		traceCtx.CurrentSpan().RecordError(err)
	} else {
		log.Debug().Msgf("Successfully republished message for subscriptionId %s", subscriptionId)
	}

	return nil
}

// RepublishPendingEvents handles the republishing of pending events for a given subscription.
// The function fetches waiting events from the database and republishes them to Kafka.
// The function takes a subscriptionId as a parameter.
func RepublishPendingEvents(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) {
	var subscriptionId = subscription.Spec.Subscription.SubscriptionId
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	throttler := throttling.CreateSubscriptionAwareThrottler(subscription)
	defer throttler.Release()

	var cursor any
	for {
		if cache.GetCancelStatus(subscriptionId) {
			log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
			return
		}

		dbMessages, c := findDBMessages(subscriptionId, republishEntry.OldDeliveryType, cursor)
		cursor = c

		if len(dbMessages) == 0 {
			break
		}

		for _, dbMessage := range dbMessages {
			throttler.Throttle()

			if cache.GetCancelStatus(subscriptionId) {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return
			}

			err := republishEvent(dbMessage, subscription)
			if err != nil {
				log.Error().Err(err)
			}
		}

		if len(dbMessages) < int(config.Current.Republishing.BatchSize) {
			break
		}
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
		if err = cache.RepublishingCache.Unlock(ctx, subscriptionId); err != nil {
			return err
		}
	}
	return nil
}
