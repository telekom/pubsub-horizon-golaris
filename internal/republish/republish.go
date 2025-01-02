// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"encoding/gob"
	"errors"
	"github.com/1pkg/gohalt"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"github.com/telekom/pubsub-horizon-go/resource"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"net"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/throttling"
	"strings"
	"time"
)

var republishPendingEventsFunc = republishPendingEvents

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
			err = Unlock(ctx, subscriptionId)
			if err != nil {
				log.Debug().Msgf("Failed to unlock RepublishingCacheEntry with subscriptionId %s and error %v", subscriptionId, err)
			}
			log.Debug().Msgf("Successfully unlocked RepublishingCacheEntry with subscriptionId %s", subscriptionId)
		}
	}()

	err = republishPendingEventsFunc(subscription, castedRepublishCacheEntry)
	if err != nil {
		log.Error().Err(err).Msgf("Error while republishing pending events for subscriptionId %s. Discarding rebublishing cache entry", subscriptionId)
		return // republishingEntry from the cache won't be deleted
	}

	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCacheEntry with subscriptionId %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully processed RepublishingCacheEntry with subscriptionId %s", subscriptionId)
}

// RepublishEvent handles the republishing of a single event for a given subscription, using a picker instance.
// The function tries to pick the event message from Kafka using the given picker instance and republishes the event
// with an updated callbackUrl and deliveryType
// The function takes three parameters:
// - picker: a kafka picker instance
// - dbMessage: a reference to the status entry from the database
// - subscription: an optional reference to the subscription resource, which will be used to check for callbackUrl and deliveryType changes (can be nil)
func RepublishEvent(picker kafka.MessagePicker, dbMessage *message.StatusMessage, subscription *resource.SubscriptionResource) error {
	subscriptionId := dbMessage.SubscriptionId

	if dbMessage.Coordinates == nil {
		log.Warn().Msgf("Coordinates in message for subscriptionId %s are nil: %v", dbMessage.SubscriptionId, dbMessage)

		return nil
	}

	kafkaMessage, err := picker.Pick(dbMessage)
	if err != nil {
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

	newDeliveryType, newCallbackUrl := checkForSubscriptionUpdates(dbMessage, subscription)

	if err := kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, newDeliveryType, newCallbackUrl, false); err != nil {
		log.Warn().Err(err).Msgf("Error while republishing message for subscriptionId %s: %v", subscriptionId, err)
		traceCtx.CurrentSpan().RecordError(err)

		return err
	}

	log.Debug().Msgf("Successfully republished message for subscriptionId %s", subscriptionId)

	return nil
}

// republishPendingEvents handles the republishing of pending events for a given subscription.
// The function fetches waiting events from the database and calls RepublishEvent for each entry.
// The function takes two parameters:
// - subscription: a reference to the subscription resource
// - republishEntry: an entry of the republishing cache
func republishPendingEvents(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
	var subscriptionId = subscription.Spec.Subscription.SubscriptionId
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	picker, err := kafka.NewPicker()

	// Returning an error results in NOT deleting the republishingEntry from the cache
	// so that the republishing job will get retried shortly
	if err != nil {
		log.Error().Err(err).Fields(map[string]any{
			"subscriptionId": subscriptionId,
		}).Msg("Could not create picker for subscription")
		return err
	}
	defer picker.Close()

	throttler := throttling.CreateSubscriptionAwareThrottler(subscription)
	defer throttler.Release()

	cache.SetCancelStatus(subscriptionId, false)

	var cursor any
	for {
		if cache.GetCancelStatus(subscriptionId) {
			log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
			return nil
		}

		dbMessages, c := findPendingDBMessages(subscriptionId, republishEntry.OldDeliveryType, cursor)
		cursor = c

		if len(dbMessages) == 0 {
			break
		}

		for _, dbMessage := range dbMessages {
			throttler.Throttle()

			if cache.GetCancelStatus(subscriptionId) {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return nil
			}

			if err := RepublishEvent(picker, &dbMessage, subscription); err != nil {
				// Returning an error results in NOT deleting the republishingEntry from the cache
				// so that the republishing job will get retried shortly
				return err
			}
		}
	}

	return nil
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

// checkForSubscriptionUpdates returns the new updated delivery type and callback URL for a subscription
// The function compares the delivery type and callback URL from database entry with those from the subscription resource
// and return the changes. If the subscription resource is not passed, the function tries to fetch it from the cache.
// The function takes three parameters:
// - dbMessage: a reference to the status entry from the database
// - subscription: an optional reference to the subscription resource, which will be used to check for callbackUrl and deliveryType changes (can be nil)
func checkForSubscriptionUpdates(dbMessage *message.StatusMessage, subscription *resource.SubscriptionResource) (newDeliveryType string, newCallbackUrl string) {
	subscriptionId := dbMessage.SubscriptionId

	if subscription == nil {
		subscription, _ = cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	}

	if subscription != nil {
		if !strings.EqualFold(string(subscription.Spec.Subscription.DeliveryType), string(dbMessage.DeliveryType)) {
			newDeliveryType = strings.ToUpper(string(subscription.Spec.Subscription.DeliveryType))
		}

		if subscription.Spec.Subscription.Callback != "" && (subscription.Spec.Subscription.Callback != dbMessage.Properties["callbackUrl"]) {
			newCallbackUrl = subscription.Spec.Subscription.Callback
		}
	}

	return
}

func findPendingDBMessages(subscriptionId string, oldDeliveryType string, cursor any) (dbMessages []message.StatusMessage, newCursor any) {
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
