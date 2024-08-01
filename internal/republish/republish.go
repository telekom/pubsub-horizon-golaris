// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"context"
	"encoding/gob"
	"github.com/1pkg/gohalt"
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
var throttler gohalt.Throttler

// register the data type RepublishingCache to gob for encoding and decoding of binary data
func init() {
	gob.Register(RepublishingCache{})
}

func createThrottler(redeliveriesPerSecond int, deliveryType string) gohalt.Throttler {
	if deliveryType == "sse" || deliveryType == "server_sent_event" || redeliveriesPerSecond <= 0 {
		return gohalt.NewThrottlerEcho(nil)
	}

	log.Info().Msgf("Creating throttler with %d redeliveries", redeliveriesPerSecond)
	return gohalt.NewThrottlerTimed(uint64(redeliveriesPerSecond), config.Current.Republishing.ThrottlingIntervalTime, 0)
}

// HandleRepublishingEntry manages the republishing process for a given subscription.
// The function takes a SubscriptionResource object as a parameter.
func HandleRepublishingEntry(subscription *resource.SubscriptionResource) {
	var acquired = false
	ctx := cache.RepublishingCache.NewLockContext(context.Background())

	subscriptionId := subscription.Spec.Subscription.SubscriptionId
	republishingEntry, err := cache.RepublishingCache.Get(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving RepublishingCache entry for subscriptionId %s", subscriptionId)
		return
	}

	if republishingEntry == nil {
		log.Debug().Msgf("No RepublishingCache entry found for subscriptionId %s", subscriptionId)
		return
	}

	// Attempt to acquire a lock on the republishing entry
	if acquired, _ = cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for RepublishingCache entry, skipping entry for subscriptionId %s", subscriptionId)
		return
	}
	log.Debug().Msgf("Successfully locked RepublishingCache entry with subscriptionId %s", subscriptionId)

	// Ensure that the lock is released if acquired before when the function is ended
	defer func() {
		if acquired {
			err = Unlock(ctx, subscriptionId)
			if err != nil {
				log.Debug().Msgf("Failed to unlock RepublishingCache entry with subscriptionId %s and error %v", subscriptionId, err)
			}
			log.Debug().Msgf("Successfully unlocked RepublishingCache entry with subscriptionId %s", subscriptionId)
		}
	}()

	republishCache, ok := republishingEntry.(RepublishingCache)
	if !ok {
		log.Error().Msgf("Error casting republishing entry for subscriptionId %s", subscriptionId)
		return
	}

	republishPendingEventsFunc(subscription, republishCache)

	err = cache.RepublishingCache.Delete(ctx, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCache entry with subscriptionId %s", subscriptionId)
		return
	}

	log.Debug().Msgf("Successfully processed RepublishingCache entry with subscriptionId %s", subscriptionId)
}

// RepublishPendingEvents handles the republishing of pending events for a given subscription.
// The function fetches waiting events from the database and republishes them to Kafka.
// The function takes a subscriptionId as a parameter.
func RepublishPendingEvents(subscription *resource.SubscriptionResource, republishEntry RepublishingCache) {
	var subscriptionId = subscription.Spec.Subscription.SubscriptionId
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	throttler = createThrottler(subscription.Spec.Subscription.RedeliveriesPerSecond, string(subscription.Spec.Subscription.DeliveryType))

	for {
		if cache.GetCancelStatus(subscriptionId) {
			log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
			return
		}

		opts := options.Find().SetLimit(batchSize).SetSkip(page * batchSize).SetSort(bson.D{{Key: "timestamp", Value: 1}})
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

		log.Debug().Msgf("Processing page %d with batch size %d", page, batchSize)
		log.Debug().Msgf("Found %d messages on page %d", len(dbMessages), page)

		for _, dbMessage := range dbMessages {
			if cache.GetCancelStatus(subscriptionId) {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return
			}

			for {
				if acquireResult := throttler.Acquire(context.Background()); acquireResult != nil {
					sleepInterval := time.Millisecond * 10
					totalSleepTime := config.Current.Republishing.ThrottlingIntervalTime
					for slept := time.Duration(0); slept < totalSleepTime; slept += sleepInterval {
						if cache.GetCancelStatus(subscriptionId) {
							log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
							return
						}

						time.Sleep(sleepInterval)
					}
					continue
				}
				defer throttler.Release(context.Background())
				break
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
				log.Printf("Coordinates in message for subscriptionId %s are nil: %v", subscriptionId, dbMessage)
				return
			}

			kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage)
			if err != nil {
				log.Printf("Error while fetching message from kafka for subscriptionId %s: %v", subscriptionId, err)
				return
			}

			var b3Ctx = tracing.WithB3FromMessage(context.Background(), kafkaMessage)
			var traceCtx = tracing.NewTraceContext(b3Ctx, "golaris", config.Current.Tracing.DebugEnabled)

			traceCtx.StartSpan("republish message")
			traceCtx.SetAttribute("component", "Horizon Golaris")
			traceCtx.SetAttribute("eventId", dbMessage.Event.Id)
			traceCtx.SetAttribute("eventType", dbMessage.Event.Type)
			traceCtx.SetAttribute("subscriptionId", dbMessage.SubscriptionId)
			traceCtx.SetAttribute("uuid", string(kafkaMessage.Key))

			err = kafka.CurrentHandler.RepublishMessage(traceCtx, kafkaMessage, newDeliveryType, newCallbackUrl, false)
			if err != nil {
				log.Warn().Msgf("Error while republishing message for subscriptionId %s: %v", subscriptionId, err)
				traceCtx.CurrentSpan().RecordError(err)
			}
			log.Debug().Msgf("Successfully republished message for subscriptionId %s", subscriptionId)

			traceCtx.EndCurrentSpan()
		}

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
		if err = cache.RepublishingCache.Unlock(ctx, subscriptionId); err != nil {
			return err
		}
	}
	return nil
}
