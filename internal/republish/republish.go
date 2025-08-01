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
	"strings"
	"time"
)

var republishPendingEventsFunc = RepublishPendingEvents

// register the data type RepublishingCacheEntry to gob for encoding and decoding of binary data
func init() {
	gob.Register(RepublishingCacheEntry{})
}

func createThrottler(redeliveriesPerSecond int, deliveryType string, subscriptionId string) gohalt.Throttler {
	if deliveryType == "sse" || deliveryType == "server_sent_event" || redeliveriesPerSecond <= 0 {
		log.Debug().Msgf("Throttling disabled for subscription %s with delivery type %s and redeliveries per second %d", subscriptionId, deliveryType, redeliveriesPerSecond)
		return gohalt.NewThrottlerEcho(nil)
	}
	log.Info().Msgf("Throttling enabled for subscription %s with delivery type %s and redeliveries per second %d", subscriptionId, deliveryType, redeliveriesPerSecond)
	return gohalt.NewThrottlerTimed(uint64(redeliveriesPerSecond), config.Current.Republishing.ThrottlingIntervalTime, 0)
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

	// Check if resource is already locked, then return early if, otherwise try to acquire lock
	isLocked, err := cache.RepublishingCache.IsLocked(ctx, subscriptionId)
	if err != nil {
		log.Error().Msgf("Failed to check locking state for RepublishingCacheEntry %s", subscriptionId)
	}
	if isLocked {
		log.Debug().Msgf("Could not acquire lock for RepublishingCacheEntry, skipping entry for subscriptionId %s", subscriptionId)
		return
	}
	if acquired, _ = cache.RepublishingCache.TryLockWithTimeout(ctx, subscriptionId, 100*time.Millisecond); !acquired {
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
		return
	}

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
func RepublishPendingEvents(subscription *resource.SubscriptionResource, republishEntry RepublishingCacheEntry) error {
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

	batchSize := config.Current.Republishing.BatchSize

	throttler := createThrottler(subscription.Spec.Subscription.RedeliveriesPerSecond, string(subscription.Spec.Subscription.DeliveryType), subscriptionId)
	defer throttler.Release(context.Background())

	cache.SetCancelStatus(subscriptionId, false)

	var lastTimestamp any
	for {
		if cache.GetCancelStatus(subscriptionId) {
			log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)

			return nil
		}

		var dbMessages []message.StatusMessage
		var err error

		if republishEntry.OldDeliveryType == "sse" || republishEntry.OldDeliveryType == "server_sent_event" {
			dbMessages, lastTimestamp, err = mongo.CurrentConnection.FindProcessedMessagesByDeliveryTypeSSE(time.Now(), lastTimestamp, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching PROCESSED messages for subscription %s from db", subscriptionId)
			}
			log.Debug().Msgf("Found %d PROCESSED messages in MongoDb", len(dbMessages))
		} else {
			dbMessages, lastTimestamp, err = mongo.CurrentConnection.FindWaitingMessages(time.Now(), lastTimestamp, subscriptionId)
			if err != nil {
				log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
			}
			log.Debug().Msgf("Found %d WAITING messages in MongoDb", len(dbMessages))
		}

		log.Debug().Msgf("Last cursor: %v", lastTimestamp)

		if len(dbMessages) == 0 {
			break
		}

		for _, dbMessage := range dbMessages {

			if cache.GetCancelStatus(subscriptionId) {
				log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
				return nil
			}

			for {
				if acquireResult := throttler.Acquire(context.Background()); acquireResult != nil {
					sleepInterval := time.Millisecond * 10
					totalSleepTime := config.Current.Republishing.ThrottlingIntervalTime
					for slept := time.Duration(0); slept < totalSleepTime; slept += sleepInterval {
						if cache.GetCancelStatus(subscriptionId) {
							log.Info().Msgf("Republishing for subscription %s has been cancelled", subscriptionId)
							return nil
						}

						time.Sleep(sleepInterval)
					}
					continue
				}
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
				log.Error().Msgf("Coordinates in message for subscriptionId %s are nil: %+v", subscriptionId, dbMessage)
				continue
			}

			kafkaMessage, err := picker.Pick(&dbMessage)
			if err != nil {
				// Returning an error results in NOT deleting the republishingEntry from the cache
				// so that the republishing job will get retried shortly
				var nErr *net.OpError
				if errors.As(err, &nErr) {
					return err
				}

				var errorList = []error{
					sarama.ErrEligibleLeadersNotAvailable,
					sarama.ErrPreferredLeaderNotAvailable,
					sarama.ErrUnknownLeaderEpoch,
					sarama.ErrFencedLeaderEpoch,
					sarama.ErrNotLeaderForPartition,
					sarama.ErrLeaderNotAvailable,
				}

				for _, e := range errorList {
					if errors.Is(err, e) {
						return err
					}
				}

				log.Error().Err(err).Msgf("Error while fetching message from kafka for subscriptionId %s", subscriptionId)
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
	}
	return nil
}

// ForceDelete attempts to forcefully delete a RepublishingCacheEntry for a given subscriptionId.
// The function takes two parameters:
// - subscriptionId: a string representing the subscriptionId of the cache entry to be deleted.
// - ctx: a context.Context object for managing timeouts and cancellation signals.
func ForceDelete(ctx context.Context, subscriptionId string) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Unlock it
	log.Debug().Msgf("Attempting to force unlock RepublishingCacheEntry for subscriptionId %s", subscriptionId)
	err := cache.RepublishingCache.ForceUnlock(ctxWithTimeout, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error force-unlocking RepublishingCacheEntry for subscriptionId %s", subscriptionId)
		return err
	}

	// Delete the entry
	log.Debug().Msgf("Attempting to delete RepublishingCacheEntry for subscriptionId %s", subscriptionId)
	err = cache.RepublishingCache.Delete(ctxWithTimeout, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error deleting RepublishingCacheEntry for subscriptionId %s", subscriptionId)
		return err
	}

	log.Debug().Msgf("Successfully deleted RepublishingCacheEntry for subscriptionId %s", subscriptionId)
	return nil
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
