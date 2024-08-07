package handler

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/mongo"
	"pubsub-horizon-golaris/internal/republish"
	"time"
)

type ProcessResult struct {
	SubscriptionId string
	Error          error
}

// CheckWaitingEvents check for waiting events without an open CircuitBreaker and without a RepublishingEntry.
// Why we need this handler?
// --> When the Quasar cache shuts down, the CircuitBreakerCache is also deleted at the same time.
//
//	If the Quasar then starts up again and the customer's endpoint is accessible again,
//	it can happen that WAITING events are stuck and are no longer delivered because there is no longer a CircuitBreaker for these WAITING events.
func CheckWaitingEvents() {
	var ctx = cache.WaitingHandler.NewLockContext(context.Background())

	if acquired, _ := cache.WaitingHandler.TryLockWithTimeout(ctx, cache.WaitingLockKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for WaitingHandler entry: %s", cache.WaitingLockKey)
		return
	}

	defer func() {
		if err := cache.WaitingHandler.Unlock(ctx, cache.WaitingLockKey); err != nil {
			log.Error().Err(err).Msg("Error unlocking WaitingHandler")
		}
	}()

	var dbMessages []message.StatusMessage
	var lastCursor any
	var err error

	for {
		dbMessages, lastCursor, err = mongo.CurrentConnection.FindUniqueWaitingMessages(time.Now(), lastCursor)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching unique waiting messages from db")
			return
		}

		if len(dbMessages) == 0 {
			return
		}

		resultChan := make(chan ProcessResult, len(dbMessages))

		for _, dbMessage := range dbMessages {
			go processWaitingMessages(dbMessage, resultChan)
		}

		for range dbMessages {
			result := <-resultChan
			if result.Error != nil {
				log.Error().Err(result.Error).Msgf("Error while processing waiting messages for subscriptionId: %s", result.SubscriptionId)
			}
		}

		close(resultChan)
	}
}

func processWaitingMessages(dbMessage message.StatusMessage, resultChan chan<- ProcessResult) {
	var subscriptionId = dbMessage.SubscriptionId

	optionalRepublishingEntry, err := cache.RepublishingCache.Get(context.Background(), subscriptionId)
	if err != nil {
		resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: err}
		return
	}

	if optionalRepublishingEntry != nil {
		resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: nil}
		return
	}

	// 10 attempts to get the circuitBreakerMessage, because the Quasar needs some time to start up
	var optionalCBEntry *message.CircuitBreakerMessage
	for attempt := 1; attempt <= 10; attempt++ {
		optionalCBEntry, err = cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching CircuitBreaker entry for subscriptionId: %s", subscriptionId)
			resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: err}
			return
		}

		if optionalCBEntry != nil {
			resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: nil}
			return
		}

		if attempt <= 10 {
			time.Sleep(config.Current.Republishing.WaitingStatesIntervalTime)
		} else {
			log.Debug().Msgf("No CircuitBreaker and no republishing entry found for subscriptionId: %s", subscriptionId)

			err = cache.RepublishingCache.Set(context.Background(), subscriptionId, republish.RepublishingCacheEntry{
				SubscriptionId: subscriptionId,
			})
			if err != nil {
				resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: err}
				return
			}

			resultChan <- ProcessResult{SubscriptionId: subscriptionId, Error: nil}
			return
		}
	}
}
