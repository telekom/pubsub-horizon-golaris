package service

import (
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/resource"
	"github.com/go-co-op/gocron"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
	"golaris/internal/health_check"
	"golaris/internal/utils"
	"time"
)

var scheduler *gocron.Scheduler

func StartScheduler(deps utils.Dependencies) {
	scheduler = gocron.NewScheduler(time.UTC)

	if _, err := scheduler.Every(config.Current.Polling.OpenCbMessageInterval).Do(func() {
		CheckCircuitBreakersByStatus(deps, enum.CircuitBreakerStatusOpen)
	}); err != nil {
		log.Error().Msgf("Error while scheduling for OPEN CircuitBreakers: %v", err)
	}
	scheduler.StartAsync()

}

func CheckCircuitBreakersByStatus(deps utils.Dependencies, status enum.CircuitBreakerStatus) {
	statusQuery := predicate.Equal("status", string(status))
	cbEntries, err := deps.CbCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	for _, entry := range cbEntries {
		log.Info().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		subscription := GetSubscriptionForCbMessage(deps, entry.SubscriptionId)
		if subscription == nil {
			log.Info().Msgf("Subscripton with id: %s does not exist. Delete cbMessage", entry.SubscriptionId)
			return
		} else {
			log.Debug().Msgf("Subscription with id %s found: %v", entry.SubscriptionId, subscription)
		}

		// ToDo: Check whether the subscription has changed
		go health_check.PerformHealthCheck(deps, entry, subscription)
	}
}

func GetSubscriptionForCbMessage(deps utils.Dependencies, subscriptionId string) *resource.SubscriptionResource {
	subscription, err := deps.SubCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Could not read subscription with id %s", subscriptionId)
		return nil
	}

	return subscription
}
