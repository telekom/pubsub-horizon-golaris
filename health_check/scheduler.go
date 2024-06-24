package health_check

import (
	"eni.telekom.de/horizon2go/pkg/enum"
	"github.com/go-co-op/gocron"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"golaris/config"
	kube "golaris/kubernetes"
	"golaris/utils"
	"time"
)

var scheduler *gocron.Scheduler

func InitializeScheduler(deps utils.Dependencies) {
	scheduler = gocron.NewScheduler(time.UTC)

	if _, err := scheduler.Every(config.Current.Polling.OpenCbMessageInterval).Do(func() {
		checkCircuitBreakersByStatus(deps, enum.CircuitBreakerStatusOpen)
	}); err != nil {
		log.Error().Msgf("Error while scheduling for OPEN CircuitBreakers: %v", err)
	}

	// Watch for pod restart and select REPUBLISHING/ CHECKING cbMessages when all pods are running again
	clientSet := kube.InitializeKubernetesClient()
	if kube.PodsHealthAfterRestart(clientSet) {
		checkCircuitBreakersByStatus(deps, enum.CircuitBreakerStatusRepublishing)
		checkCircuitBreakersByStatus(deps, enum.CircuitBreakerStatusChecking)
	}

	scheduler.StartAsync()
}

func checkCircuitBreakersByStatus(deps utils.Dependencies, status enum.CircuitBreakerStatus) {
	statusQuery := predicate.Equal("status", string(status))
	cbEntries, err := deps.CbCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, statusQuery)
	if err != nil {
		log.Debug().Msgf("Error while getting CircuitBreaker messages: %v", err)
		return
	}

	for _, entry := range cbEntries {
		log.Info().Msgf("Checking CircuitBreaker with id %s", entry.SubscriptionId)

		if entry.Status != enum.CircuitBreakerStatusChecking {
			entry.Status = enum.CircuitBreakerStatusChecking
			entry.LastModified = time.Now().UTC()

			if err = deps.CbCache.Put(config.Current.Hazelcast.Caches.CircuitBreakerCache, entry.SubscriptionId, entry); err != nil {
				log.Error().Err(err).Msgf("Error putting CircuitBreakerMessage to cache for subscription %s", entry.SubscriptionId)
				return
			}
			log.Debug().Msgf("Updated CircuitBreaker with id %s to status checking", entry.SubscriptionId)
		}

		go checkSubscriptionForCbMessage(deps, entry)
	}
}
