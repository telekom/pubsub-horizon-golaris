package golaris

import (
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/rs/zerolog/log"
	"golaris/config"
	"golaris/utils"
)

func checkSubscriptionForCbMessage(deps utils.Dependencies, cbMessage message.CircuitBreakerMessage) {
	subscriptionId := cbMessage.SubscriptionId

	subscription, err := deps.SubCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Could not read subscription with id %s", subscriptionId)
		return
	}

	if subscription == nil {
		log.Info().Msgf("Subscripton is: %v with id %s.", subscription, subscriptionId)
		return
	} else {
		log.Debug().Msgf("Subscription with id %s found: %v", subscriptionId, subscription)
	}

	// ToDo: Check whether the subscription has changed

	go performHealthCheck(deps, cbMessage, subscription)
}
