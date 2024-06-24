package health_check

import (
	"context"
	"encoding/gob"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"golaris/config"
	"golaris/utils"
	"time"
)

type HealthCheck struct {
	Environment         string    `json:"environment"`
	Method              string    `json:"method"`
	CallbackUrl         string    `json:"callbackUrl"`
	CheckingFor         string    `json:"checkingFor"`
	LastChecked         time.Time `json:"lastChecked"`
	LastedCheckedStatus int       `json:"lastCheckedStatus"`
}

func init() {
	gob.Register(HealthCheck{})
}

func NewHealthCheckCache(hzConfig hazelcast.Config) (*hazelcast.Map, error) {
	hazelcastClient, err := hazelcast.StartNewClientWithConfig(context.Background(), hzConfig)
	if err != nil {
		return nil, err
	}

	HealthCheckMap, err := hazelcastClient.GetMap(context.Background(), config.Current.Hazelcast.Caches.HealthCheckCache)
	if err != nil {
		return nil, err
	}

	return HealthCheckMap, nil
}

func performHealthCheck(deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, subscription *resource.SubscriptionResource) {
	// Specify HTTP method based on the subscription configuration
	httpMethod := "HEAD"
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = "GET"
	}

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)
	healthCheckData := buildHealthCheckData(subscription, httpMethod)

	// Attempt to acquire a lock for the health check key
	ctx := deps.HealthCache.NewLockContext(context.Background())
	if acquired, _ := deps.HealthCache.TryLockWithTimeout(ctx, healthCheckKey, 10*time.Millisecond); !acquired {
		log.Debug().Msgf("Could not acquire lock for key %s, skipping health check", healthCheckKey)
		return
	}

	// Ensure that the lock is released when the function is ended
	defer func() {
		if err := deps.HealthCache.Unlock(ctx, healthCheckKey); err != nil {
			log.Error().Err(err).Msgf("Error unlocking key %s", healthCheckKey)
		}
		log.Debug().Msgf("Successfully unlocked key %s", healthCheckKey)
	}()

	//Check if there is already a HealthCheck entry for the HealthCheckKey
	if shouldSkipHealthCheck(ctx, deps, healthCheckKey) {
		return
	}

	checkConsumerHealth(ctx, deps, cbMessage, healthCheckData, healthCheckKey)

	log.Info().Msgf("Successfully updated health check for key %s", healthCheckKey)
	return
}

func buildHealthCheckData(subscription *resource.SubscriptionResource, httpMethod string) HealthCheck {
	return HealthCheck{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
		CheckingFor: subscription.Spec.Subscription.SubscriptionId,
	}
}

func updateHealthCheck(ctx context.Context, deps utils.Dependencies, healthCheckKey string, healthCheckData HealthCheck, statusCode int) {
	healthCheckData.LastedCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := deps.HealthCache.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update health check for key %s", healthCheckKey)
	}
}

func shouldSkipHealthCheck(ctx context.Context, deps utils.Dependencies, healthCheckKey string) bool {
	existingHealthCheckData, err := deps.HealthCache.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving health check for key %s", healthCheckKey)
		return true
	}

	if existingHealthCheckData != nil {
		lastCheckedTime := existingHealthCheckData.(HealthCheck).LastChecked
		duration := time.Since(lastCheckedTime)
		if duration.Seconds() < config.Current.RequestCooldownTime.Seconds() {
			log.Debug().Msgf("Skipping health check for key %s due to cooldown", healthCheckKey)
			return true
		}
	}
	return false
}
