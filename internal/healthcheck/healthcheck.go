// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"net/http"
	"pubsub-horizon-golaris/internal/auth"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"strings"
	"time"
)

// register the data type HealthCheckCacheEntry to gob for encoding and decoding of binary data
func init() {
	gob.Register(HealthCheckCacheEntry{})
}

// NewHealthCheckEntry creates a new basic HealthCheckCacheEntry  with the fields Environment, Method, and CallbackUrl.
func NewHealthCheckEntry(subscription *resource.SubscriptionResource, httpMethod string) HealthCheckCacheEntry {
	return HealthCheckCacheEntry{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
	}
}

// updateHealthCheckEntry updates a HealthCheckCacheEntry with the provided status code and the current time.
func updateHealthCheckEntry(ctx context.Context, healthCheckKey string, healthCheckData HealthCheckCacheEntry, statusCode int) {
	healthCheckData.LastCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := cache.HealthCheckCache.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update HealthCheckCacheEntry for key %s", healthCheckKey)
	}
}

// IsHealthCheckInCoolDown compares the HealthCheckCacheEntry's LastChecked time with the configured cool down time.
func IsHealthCheckInCoolDown(healthCheckData HealthCheckCacheEntry) bool {
	lastCheckedTime := healthCheckData.LastChecked
	if lastCheckedTime.IsZero() {
		return false
	}
	if time.Since(lastCheckedTime).Seconds() < config.Current.HealthCheck.CoolDownTime.Seconds() {
		return true
	}
	return false
}

// getCredentialsForEnvironment resolves the credentials for the given environment.
func getCredentialsForEnvironment(environment string) (string, string, string, error) {
	var issuerUrl = strings.ReplaceAll(config.Current.Security.Url, "<realm>", environment)

	var secrets = make(map[string]string)
	for _, secret := range config.Current.Security.ClientSecret {
		var splittedSecret = strings.SplitN(secret, "=", 2)
		if len(splittedSecret) < 2 {
			return "", "", "", fmt.Errorf("could not resolve secret '%s' for environment '%s'", secret, environment)
		}

		var clientEnvironment, clientSecret = splittedSecret[0], splittedSecret[1]
		secrets[clientEnvironment] = clientSecret
	}

	return issuerUrl, config.Current.Security.ClientId, secrets[environment], nil
}

// CheckConsumerHealth retrieves the consumer token and calls the
// executeHealthRequestWithToken function to perform the health check before calling the updateHealthCheckEntry.
func CheckConsumerHealth(hcData *PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
	log.Debug().Msg("Checking consumer health")

	var issuerUrl, clientId, clientSecret, err = getCredentialsForEnvironment(subscription.Spec.Environment)
	if err != nil {
		return err
	}

	token, err := auth.RetrieveToken(issuerUrl, clientId, clientSecret)
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return err
	}

	resp, err := executeHealthRequestWithToken(hcData.HealthCheckEntry.CallbackUrl, hcData.HealthCheckEntry.Method, subscription, token)
	if err != nil {
		log.Info().Err(err).Msgf("Failed to perform http-request for callback-url %s", hcData.HealthCheckEntry.CallbackUrl)
		updateHealthCheckEntry(hcData.Ctx, hcData.HealthCheckKey, hcData.HealthCheckEntry, 0)
		return err
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", hcData.HealthCheckEntry.CallbackUrl, resp.StatusCode)

	hcData.HealthCheckEntry.LastCheckedStatus = resp.StatusCode
	updateHealthCheckEntry(hcData.Ctx, hcData.HealthCheckKey, hcData.HealthCheckEntry, resp.StatusCode)

	return nil
}

func executeHealthRequestWithToken(callbackUrl string, httpMethod string, subscription *resource.SubscriptionResource, token string) (*http.Response, error) {
	log.Debug().Msgf("Performing health request for calllback-url %s with http-method %s", callbackUrl, httpMethod)

	request, err := http.NewRequest(httpMethod, callbackUrl, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create request for URL %s: %v", callbackUrl, err)
	}

	request.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	request.Header.Add("x-pubsub-publisher-id", subscription.Spec.Subscription.PublisherId)
	request.Header.Add("x-pubsub-subscriber-id", subscription.Spec.Subscription.SubscriberId)

	response, err := auth.Client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Failed to perform %s request to %s: %v", httpMethod, callbackUrl, err)
	}
	defer response.Body.Close()

	return response, nil
}

// PrepareHealthCheck tries to get an entry from the HealthCheckCache. If no entry exists it creates a new one. The entry then gets locked.
// It returns a PreparedHealthCheckData struct containing the context, health check key, health check entry, and a boolean indicating if the lock was acquired.
func PrepareHealthCheck(subscription *resource.SubscriptionResource) (*PreparedHealthCheckData, error) {
	httpMethod := GetHttpMethod(subscription)

	healthCheckKey := fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, httpMethod, subscription.Spec.Subscription.Callback)

	ctx := cache.HealthCheckCache.NewLockContext(context.Background())

	// Get the health check entry for the healthCacheKey
	healthCheckEntry, err := cache.HealthCheckCache.Get(ctx, healthCheckKey)
	if err != nil {
		log.Error().Err(err).Msgf("Error retrieving HealthCheckCacheEntry for key %s", healthCheckKey)
	}

	// If no entry exists, create a new one
	if healthCheckEntry == nil {
		healthCheckEntry = NewHealthCheckEntry(subscription, httpMethod)
		err := cache.HealthCheckCache.Set(ctx, healthCheckKey, healthCheckEntry)
		if err != nil {
			return &PreparedHealthCheckData{}, err
		}

		log.Debug().Msgf("Creating new HealthCheckCacheEntry for key %s", healthCheckKey)
	}

	// Attempt to acquire a lock for the health check key
	//isAcquired, _ := cache.HealthCheckCache.TryLockWithTimeout(ctx, healthCheckKey, 10*time.Millisecond)
	isAcquired, _ := cache.HealthCheckCache.TryLockWithLeaseAndTimeout(ctx, healthCheckKey, 60000*time.Millisecond, 10*time.Millisecond)

	castedHealthCheckEntry := healthCheckEntry.(HealthCheckCacheEntry)
	return &PreparedHealthCheckData{Ctx: ctx, HealthCheckKey: healthCheckKey, HealthCheckEntry: castedHealthCheckEntry, IsAcquired: isAcquired}, nil
}

// GetHttpMethod specifies the HTTP method based on the subscription configuration
func GetHttpMethod(subscription *resource.SubscriptionResource) string {
	httpMethod := http.MethodHead
	if subscription.Spec.Subscription.EnforceGetHealthCheck == true {
		httpMethod = http.MethodGet
	}
	return httpMethod
}
