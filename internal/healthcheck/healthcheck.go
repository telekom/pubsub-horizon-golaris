// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"context"
	"encoding/gob"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/internal/auth"
	"golaris/internal/cache"
	"golaris/internal/config"
	"net/http"
	"strings"
	"time"
)

// register the data type HealthCheck to gob for encoding and decoding of binary data
func init() {
	gob.Register(HealthCheck{})
}

// NewHealthCheckEntry creates a new basic HealthCheck entry with the fields Environment, Method, and CallbackUrl.
func NewHealthCheckEntry(subscription *resource.SubscriptionResource, httpMethod string) HealthCheck {
	return HealthCheck{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
	}
}

// updateHealthCheckEntry updates a HealthCheck entry with the provided status code and the current time.
func updateHealthCheckEntry(ctx context.Context, healthCheckKey string, healthCheckData HealthCheck, statusCode int) {
	healthCheckData.LastCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := cache.HealthCheckCache.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update health check for key %s", healthCheckKey)
	}
}

// IsHealthCheckInCoolDown compares the HealthCheck entry's LastChecked time with the configured cool down time.
func IsHealthCheckInCoolDown(healthCheckData HealthCheck) bool {
	lastCheckedTime := healthCheckData.LastChecked
	duration := time.Since(lastCheckedTime)
	if duration.Seconds() < config.Current.HealthCheck.CoolDownTime.Seconds() {
		return true
	}
	return false
}

// CheckConsumerHealth retrieves the consumer token and calls the
// executeHealthRequestWithToken function to perform the health check before calling the updateHealthCheckEntry.
func CheckConsumerHealth(hcData *PreparedHealthCheckData, subscription *resource.SubscriptionResource) error {
	log.Debug().Msg("Checking consumer health")

	//Todo Take several virtual environments into account
	clientSecret := strings.Split(config.Current.Security.ClientSecret, "=")
	issuerUrl := strings.ReplaceAll(config.Current.Security.Url, "<realm>", clientSecret[0])

	// Todo caching for token?
	token, err := auth.RetrieveToken(issuerUrl, config.Current.Security.ClientId, clientSecret[1])
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return err
	}

	resp, err := executeHealthRequestWithToken(hcData.HealthCheckEntry.CallbackUrl, hcData.HealthCheckEntry.Method, subscription, token)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to perform http-request for callback-url %s", hcData.HealthCheckEntry.CallbackUrl)
		return err
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", hcData.HealthCheckEntry.CallbackUrl, resp.StatusCode)

	hcData.HealthCheckEntry.LastCheckedStatus = resp.StatusCode
	updateHealthCheckEntry(hcData.Ctx, hcData.HealthCheckKey, hcData.HealthCheckEntry, resp.StatusCode)

	return nil
}

func executeHealthRequestWithToken(callbackUrl string, httpMethod string, subscription *resource.SubscriptionResource, token string) (*http.Response, error) {
	log.Debug().Msgf("Performing health request for calllback-url %s with http-method %s", callbackUrl, httpMethod)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, httpMethod, callbackUrl, nil)
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
