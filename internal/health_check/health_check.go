// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package health_check

import (
	"context"
	"encoding/gob"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/auth"
	"golaris/internal/cache"
	"golaris/internal/config"
	"net/http"
	"strings"
	"time"
)

func init() {
	gob.Register(HealthCheck{})
}

func BuildHealthCheckData(subscription *resource.SubscriptionResource, httpMethod string) HealthCheck {
	return HealthCheck{
		Environment: subscription.Spec.Environment,
		Method:      httpMethod,
		CallbackUrl: subscription.Spec.Subscription.Callback,
	}
}

func UpdateHealthCheck(ctx context.Context, healthCheckKey string, healthCheckData HealthCheck, statusCode int) {
	healthCheckData.LastedCheckedStatus = statusCode
	healthCheckData.LastChecked = time.Now()

	if err := cache.HealthChecks.Set(ctx, healthCheckKey, healthCheckData); err != nil {
		log.Error().Err(err).Msgf("Failed to update health check for key %s", healthCheckKey)
	}
}

func InCoolDown(healthCheckData HealthCheck) bool {
	lastCheckedTime := healthCheckData.LastChecked
	duration := time.Since(lastCheckedTime)
	if duration.Seconds() < config.Current.HealthCheck.CoolDownTime.Seconds() {
		return true
	}
	return false
}

func CheckConsumerHealth(healthCheckData HealthCheck, subscription *resource.SubscriptionResource) (*http.Response, error) {
	log.Debug().Msg("Checking consumer health")

	//Todo Take several virtual environments into account
	clientSecret := strings.Split(config.Current.Security.ClientSecret, "=")
	issuerUrl := strings.ReplaceAll(config.Current.Security.Url, "<realm>", clientSecret[0])

	token, err := auth.RetrieveToken(issuerUrl, config.Current.Security.ClientId, clientSecret[1])
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return &http.Response{}, err
	}

	resp, err := executeHealthRequestWithToken(healthCheckData.CallbackUrl, healthCheckData.Method, subscription, token)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to perform http-request for callback-url %s", healthCheckData.CallbackUrl)
		return &http.Response{}, err
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", healthCheckData.CallbackUrl, resp.StatusCode)
	return resp, nil
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

	return response, nil
}
