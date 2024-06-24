package health_check

import (
	"context"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"fmt"
	"github.com/rs/zerolog/log"
	"golaris/internal/auth"
	"golaris/internal/circuit_breaker"
	"golaris/internal/config"
	"golaris/internal/republish"
	"golaris/internal/utils"
	"net/http"
	"strings"
)

func checkConsumerHealth(ctx context.Context, deps utils.Dependencies, cbMessage message.CircuitBreakerMessage, healthCheckData HealthCheck, healthCheckKey string, subscription *resource.SubscriptionResource) {
	log.Debug().Msg("Checking consumer health")

	clientSecret := strings.Split(config.Current.Security.ClientSecret, "=")
	issuerUrl := strings.ReplaceAll(config.Current.Security.Url, "<realm>", clientSecret[0])

	token, err := auth.RetrieveToken(issuerUrl, config.Current.Security.ClientId, clientSecret[1])
	if err != nil {
		log.Error().Err(err).Msg("Failed to retrieve OAuth2 token")
		return
	}

	resp, err := executeHealthRequestWithToken(healthCheckData.CallbackUrl, healthCheckData.Method, subscription, token)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to perform http-request for callback-url %s", healthCheckData.CallbackUrl)
		return
	}
	log.Debug().Msgf("Received response for callback-url %s with http-status: %v", healthCheckData.CallbackUrl, resp.StatusCode)

	if utils.Contains(config.Current.SuccessfulResponseCodes, resp.StatusCode) {
		updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)
		go republish.RepublishPendingEvents(deps, subscription.Spec.Subscription.SubscriptionId)

		// ToDo Check whether there are still  waiting messages in the db for the subscriptionId?
		circuit_breaker.CloseCircuitBreaker(deps, cbMessage.SubscriptionId)
	} else {
		updateHealthCheck(ctx, deps, healthCheckKey, healthCheckData, resp.StatusCode)
	}
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
