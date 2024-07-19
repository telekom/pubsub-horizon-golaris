package api

import (
	"context"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/circuitbreaker"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/healthcheck"
	"pubsub-horizon-golaris/internal/republish"
	"pubsub-horizon-golaris/internal/utils"
)

type CircuitBreakerResponse struct {
	message.CircuitBreakerMessage
	HealthCheck  healthcheck.HealthCheck `json:"healthCheck"`
	SubscriberId string                  `json:"subscriberId"`
	PublisherId  string                  `json:"publisherId"`
}

func getAllCircuitBreakerMessages(ctx *fiber.Ctx) error {
	// Create a query to select all entries
	query := predicate.Equal("status", string(enum.CircuitBreakerStatusOpen))

	// Get all circuit breaker messages
	cbMessages, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, query)
	if err != nil {
		log.Error().Err(err).Msg("Error while getting all CircuitBreaker messages")
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Error retrieving circuit breaker messages"})
	}

	// Build body with items wrapper
	var body = struct {
		Items []CircuitBreakerResponse `json:"items"`
	}{make([]CircuitBreakerResponse, 0)}

	for _, cbMessage := range cbMessages {
		body.Items = append(body.Items, makeCircuitBreakerResponse(&cbMessage))
	}

	// Send the circuit breaker messages as the response
	return ctx.Status(fiber.StatusOK).JSON(body)
}

func getCircuitBreakerMessageById(ctx *fiber.Ctx) error {
	// Get the subscriptionId from the request parameters
	subscriptionId := ctx.Params("subscriptionId")

	// Read from the circuit breaker cache
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return ctx.Status(fiber.StatusInternalServerError).SendString("Error retrieving circuit breaker message")
	}

	//add if cbMessage is nil, then return not found status code
	if cbMessage == nil {
		return ctx.Status(fiber.StatusNotFound).SendString("Circuit breaker message not found for subscription-id " + subscriptionId)
	}

	// Send the circuit breaker message as the response
	return ctx.Status(fiber.StatusOK).JSON(makeCircuitBreakerResponse(cbMessage))
}

func putCloseCircuitBreakerById(ctx *fiber.Ctx) error {
	// Get the subscriptionId from the request parameters
	subscriptionId := ctx.Params("subscriptionId")

	// Read from the circuit breaker cache
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)
	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return ctx.Status(fiber.StatusInternalServerError).SendString("Error retrieving circuit breaker message")
	}

	//add if cbMessage is nil, then return not found status code
	if cbMessage == nil {
		return ctx.Status(fiber.StatusNotFound).SendString("Circuit breaker message not found for subscription-id " + subscriptionId)
	}

	// Set new republishing entry to pick the last waiting
	err = cache.RepublishingCache.Set(ctx.Context(), subscriptionId, republish.RepublishingCache{SubscriptionId: subscriptionId})
	if err != nil {
		log.Error().Err(err).Msgf("Error while setting Republishing Cache entry for subscription %s", subscriptionId)
		return ctx.Status(fiber.StatusInternalServerError).SendString("Error to set the republishing cache entry")
	}

	circuitbreaker.CloseCircuitBreaker(cbMessage)
	log.Info().Msgf("Successfully closed circuit breaker for subscription with status %s", cbMessage.Status)
	// Send the circuit breaker message as the response
	return ctx.Status(fiber.StatusOK).JSON(makeCircuitBreakerResponse(cbMessage))
}

func makeCircuitBreakerResponse(cbMsg *message.CircuitBreakerMessage) CircuitBreakerResponse {
	var resp = CircuitBreakerResponse{CircuitBreakerMessage: *cbMsg}
	populateCircuitBreakerResponse(&resp)
	return resp
}

func populateCircuitBreakerResponse(res *CircuitBreakerResponse) {
	subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, res.SubscriptionId)
	if err != nil {
		log.Warn().Fields(map[string]any{
			"subscriptionId": res.SubscriptionId,
		}).Msg("could not populate circuit-breaker response with subscription data")
	} else {
		res.SubscriberId = subscription.Spec.Subscription.SubscriberId
		res.PublisherId = subscription.Spec.Subscription.PublisherId
	}

	var healthCheckCache = cache.HealthCheckCache.(*hazelcast.Map)
	var healthCheckMethod = utils.IfThenElse(subscription.Spec.Subscription.EnforceGetHealthCheck, fiber.MethodGet, fiber.MethodHead)
	var healthCheckKey = fmt.Sprintf("%s:%s:%s", subscription.Spec.Environment, healthCheckMethod, subscription.Spec.Subscription.Callback)

	healthCheck, err := healthCheckCache.Get(context.Background(), healthCheckKey)
	if err != nil {
		log.Warn().Fields(map[string]any{
			"subscriptionId": res.SubscriptionId,
		}).Err(err).Msg("could not populate circuit-breaker response with healthcheck data")
		return
	} else {
		res.HealthCheck = healthCheck.(healthcheck.HealthCheck)
	}
}
