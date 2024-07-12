package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/message"
	"golaris/internal/cache"
	"golaris/internal/config"
)

func getAllCircuitBreakerMessages(ctx *fiber.Ctx) error {
	// Create a predicate to select all entries
	//pred := predicate.True()
	pred := predicate.Equal("status", string(enum.CircuitBreakerStatusOpen))

	// Set the Content-Type header to application/json
	ctx.Set("Content-Type", "application/json")

	// Get all circuit breaker messages
	cbMessages, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, pred)
	if err != nil {
		log.Error().Err(err).Msg("Error while getting all CircuitBreaker messages")
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Error retrieving circuit breaker messages"})
	}

	var body = struct {
		Items []message.CircuitBreakerMessage `json:"items"`
	}{cbMessages}

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

	// Set the Content-Type header to application/json
	ctx.Set("Content-Type", "application/json")

	//add if cbMessage is nil, then return not found status code
	if cbMessage == nil {
		return ctx.Status(fiber.StatusNotFound).SendString("Circuit breaker message not found for subscription-id " + subscriptionId)
	}

	// Send the circuit breaker message as the response
	return ctx.Status(fiber.StatusOK).JSON(cbMessage)
}
