// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"

	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/metrics"
	"pubsub-horizon-golaris/internal/tracing"
)

var (
	app *fiber.App
)

func init() {
	app = fiber.New()

	app.Use(tracing.Middleware())
	app.Use(healthcheck.New())
	app.Use(pprof.New())

	app.Get("/metrics", metrics.NewPrometheusMiddleware())
	// setup routes
	v1 := app.Group("/api/v1")

	v1.Get("/circuit-breakers/:subscriptionId", getCircuitBreakerMessageById)

	v1.Get("/circuit-breakers", getAllCircuitBreakerMessages)
}

func getAllCircuitBreakerMessages(ctx *fiber.Ctx) error {
	// Create a predicate to select all entries
	//pred := predicate.True()
	pred := predicate.Equal("status", enum.CircuitBreakerStatusOpen)

	// Set the Content-Type header to application/json
	ctx.Set("Content-Type", "application/json")

	// Get all circuit breaker messages
	cbMessages, err := cache.CircuitBreakerCache.GetQuery(config.Current.Hazelcast.Caches.CircuitBreakerCache, pred)
	if err != nil {
		log.Error().Err(err).Msg("Error while getting all CircuitBreaker messages")
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Error retrieving circuit breaker messages"})
	}

	// Convert the circuit breaker messages to JSON
	cbMessagesJSON, err := json.Marshal(cbMessages)
	if err != nil {
		log.Error().Err(err).Msg("Error converting circuit breaker messages to JSON")
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Error converting circuit breaker messages to JSON"})
	}

	// Send the circuit breaker messages as the response
	return ctx.Status(fiber.StatusOK).Send(cbMessagesJSON)
}

func getCircuitBreakerMessageById(ctx *fiber.Ctx) error {
	// Get the subscriptionId from the request parameters
	subscriptionId := ctx.Params("subscriptionId")

	// Read from the circuit breaker cache
	cbMessage, err := cache.CircuitBreakerCache.Get(config.Current.Hazelcast.Caches.CircuitBreakerCache, subscriptionId)

	// Set the Content-Type header to application/json
	ctx.Set("Content-Type", "application/json")

	if err != nil {
		log.Error().Err(err).Msgf("Error while getting CircuitBreaker message for subscription %s", subscriptionId)
		return ctx.Status(fiber.StatusInternalServerError).SendString("Error retrieving circuit breaker message")
	}
	//add if cbMessage is nil, then return not found status code
	if cbMessage == nil {
		return ctx.Status(fiber.StatusNotFound).SendString("Circuit breaker message not found for subscription-id " + subscriptionId)
	}

	// Convert the circuit breaker message to JSON
	cbMessageJSON, err := json.Marshal(cbMessage)
	if err != nil {
		log.Error().Err(err).Msg("Error converting circuit breaker message to JSON")
		return ctx.Status(fiber.StatusInternalServerError).SendString("Error converting circuit breaker message to JSON")
	}

	// Send the circuit breaker message as the response
	return ctx.Status(fiber.StatusOK).Send(cbMessageJSON)
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)
	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
}
