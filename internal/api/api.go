// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/gofiber/fiber/v2/middleware/pprof"
	"github.com/rs/zerolog/log"
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
	v1.Get("/health-checks", getAllHealthChecks)
	v1.Get("/circuit-breakers/:subscriptionId", getCircuitBreakerMessageById)
	v1.Get("/circuit-breakers", getAllCircuitBreakerMessages)
	v1.Put("/circuit-breakers/close/:subscriptionId", putCloseCircuitBreakerById)
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)
	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
}
