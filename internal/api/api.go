// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/rs/zerolog/log"
	"golaris/internal/metrics"
	"golaris/internal/tracing"
)

var (
	app *fiber.App
)

func init() {
	app = fiber.New()

	app.Use(tracing.Middleware())
	app.Use(healthcheck.New())

	app.Get("/metrics", metrics.NewPrometheusMiddleware())

	v1 := app.Group("/api/v1")

	v1.Get("/circuit-breakers/:subscriptionId", getCircuitBreakerMessage)
}

func getCircuitBreakerMessage(c *fiber.Ctx) error {
	return c.SendString("Hello, World!")
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)
	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
}
