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
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/metrics"
)

var (
	app *fiber.App
)

func init() {
	app = fiber.New()

	app.Use(healthcheck.New())
	app.Use(pprof.New())

	// setup routes
	v1 := app.Group("/api/v1")
	v1.Get("/health-checks", getAllHealthChecks)
	v1.Get("/circuit-breakers/:subscriptionId", getCircuitBreakerMessageById)
	v1.Get("/circuit-breakers", getAllCircuitBreakerMessages)
	v1.Put("/circuit-breakers/close/:subscriptionId", putCloseCircuitBreakerById)
	v1.Get("/republishing-entries", getRepublishingEntries)
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)

	if config.Current.Metrics.Enabled {
		log.Debug().Msg("Metrics enabled")
		app.Get("/metrics", metrics.NewPrometheusMiddleware())
		metrics.PopulateFromCache()
		metrics.ListenForChanges()
	}

	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}
}
