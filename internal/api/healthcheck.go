// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/rs/zerolog/log"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/healthcheck"
	"pubsub-horizon-golaris/internal/utils"
)

type HealthCheckResponse struct {
	healthcheck.HealthCheckCacheEntry
	SubscriptionIds []string `json:"subscriptionIds"`
}

func getAllHealthChecks(ctx *fiber.Ctx) error {
	var healthStateCache = cache.HealthCheckCache.(*hazelcast.Map)

	values, err := healthStateCache.GetValues(context.Background())
	if err != nil {
		return err
	}

	var body = struct {
		Items []HealthCheckResponse `json:"items"`
	}{make([]HealthCheckResponse, 0)}

	for _, value := range values {
		healthCheck := value.(healthcheck.HealthCheckCacheEntry)
		body.Items = append(body.Items, makeResponse(&healthCheck))
	}

	return ctx.Status(fiber.StatusOK).JSON(body)
}

func makeResponse(healthCheck *healthcheck.HealthCheckCacheEntry) HealthCheckResponse {
	var res = HealthCheckResponse{HealthCheckCacheEntry: *healthCheck}
	populateHealthCheckResponse(&res)
	return res
}

func populateHealthCheckResponse(res *HealthCheckResponse) {
	query := predicate.And(predicate.Equal("spec.subscription.callback", res.CallbackUrl))
	subscriptions, err := cache.SubscriptionCache.GetQuery(config.Current.Hazelcast.Caches.SubscriptionCache, query)
	if err != nil {
		log.Warn().Fields(map[string]any{
			"callbackUrl": res.CallbackUrl,
		}).Err(err).Msg("Could not populate health-check with subscription-ids")
		return
	}

	for _, subscription := range subscriptions {
		var enforceGetRequest = utils.IfThenElse(res.Method == fiber.MethodGet, true, false)
		if enforceGetRequest == subscription.Spec.Subscription.EnforceGetHealthCheck {
			res.SubscriptionIds = append(res.SubscriptionIds, subscription.Spec.Subscription.SubscriptionId)
		}
	}
}
