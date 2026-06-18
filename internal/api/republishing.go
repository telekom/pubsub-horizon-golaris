// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/republish"

	"github.com/gofiber/fiber/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
)

func getRepublishingEntries(ctx *fiber.Ctx) error {
	body := struct {
		Items []republish.RepublishingCacheEntry `json:"items"`
	}{make([]republish.RepublishingCacheEntry, 0)}

	republishingCache := cache.RepublishingCache.(*hazelcast.Map)
	values, err := republishingCache.GetValues(context.Background())
	if err != nil {
		log.Error().Err(err).Msg("Could not retrieve republishing cache entries")
	}

	for _, value := range values {
		body.Items = append(body.Items, value.(republish.RepublishingCacheEntry))
	}

	return ctx.Status(fiber.StatusOK).JSON(body)
}
