// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"pubsub-horizon-golaris/internal/config"

	"go.mongodb.org/mongo-driver/mongo"
)

type Connection struct {
	Client *mongo.Client
	Config *config.Mongo
}
