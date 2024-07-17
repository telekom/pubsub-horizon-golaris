// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"go.mongodb.org/mongo-driver/mongo"
	"pubsub-horizon-golaris/internal/config"
)

type Connection struct {
	Client *mongo.Client
	Config *config.Mongo
}
