// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"go.mongodb.org/mongo-driver/mongo"
	"golaris/internal/config"
)

type Connection struct {
	client *mongo.Client
	config *config.Mongo
}
