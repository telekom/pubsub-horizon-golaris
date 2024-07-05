// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"context"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func (connection Connection) findMessagesByQuery(query bson.M, pageable options.FindOptions) ([]message.StatusMessage, error) {
	collection := connection.client.Database(connection.config.Database).Collection(connection.config.Collection)

	cursor, err := collection.Find(context.Background(), query, &pageable)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding documents: %v", err)
		return nil, err
	}

	var messages []message.StatusMessage
	if err = cursor.All(context.Background(), &messages); err != nil {
		log.Error().Err(err).Msgf("Error reading documents from cursor: %v", err)
		return nil, err
	}

	return messages, nil
}

func (connection Connection) FindWaitingMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error) {
	query := bson.M{
		"status":         "WAITING",
		"subscriptionId": subscriptionId,
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, *pageable)
}

func (connection Connection) FindDeliveringMessagesByDeliveryType(status string, timestamp time.Time, pageable options.FindOptions, deliveryType string) ([]message.StatusMessage, error) {
	query := bson.M{
		"status":       status,
		"deliveryType": deliveryType,
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, pageable)
}

// ToDo: Here we need to discuss which FAILED events we want to republish!
func (connection Connection) FindFailedMessagesWithXYZException(status string, timestamp time.Time, pageable options.FindOptions) ([]message.StatusMessage, error) {
	query := bson.M{
		"status":     status,
		"error.type": "",
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, pageable)
}
