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

func (connection Connection) findMessagesByQuery(query bson.M, lastCursor any) ([]message.StatusMessage, any, error) {
	opts := options.Find().SetBatchSize(int32(10)).SetSort(bson.D{{Key: "timestamp", Value: 1}})
	collection := connection.Client.Database(connection.Config.Database).Collection(connection.Config.Collection)

	cursor, err := collection.Find(context.Background(), query, opts)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding documents: %v", err)
		return nil, nil, err
	}

	var messages []message.StatusMessage
	if err = cursor.All(context.Background(), &messages); err != nil {
		log.Error().Err(err).Msgf("Error reading documents from cursor: %v", err)
		return nil, nil, err
	}

	if len(messages) > 0 {
		lastCursor = messages[len(messages)-1].Timestamp
	}

	return messages, lastCursor, nil
}

func (connection Connection) FindWaitingMessages(timestamp time.Time, cursor any, subscriptionId string) ([]message.StatusMessage, any, error) {
	query := bson.M{
		"status":         "WAITING",
		"subscriptionId": subscriptionId,
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, cursor)
}

func (connection Connection) FindProcessedMessagesByDeliveryTypeSSE(timestamp time.Time, cursor any, subscriptionId string) ([]message.StatusMessage, any, error) {
	query := bson.M{
		"status":         "PROCESSED",
		"deliveryType":   "SERVER_SENT_EVENT",
		"subscriptionId": subscriptionId,
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, cursor)
}

func (connection Connection) FindDeliveringMessagesByDeliveryType(timestamp time.Time, cursor any) ([]message.StatusMessage, any, error) {
	query := bson.M{
		"status": "DELIVERING",
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, cursor)
}

func (connection Connection) FindFailedMessagesWithCallbackUrlNotFoundException(timestamp time.Time, cursor any) ([]message.StatusMessage, any, error) {
	query := bson.M{
		"status":    "FAILED",
		"errorType": "de.telekom.horizon.comet.exception.CallbackUrlNotFoundException",
		"modified": bson.M{
			"$lte": timestamp,
		},
	}

	return connection.findMessagesByQuery(query, cursor)
}
