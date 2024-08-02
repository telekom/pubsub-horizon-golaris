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
	"pubsub-horizon-golaris/internal/config"
	"time"
)

func (connection Connection) findMessagesByQuery(query bson.M, lastCursor any) ([]message.StatusMessage, any, error) {
	var batchSize = config.Current.Republishing.BatchSize

	opts := options.Find().
		SetBatchSize(int32(batchSize)).
		SetSort(bson.D{{Key: "timestamp", Value: 1}})

	collection := connection.Client.Database(connection.Config.Database).Collection(connection.Config.Collection)

	// If a lastCursor is provided, add it to the query to retrieve only documents after this timestamp.
	if lastCursor != nil {
		query["timestamp"] = bson.M{"$gt": lastCursor}
	}

	cursor, err := collection.Find(context.Background(), query, opts)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding documents: %v", err)
		return nil, nil, err
	}

	var messages []message.StatusMessage
	// Iterate through the results in the cursor.
	for cursor.Next(context.Background()) {
		var msg message.StatusMessage
		if err := cursor.Decode(&msg); err != nil {
			log.Error().Err(err).Msgf("Error decoding document: %v", err)
			return nil, nil, err
		}
		messages = append(messages, msg)
		if len(messages) >= int(batchSize) {
			break
		}
	}

	if err := cursor.Err(); err != nil {
		log.Error().Err(err).Msgf("Error iterating cursor: %v", err)
		return nil, nil, err
	}

	// Set the lastCursor to the timestamp of the last message in the list,
	// so that the next fetch only retrieves messages after this timestamp.
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
