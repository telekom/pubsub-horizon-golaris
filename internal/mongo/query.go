// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package mongo

import (
	"context"
	"fmt"
	"pubsub-horizon-golaris/internal/config"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	fieldStatus         = "status"
	fieldDeliveryType   = "deliveryType"
	fieldModified       = "modified"
	fieldSubscriptionId = "subscriptionId"
	opLte               = "$lte"
)

func (connection Connection) findMessagesByQuery(query bson.M, lastTimestamp any) ([]message.StatusMessage, any, error) {
	batchSize := config.Current.Republishing.BatchSize
	ctx := context.Background()

	opts := options.Find().
		SetLimit(batchSize).
		SetSort(bson.D{{Key: "timestamp", Value: 1}})

	if lastTimestamp != nil {
		query["timestamp"] = bson.M{"$gt": lastTimestamp}
		log.Debug().Msgf("Querying for messages with timestamp > %v", lastTimestamp)
	}

	collection := connection.Client.Database(connection.Config.Database).Collection(connection.Config.Collection)

	cursor, err := collection.Find(ctx, query, opts)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding documents: %v", err)
		return nil, nil, err
	}

	var messages []message.StatusMessage
	if err := cursor.All(ctx, &messages); err != nil {
		log.Error().
			Err(err).
			Str("query", fmt.Sprintf("%+v", query)).
			Msg("Error decoding messages from cursor")
	}

	var lastTimestampOfBatch any
	if len(messages) > 0 {
		lastTimestampOfBatch = messages[len(messages)-1].Timestamp
	}

	return messages, lastTimestampOfBatch, nil
}

func (connection Connection) distinctFieldByQuery(query bson.M, fieldName string) ([]interface{}, error) {
	opts := options.Distinct()

	collection := connection.Client.Database(connection.Config.Database).Collection(connection.Config.Collection)

	fields, err := collection.Distinct(context.Background(), fieldName, query, opts)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding distinct field %s in db", fieldName)
		return nil, err
	}

	return fields, nil
}

func (connection Connection) FindDistinctSubscriptionsForWaitingEvents(beginTimestamp, endTimestamp time.Time) ([]string, error) {
	query := bson.M{
		fieldStatus:       "WAITING",
		fieldDeliveryType: "CALLBACK",
		fieldModified: bson.M{
			"$gte": beginTimestamp,
			opLte:  endTimestamp,
		},
	}

	subscriptions, err := connection.distinctFieldByQuery(query, fieldSubscriptionId)
	if err != nil {
		return nil, err
	}

	castedSubscriptions := make([]string, len(subscriptions))
	for i, subscription := range subscriptions {
		castedSubscriptions[i] = subscription.(string)
	}

	return castedSubscriptions, err
}

func (connection Connection) FindWaitingMessages(
	timestamp time.Time,
	lastTimestamp any,
	subscriptionId string,
) ([]message.StatusMessage, any, error) {
	query := bson.M{
		fieldStatus:         "WAITING",
		fieldSubscriptionId: subscriptionId,
		fieldModified: bson.M{
			opLte: timestamp,
		},
	}

	return connection.findMessagesByQuery(query, lastTimestamp)
}

func (connection Connection) FindProcessedMessagesByDeliveryTypeSSE(
	timestamp time.Time,
	lastTimestamp any,
	subscriptionId string,
) ([]message.StatusMessage, any, error) {
	query := bson.M{
		fieldStatus:         "PROCESSED",
		fieldDeliveryType:   "SERVER_SENT_EVENT",
		fieldSubscriptionId: subscriptionId,
		fieldModified: bson.M{
			opLte: timestamp,
		},
	}

	return connection.findMessagesByQuery(query, lastTimestamp)
}

func (connection Connection) FindDeliveringMessagesByDeliveryType(
	timestamp time.Time,
	lastTimestamp any,
) ([]message.StatusMessage, any, error) {
	query := bson.M{
		fieldStatus: "DELIVERING",
		fieldModified: bson.M{
			opLte: timestamp,
		},
	}

	return connection.findMessagesByQuery(query, lastTimestamp)
}

func (connection Connection) FindFailedMessagesWithCallbackUrlNotFoundException(
	timestamp time.Time,
	lastTimestamp any,
) ([]message.StatusMessage, any, error) {
	query := bson.M{
		fieldStatus: "FAILED",
		"errorType": "de.telekom.horizon.comet.exception.CallbackUrlNotFoundException",
		fieldModified: bson.M{
			opLte: timestamp,
		},
	}

	return connection.findMessagesByQuery(query, lastTimestamp)
}
