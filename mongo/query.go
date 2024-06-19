package mongo

import (
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type MessageParams struct {
	ctx       *fiber.Ctx
	status    string
	timestamp time.Time
	pageable  options.FindOptions
}

func (connection Connection) findMessagesByQuery(ctx *fiber.Ctx, query bson.M, pageable options.FindOptions) ([]message.StatusMessage, error) {
	collection := connection.client.Database(connection.config.Database).Collection(connection.config.Collection)

	cursor, err := collection.Find(ctx.Context(), query, &pageable)
	if err != nil {
		log.Error().Err(err).Msgf("Error finding documents: %v", err)
		return nil, err
	}

	var messages []message.StatusMessage
	if err = cursor.All(ctx.Context(), &messages); err != nil {
		log.Error().Err(err).Msgf("Error reading documents from cursor: %v", err)
		return nil, err
	}

	return messages, nil
}

func (connection Connection) findWaitingMessagesOrWithCallbackUrlNotFoundException(messageParams MessageParams, statusList []string, subscriptionIDs []string) ([]message.StatusMessage, error) {
	ctx := messageParams.ctx

	query := bson.M{
		"$or": []bson.M{
			{"status": "WAITING"},
			//ToDo: Only Waiting messages
			{"error.type": "de.telekom.horizon.dude.exception.CallbackUrlNotFoundException"},
		},
		"status": bson.M{
			"$in": statusList,
		},
		"subscriptionId": bson.M{
			"$in": subscriptionIDs,
		},
		"modified": bson.M{
			"$lte": messageParams.timestamp,
		},
	}

	return connection.findMessagesByQuery(ctx, query, messageParams.pageable)
}

func (connection Connection) findDeliveringMessagesByDeliveryType(messageParams MessageParams, deliveryType string) ([]message.StatusMessage, error) {
	ctx := messageParams.ctx

	query := bson.M{
		"deliveryType": deliveryType,
		"status":       messageParams.status,
		"modified": bson.M{
			"$lte": messageParams.timestamp,
		},
	}

	return connection.findMessagesByQuery(ctx, query, messageParams.pageable)
}

// ToDo: Here we need to discuss which FAILED events we want to republish!
func (connection Connection) findFailedMessagesWithXYZException(messageParams MessageParams) ([]message.StatusMessage, error) {
	ctx := messageParams.ctx

	query := bson.M{
		"status":     messageParams.status,
		"error.type": "",
		"modified": bson.M{
			"$lte": messageParams.timestamp,
		},
	}

	return connection.findMessagesByQuery(ctx, query, messageParams.pageable)
}
