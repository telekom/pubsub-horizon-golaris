package republish

import (
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mongo"
	"time"
)

func RepublishPendingEvents(subscriptionId string) {
	log.Info().Msgf("Republishing pending events for subscription %s", subscriptionId)

	batchSize := int64(config.Current.RepublishingBatchSize)
	page := int64(0)

	// Start a loop to paginate through the events
	for {
		pageable := options.Find().
			SetLimit(batchSize).
			// Skip the number of events already processed
			SetSkip(page * batchSize).
			SetSort(bson.D{{Key: "timestamp", Value: 1}})

		//Get Waiting events from database pageable!
		dbMessages, err := mongo.CurrentConnection.FindWaitingMessages(time.Now(), pageable, subscriptionId)
		if err != nil {
			log.Error().Err(err).Msgf("Error while fetching messages for subscription %s from db", subscriptionId)
		}

		log.Info().Msgf("Found %d event states in MongoDb", len(dbMessages))
		log.Debug().Msgf("dbMessages: %v", dbMessages)

		if len(dbMessages) == 0 {
			break
		}

		// Iterate over each message to republish
		for _, dbMessage := range dbMessages {
			log.Debug().Msgf("Republishing message for subscription %s: %v", subscriptionId, dbMessage)

			if dbMessage.Coordinates == nil {
				log.Error().Msgf("Coordinates in message for subscription %s are nil: %v", subscriptionId, dbMessage)
				continue
			}

			kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
			if err != nil {
				log.Warn().Msgf("Error while fetching message from kafka for subscription %s", subscriptionId)
				continue
			}
			err = kafka.CurrentHandler.RepublishMessage(kafkaMessage)
			if err != nil {
				log.Warn().Msgf("Error while republishing message for subscription %s", subscriptionId)
			}
			log.Debug().Msgf("Successfully republished message for subscription %s", subscriptionId)
		}

		// If the number of fetched messages is less than the batch size, exit the loop
		if len(dbMessages) < int(batchSize) {
			break
		}

		// Increment the page number for the next iteration
		page++
	}
}
