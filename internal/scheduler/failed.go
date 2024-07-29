package scheduler

import (
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"pubsub-horizon-golaris/internal/cache"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"time"
)

// ToDo: However, we have to build the code that only one pod is allowed to do this at a time
func checkFailedEvents() {
	batchSize := config.Current.Republishing.BatchSize
	page := int64(0)

	opts := options.Find().SetLimit(batchSize).SetSkip(page * batchSize).SetSort(bson.D{{Key: "timestamp", Value: 1}})

	var dbMessages []message.StatusMessage
	var err error
	dbMessages, err = mongo.CurrentConnection.FindFailedMessagesWithCallbackUrlNotFoundException(time.Now(), opts)
	if err != nil {
		log.Error().Err(err).Msgf("Error while fetching messages for subscription from db")
		return
	}

	if len(dbMessages) == 0 {
		return
	}

	for _, dbMessage := range dbMessages {
		subscriptionId := dbMessage.SubscriptionId

		subscription, err := cache.SubscriptionCache.Get(config.Current.Hazelcast.Caches.SubscriptionCache, subscriptionId)
		if err != nil {
			log.Printf("Error while fetching republishing entry for subscriptionId %s: %v", subscriptionId, err)
			return
		}

		if subscription != nil {
			if subscription.Spec.Subscription.DeliveryType == "sse" || subscription.Spec.Subscription.DeliveryType == "server_sent_event" {
				var newDeliveryType = "SERVER_SENT_EVENT"

				if dbMessage.Coordinates == nil {
					log.Printf("Coordinates in message for subscriptionId %s are nil: %v", subscriptionId, dbMessage)
					return
				}

				kafkaMessage, err := kafka.CurrentHandler.PickMessage(dbMessage.Topic, dbMessage.Coordinates.Partition, dbMessage.Coordinates.Offset)
				if err != nil {
					log.Printf("Error while fetching message from kafka for subscriptionId %s: %v", subscriptionId, err)
					return
				}

				err = kafka.CurrentHandler.RepublishMessage(kafkaMessage, newDeliveryType, "")
				if err != nil {
					log.Printf("Error while republishing message for subscriptionId %s: %v", subscriptionId, err)
					return
				}
				log.Printf("Successfully republished message for subscriptionId %s", subscriptionId)

			}

			if len(dbMessages) < int(batchSize) {
				break
			}
			page++
		}
	}
}
