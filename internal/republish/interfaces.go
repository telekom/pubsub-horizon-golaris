package republish

import (
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type MongoHandler interface {
	FindWaitingMessages(now time.Time, opts *options.FindOptions, subscriptionId string) ([]DBMessage, error)
}

type KafkaHandler interface {
	PickMessage(topic string, partition int32, offset int64) (KafkaMessage, error)
	RepublishMessage(message KafkaMessage) error
}
