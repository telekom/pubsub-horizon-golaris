package utils

import (
	"eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"github.com/hazelcast/hazelcast-go-client"
	"golaris/kafka"
	"golaris/mongo"
)

type Dependencies struct {
	SubCache     *cache.Cache[resource.SubscriptionResource]
	CbCache      *cache.Cache[message.CircuitBreakerMessage]
	HealthCache  *hazelcast.Map
	MongoConn    *mongo.Connection
	KafkaHandler *kafka.Handler
}
