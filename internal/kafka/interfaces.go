package kafka

import "github.com/IBM/sarama"

type HandlerInterface interface {
	PickMessage(topic string, partition *int32, offset *int64) (*sarama.ConsumerMessage, error)
	RepublishMessage(message *sarama.ConsumerMessage) error
}
