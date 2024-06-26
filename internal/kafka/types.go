package kafka

import "github.com/IBM/sarama"

type Handler struct {
	consumer sarama.Consumer
	producer sarama.SyncProducer
}
