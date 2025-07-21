// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/config"
	"strings"
	"time"
)

// ConfluentPicker implements MessagePicker using Confluent Kafka
type ConfluentPicker struct {
	consumer *kafka.Consumer
}

// NewConfluentPicker creates a new MessagePicker using Confluent Kafka
func NewConfluentPicker() (MessagePicker, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(config.Current.Kafka.Brokers, ","),
		"group.id":                 "golaris-republishing-picker",
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       false,
		"go.events.channel.enable": false,
	})

	if err != nil {
		return nil, err
	}

	return &ConfluentPicker{consumer}, nil
}

// Close implements MessagePicker.Close()
func (p *ConfluentPicker) Close() {

	// ToDo Performance tracking: Record start time
	startTime := time.Now()

	if err := p.consumer.Close(); err != nil {
		log.Error().Err(err).Msg("Could not close confluent picker gracefully")
	}

	// Todo Performance tracking: Record elapsed time
	log.Debug().Msgf("Performance tracking for Confluent Kafka Pick: Closed consumer after duration: %v", time.Since(startTime))
}

// Pick implements MessagePicker.Pick()
func (p *ConfluentPicker) Pick(status *message.StatusMessage) (*sarama.ConsumerMessage, error) {

	// ToDo Performance tracking: Record start time
	startTime := time.Now()
	partition, offset := *status.Coordinates.Partition, *status.Coordinates.Offset

	// Assign to topic/partition
	err := p.consumer.Assign([]kafka.TopicPartition{
		{
			Topic:     &status.Topic,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		},
	})

	if err != nil {
		return nil, err
	}

	msg, err := p.consumer.ReadMessage(5 * time.Second)
	if err != nil {
		return nil, err
	}

	// ToDo Performance tracking: Record elapsed time
	log.Debug().Msgf("Performance tracking for Confluent Kafka Pick: Read message after duration: %v", time.Since(startTime))

	// Convert Confluent message to Sarama message for backward compatibility
	saramaMsg := &sarama.ConsumerMessage{
		Topic:     *msg.TopicPartition.Topic,
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   convertConfluentHeadersToSarama(msg.Headers),
		Timestamp: msg.Timestamp,
	}

	// Performance tracking: Record  message details
	log.Debug().Msgf("Performance tracking for Confluent Kafka Pick: Read message with key %s from topic %s, partition %d, offset %d, body %v",
		string(saramaMsg.Key), saramaMsg.Topic, saramaMsg.Partition, saramaMsg.Offset, string(saramaMsg.Value))

	return saramaMsg, nil
}

// Convert Confluent Kafka headers to Sarama headers
func convertConfluentHeadersToSarama(headers []kafka.Header) []*sarama.RecordHeader {
	saramaHeaders := make([]*sarama.RecordHeader, 0, len(headers))
	for _, header := range headers {
		saramaHeaders = append(saramaHeaders, &sarama.RecordHeader{
			Key:   []byte(header.Key),
			Value: header.Value,
		})
	}
	return saramaHeaders
}
