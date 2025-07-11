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
		"group.id":                 "golaris-republishing",
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
	if err := p.consumer.Close(); err != nil {
		log.Error().Err(err).Msg("Could not close confluent picker gracefully")
	}
}

// Pick implements MessagePicker.Pick()
func (p *ConfluentPicker) Pick(status *message.StatusMessage) (*sarama.ConsumerMessage, error) {
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

	elapsedTime := time.Since(startTime)
	log.Debug().Msgf("Confluent Kafka Pick: Creating consumer duration: %v", elapsedTime)

	startTime = time.Now()
	msg, err := p.consumer.ReadMessage(5 * time.Second)
	elapsedTime = time.Since(startTime)
	log.Debug().Msgf("Confluent Kafka Pick: Reading message from kafka: %v", elapsedTime)
	if err != nil {
		return nil, err
	}

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

	log.Debug().Msgf("Confluent Kafka Pick: Read message with key %s from topic %s, partition %d, offset %d, body %v",
		string(saramaMsg.Key), saramaMsg.Topic, saramaMsg.Partition, saramaMsg.Offset, string(saramaMsg.Value))

	// print the body field of the msg.Value
	if saramaMsg.Value == nil && len(saramaMsg.Headers) > 0 {
		for _, header := range saramaMsg.Headers {
			if string(header.Key) == "body" {
				log.Warn().Msgf("Received message with nil value but body header present: %s", string(header.Value))
			}
		}
	}
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
