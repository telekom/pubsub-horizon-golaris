// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/config"
	"strings"
)

// ConfluentHandler implements Handler using Confluent Kafka
type ConfluentHandler struct {
	producer *kafka.Producer
}

// NewConfluentHandler creates a new Handler using Confluent Kafka
func NewConfluentHandler() (*ConfluentHandler, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Current.Kafka.Brokers, ","),
	})

	if err != nil {
		log.Error().Err(err).Msg("Could not create Confluent Kafka producer")
		return nil, err
	}

	return &ConfluentHandler{
		producer: producer,
	}, nil
}

// RepublishMessage implements HandlerInterface.RepublishMessage()
func (h *ConfluentHandler) RepublishMessage(traceCtx *tracing.TraceContext, message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string, errorParams bool) error {
	// Reuse the same message processing logic but with Confluent producer
	updatedSaramaMessage, err := updateMessage(message, newDeliveryType, newCallbackUrl)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}

	if traceCtx != nil {
		traceCtx.StartSpan("produce message")
		defer traceCtx.EndCurrentSpan()
	}

	// Convert Sarama producer message to Confluent producer message
	confluentMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &updatedSaramaMessage.Topic,
			Partition: kafka.PartitionAny,
		},
		Key:     updatedSaramaMessage.Key.(sarama.ByteEncoder),
		Value:   updatedSaramaMessage.Value.(sarama.ByteEncoder),
		Headers: convertSaramaHeadersToConfluent(updatedSaramaMessage.Headers),
	}

	// Handle optional metadata message
	if errorParams {
		optionalMetadataMessage, err := updateMetaData(message)
		if err != nil {
			log.Error().Err(err).Msg("Could not update message metadata")
			return err
		}

		// Send metadata message
		metadataConfluentMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &optionalMetadataMessage.Topic,
				Partition: kafka.PartitionAny,
			},
			Key:     optionalMetadataMessage.Key.(sarama.ByteEncoder),
			Value:   optionalMetadataMessage.Value.(sarama.ByteEncoder),
			Headers: convertSaramaHeadersToConfluent(optionalMetadataMessage.Headers),
		}

		if err := h.producer.Produce(metadataConfluentMsg, nil); err != nil {
			log.Error().Err(err).Msgf("Could not send metadata message to Kafka")
			if traceCtx != nil {
				traceCtx.CurrentSpan().RecordError(err)
			}
			return err
		}
	}

	// Send the main message
	err = h.producer.Produce(confluentMsg, nil)
	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to Kafka", string(message.Key))
		if traceCtx != nil {
			traceCtx.CurrentSpan().RecordError(err)
		}
		return err
	}

	// Ensure messages are delivered
	h.producer.Flush(5000)

	log.Debug().Msgf("Message with id %s sent to Kafka: newDeliveryType %s newCallBackUrl %s",
		string(message.Key), newDeliveryType, newCallbackUrl)

	if traceCtx != nil {
		traceCtx.SetAttribute("partition", "any")  // Confluent determines partition
		traceCtx.SetAttribute("offset", "unknown") // Offset will be determined after delivery

		log.Debug().Fields(map[string]any{
			"uuid": string(message.Key),
		}).Msgf("Republished message")
	}

	return nil
}

// Convert Sarama headers to Confluent Kafka headers
func convertSaramaHeadersToConfluent(saramaHeaders []sarama.RecordHeader) []kafka.Header {
	confluentHeaders := make([]kafka.Header, 0, len(saramaHeaders))
	for _, header := range saramaHeaders {
		confluentHeaders = append(confluentHeaders, kafka.Header{
			Key:   string(header.Key),
			Value: header.Value,
		})
	}
	return confluentHeaders
}
