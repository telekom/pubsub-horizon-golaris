// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/config"
	"strings"
	"time"
)

// ConfluentHandler implements Handler using Confluent Kafka
type ConfluentHandler struct {
	producer *kafka.Producer
}

// NewConfluentHandler creates a new Handler using Confluent Kafka
func NewConfluentHandler() (*ConfluentHandler, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Current.Kafka.Brokers, ","),
		"client.id":         "golaris-republishing-handler",
		"acks":              "1",
		"linger.ms":         5,
	})

	if err != nil {
		log.Error().Err(err).Msg("Could not create Confluent Kafka producer")
		return nil, err
	}

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Error().Err(ev.TopicPartition.Error).
						Msgf("Delivery failed for message with key: %s", string(ev.Key))
				}
			}
		}
	}()

	return &ConfluentHandler{
		producer: producer,
	}, nil
}

// RepublishMessage implements HandlerInterface.RepublishMessage()
func (h *ConfluentHandler) RepublishMessage(traceCtx *tracing.TraceContext, message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string, errorParams bool) error {

	// Update data message
	updatedSaramaDataMessage, err := updateMessage(message, newDeliveryType, newCallbackUrl)
	if err != nil {
		log.Error().Err(err).Msgf("Could not update kafka message with id %s", string(message.Key))
		return err
	}

	// Convert Sarama producer message to Confluent producer message
	updatedDataMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &updatedSaramaDataMessage.Topic,
			Partition: updatedSaramaDataMessage.Partition,
		},
		Key:     updatedSaramaDataMessage.Key.(sarama.ByteEncoder),
		Value:   updatedSaramaDataMessage.Value.(sarama.ByteEncoder),
		Headers: convertSaramaHeadersToConfluent(updatedSaramaDataMessage.Headers),
	}

	// Send data message and wait for delivery report
	deliveredDataMsg, err := h.produceAndWaitForDelivery(updatedDataMsg, "data", traceCtx)
	if err != nil {
		return err
	}

	// Handle optional metadata message
	if errorParams {
		updatedSaramaMetaMessage, err := updateMetaData(message)
		if err != nil {
			log.Error().Err(err).Msgf("Could not update metadata for message with id %s", string(message.Key))
			return err
		}

		// Send metadata message and wait for delivery report
		metadataConfluentMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &updatedSaramaMetaMessage.Topic,
				Partition: updatedSaramaMetaMessage.Partition,
			},
			Key:     updatedSaramaMetaMessage.Key.(sarama.ByteEncoder),
			Value:   updatedSaramaMetaMessage.Value.(sarama.ByteEncoder),
			Headers: convertSaramaHeadersToConfluent(updatedSaramaMetaMessage.Headers),
		}

		_, err = h.produceAndWaitForDelivery(metadataConfluentMsg, "metadata", traceCtx)
		if err != nil {
			return err
		}
	}

	log.Debug().Msgf("Message with id %s sent to Kafka: newDeliveryType %s newCallBackUrl %s", string(message.Key), newDeliveryType, newCallbackUrl)

	if traceCtx != nil {
		traceCtx.SetAttribute("partition", fmt.Sprintf("%d", deliveredDataMsg.TopicPartition.Partition))
		traceCtx.SetAttribute("offset", fmt.Sprintf("%d", deliveredDataMsg.TopicPartition.Offset))

		log.Debug().Fields(map[string]any{
			"uuid":      string(message.Key),
			"partition": deliveredDataMsg.TopicPartition.Partition,
			"offset":    deliveredDataMsg.TopicPartition.Offset,
		}).Msgf("Republished message")
	}

	return nil
}

// produceAndWaitForDelivery sends a message to Kafka and waits for delivery confirmation with a timeout.
// Returns the delivered message and error if any.
func (h *ConfluentHandler) produceAndWaitForDelivery(msg *kafka.Message, messageType string, traceCtx *tracing.TraceContext) (*kafka.Message, error) {
	const DELIVERY_TIMEOUT = 10 * time.Second

	// Create a channel for this message's delivery report
	deliveryChan := make(chan kafka.Event, 1)

	// ToDo Performance tracking: Record start time
	startTime := time.Now()

	// Produce the message
	if err := h.producer.Produce(msg, deliveryChan); err != nil {
		log.Error().Err(err).Msgf("Failed producing kafka message of type %s with id %s", messageType, string(msg.Key))
		if traceCtx != nil {
			traceCtx.CurrentSpan().RecordError(err)
		}
		return nil, err
	}

	// Wait for delivery with timeout
	var e kafka.Event
	select {
	case e = <-deliveryChan:
		deliveredMsg := e.(*kafka.Message)
		if deliveredMsg.TopicPartition.Error != nil {
			log.Error().Err(deliveredMsg.TopicPartition.Error).Msgf("Failed producing kafka message of type %s with id %s", messageType, string(msg.Key))
			return nil, deliveredMsg.TopicPartition.Error
		}

		// Performance tracking: Record elapsed time
		log.Debug().Msgf("Performance tracking: Successfully produced kafka message of type %s with id %s after duration %v", messageType, string(msg.Key), time.Since(startTime))
		return deliveredMsg, nil

	case <-time.After(DELIVERY_TIMEOUT):
		errMsg := fmt.Errorf("timeout producing kafka message after %v", DELIVERY_TIMEOUT)
		log.Error().Err(errMsg).Msgf("Timeout producing kafka message of type %s with id %s", messageType, string(msg.Key))
		return nil, errMsg
	}
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
