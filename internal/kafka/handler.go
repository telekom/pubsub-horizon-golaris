// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/burdiyan/kafkautil"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/tracing"
	"pubsub-horizon-golaris/internal/config"
)

var CurrentHandler HandlerInterface

func Initialize() {
	var err error

	conn, err := newKafkaHandler()
	if err != nil {
		log.Panic().Err(err).Msg("error while initializing Kafka picker")
	}
	CurrentHandler = conn
}

func newKafkaHandler() (*Handler, error) {
	kafkaConfig := sarama.NewConfig()

	// Initialize the Kafka Producer to send the updated messages back to Kafka (resetMessage)
	kafkaConfig.Producer.Partitioner = kafkautil.NewJVMCompatiblePartitioner
	kafkaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(config.Current.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Error().Err(err).Msg("Could not create Kafka producer")
		return nil, err
	}

	return &Handler{
		Producer: producer,
	}, nil
}

func (kafkaHandler Handler) RepublishMessage(traceCtx *tracing.TraceContext, message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string, errorParams bool) error {
	var kafkaMessages = make([]*sarama.ProducerMessage, 0)

	updatedMessage, err := updateMessage(message, newDeliveryType, newCallbackUrl)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}
	kafkaMessages = append(kafkaMessages, updatedMessage)

	if traceCtx != nil {
		traceCtx.StartSpan("produce message")
		defer traceCtx.EndCurrentSpan()
	}

	// TODO: Really required??
	if errorParams == true {
		optionalMetadataMessage, err := updateMetaData(message)
		if err != nil {
			log.Error().Err(err).Msg("Could not update message metadata")
			return err
		}
		kafkaMessages = append(kafkaMessages, optionalMetadataMessage)
	}

	err = kafkaHandler.Producer.SendMessages(kafkaMessages)
	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to kafka", string(message.Key))
		if traceCtx != nil {
			traceCtx.CurrentSpan().RecordError(err)
		}
		return err
	}

	log.Debug().Msgf("Message with id %s sent to kafka: newDeliveryType %s newCallBackUrl %s", string(message.Key), newDeliveryType, newCallbackUrl)

	if traceCtx != nil {
		traceCtx.SetAttribute("partition", fmt.Sprintf("%d", updatedMessage.Partition))
		traceCtx.SetAttribute("offset", fmt.Sprintf("%d", updatedMessage.Offset))

		log.Debug().Fields(map[string]any{
			"uuid":      string(message.Key),
			"partition": updatedMessage.Partition,
			"offset":    updatedMessage.Offset,
		}).Msgf("Republished message")
	}

	return nil
}

func copyHeaders(headers []*sarama.RecordHeader) []sarama.RecordHeader {
	var newHeaders []sarama.RecordHeader
	for _, header := range headers {
		if string(header.Key) != "clientId" {
			newHeaders = append(newHeaders, *header)
		}
	}
	newHeaders = append(newHeaders, sarama.RecordHeader{Key: []byte("clientId"), Value: []byte("golaris")})
	return newHeaders
}

func updateMessage(message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string) (*sarama.ProducerMessage, error) {
	var messageValue map[string]any
	if err := json.Unmarshal(message.Value, &messageValue); err != nil {
		log.Error().Err(err).Msg("Could not unmarshal message value")
		return nil, err
	}

	// Map newDeliveryType to the appropriate value
	switch newDeliveryType {
	case "CALLBACK", "SERVER_SENT_EVENT", "SSE":
		messageValue["deliveryType"] = newDeliveryType
	}

	// Update callbackUrl if there is a new one
	if newCallbackUrl != "" {
		additionalFields, ok := messageValue["additionalFields"].(map[string]any)
		if !ok {
			additionalFields = make(map[string]any)
			messageValue["additionalFields"] = additionalFields
		}
		additionalFields["callback-url"] = newCallbackUrl
	}

	// delete callbackUrl if newDeliveryType is sse or server_sent_event
	if newDeliveryType == "SERVER_SENT_EVENT" || newDeliveryType == "SSE" {
		additionalFields, ok := messageValue["additionalFields"].(map[string]any)
		if ok {
			if _, exists := additionalFields["callback-url"]; exists {
				log.Debug().Msgf("Replacing callback-url in message with an empty string")
				additionalFields["callback-url"] = ""
			}
		}
	}
	messageValue["status"] = enum.StatusProcessed

	modifiedValue, err := json.Marshal(messageValue)
	if err != nil {
		log.Error().Err(err).Msg("Could not marshal modified message value")
		return nil, err
	}

	msg := &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(message.Key),
		Topic:   message.Topic,
		Headers: copyHeaders(message.Headers),
		Value:   sarama.ByteEncoder(modifiedValue),
	}

	return msg, nil
}

func updateMetaData(message *sarama.ConsumerMessage) (*sarama.ProducerMessage, error) {
	var messageValue map[string]any
	if err := json.Unmarshal(message.Value, &messageValue); err != nil {
		log.Error().Err(err).Msg("Could not unmarshal message value")
		return nil, err
	}

	var metadataValue = map[string]any{
		"uuid": messageValue["uuid"],
		"event": map[string]any{
			"id": messageValue["event"].(map[string]any)["id"],
		},
		"errorMessage": "",
		"errorType":    "",
	}

	newMessageType := "METADATA"
	newHeaders := []sarama.RecordHeader{
		{Key: []byte("type"), Value: []byte(newMessageType)},
	}

	valueBytes, err := json.Marshal(metadataValue)
	if err != nil {
		log.Error().Err(err).Msg("Could not marshal metadata value")
		return nil, err
	}

	metadataMessage := &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(message.Key),
		Topic:   message.Topic,
		Headers: newHeaders,
		Value:   sarama.ByteEncoder(valueBytes),
	}

	return metadataMessage, nil
}
