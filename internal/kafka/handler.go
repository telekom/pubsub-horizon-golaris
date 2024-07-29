// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/burdiyan/kafkautil"
	"github.com/rs/zerolog/log"
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

	// Initialize the Kafka Consumer to read messages from Kafka
	consumer, err := sarama.NewConsumer(config.Current.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Error().Err(err).Msg("Could not create Kafka consumer")
		return nil, err
	}

	// Initialize the Kafka Producer to send the updated messages back to Kafka (resetMessage)
	kafkaConfig.Producer.Partitioner = kafkautil.NewJVMCompatiblePartitioner
	kafkaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(config.Current.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Error().Err(err).Msg("Could not create Kafka producer")
		return nil, err
	}

	return &Handler{
		Consumer: consumer,
		Producer: producer,
	}, nil
}

func (kafkaHandler Handler) PickMessage(topic string, partition *int32, offset *int64) (*sarama.ConsumerMessage, error) {
	log.Debug().Msgf("Picking message at partition %d with offset %d", *partition, *offset)

	consumer, err := kafkaHandler.Consumer.ConsumePartition(topic, *partition, *offset)
	if err != nil {
		log.Debug().Msgf("KafkaPick for partition %d and topic %s and offset %d failed: %v", *partition, topic, *offset, err)
		return nil, err
	}
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Error().Err(err).Msg("Could not close consumer")
		}
	}()

	message := <-consumer.Messages()
	return message, nil
}

func (kafkaHandler Handler) RepublishMessage(message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string) error {
	modifiedValue, err := updateMessage(message, newDeliveryType, newCallbackUrl)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}

	msg := &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(message.Key),
		Topic:   message.Topic,
		Headers: copyHeaders(message.Headers),
		Value:   sarama.ByteEncoder(modifiedValue),
	}

	partition, offset, err := kafkaHandler.Producer.SendMessage(msg)
	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to kafka", string(message.Key))
		return err
	}
	log.Debug().Msgf("Message with id %s sent to kafka: partition %v offset %v newDeliveryType %s newCallBackUrl %s", string(message.Key), partition, offset, newDeliveryType, newCallbackUrl)

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

func updateMessage(message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string) ([]byte, error) {
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

	// These fields can be set if the FAILED handler republishes an old callback event with a new deliveryType SSE.
	for _, key := range []string{"errorType", "errorMessage"} {
		if _, exists := messageValue[key]; exists {
			log.Debug().Msgf("Replacing %s in message with an empty string", key)
			messageValue[key] = ""
		}
	}

	modifiedValue, err := json.Marshal(messageValue)
	if err != nil {
		log.Error().Err(err).Msg("Could not marshal modified message value")
		return nil, err
	}

	return modifiedValue, nil
}
