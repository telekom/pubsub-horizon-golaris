// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"eni.telekom.de/horizon2go/pkg/enum"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golaris/internal/config"
)

var CurrentHandler *Handler

func Initialize() {
	var err error

	CurrentHandler, err = newKafkaHandler()
	if err != nil {
		log.Panic().Err(err).Msg("error while initializing Kafka picker")
	}
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
	kafkaConfig.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(config.Current.Kafka.Brokers, kafkaConfig)
	if err != nil {
		log.Error().Err(err).Msg("Could not create Kafka producer")
		return nil, err
	}

	return &Handler{
		consumer: consumer,
		producer: producer,
	}, nil
}

func (kafkaHandler Handler) PickMessage(topic string, partition *int32, offset *int64) (*sarama.ConsumerMessage, error) {
	log.Debug().Msgf("Picking message at partition %d with offset %d", *partition, *offset)

	consumer, err := kafkaHandler.consumer.ConsumePartition(topic, *partition, *offset)
	if err != nil {
		log.Debug().Msgf("While kafkaPick, consumePartiton for topic %s failed.", topic)
		return nil, err
	}

	message := <-consumer.Messages()

	return message, nil
}

func (kafkaHandler Handler) RepublishMessage(message *sarama.ConsumerMessage) error {
	newUuid := uuid.New()
	modifiedValue, err := updateMessage(message, newUuid)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}

	msg := &sarama.ProducerMessage{
		Key:   sarama.StringEncoder(newUuid.String()),
		Topic: message.Topic,
		Value: sarama.ByteEncoder(modifiedValue),
	}

	_, _, err = kafkaHandler.producer.SendMessage(msg)

	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to kafka", msg.Key)
		return err
	}
	log.Debug().Msgf("Message with id %v sent to kafka", msg.Key)

	return nil
}

func updateMessage(message *sarama.ConsumerMessage, uuid uuid.UUID) ([]byte, error) {
	var messageValue map[string]any
	if err := json.Unmarshal(message.Value, &messageValue); err != nil {
		log.Error().Err(err).Msg("Could not unmarshal message value")
		return nil, err
	}

	// ToDo: If there are changes, we have to adjust the data here so that the current data is written to the kafka
	// --> DeliveryType
	// --> CallbackUrl
	// --> circuitBreakerOptOut?
	// --> HttpMethod?

	messageValue["uuid"] = uuid.String()
	messageValue["status"] = enum.StatusProcessed

	modifiedValue, err := json.Marshal(messageValue)
	if err != nil {
		log.Error().Err(err).Msg("Could not marshal modified message value")
		return nil, err
	}

	return modifiedValue, nil
}
