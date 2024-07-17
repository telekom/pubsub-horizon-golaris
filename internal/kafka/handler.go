// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/burdiyan/kafkautil"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/enum"
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

	modifiedValue, newHeaders, err := updateMessage(message, newDeliveryType, newCallbackUrl)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}

	msg := &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(message.Key),
		Topic:   message.Topic,
		Headers: newHeaders,
		Value:   sarama.ByteEncoder(modifiedValue),
	}

	_, _, err = kafkaHandler.Producer.SendMessage(msg)

	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to kafka", string(message.Key))
		return err
	}
	log.Debug().Msgf("Message with id %v sent to kafka", string(message.Key))

	return nil
}

func updateMessage(message *sarama.ConsumerMessage, newDeliveryType string, newCallbackUrl string) ([]byte, []sarama.RecordHeader, error) {
	var messageValue map[string]any
	if err := json.Unmarshal(message.Value, &messageValue); err != nil {
		log.Error().Err(err).Msg("Could not unmarshal message value")
		return nil, nil, err
	}

	// Map newDeliveryType to the appropriate value
	switch newDeliveryType {
	case "callback":
		newDeliveryType = "CALLBACK"
	case "server_sent_event", "sse":
		newDeliveryType = "SERVER_SENT_EVENT"
	}

	var metadataValue = map[string]any{
		"uuid": messageValue["uuid"],
		"event": map[string]any{
			"id": messageValue["event"].(map[string]interface{})["id"],
		},
		"status":           enum.StatusProcessed,
		"additionalFields": map[string]interface{}{},
	}

	if newCallbackUrl != "" {
		metadataValue["additionalFields"].(map[string]interface{})["callback-url"] = newCallbackUrl
	}

	if newDeliveryType == "SERVER_SENT_EVENT" {
		delete(metadataValue["additionalFields"].(map[string]interface{}), "callback-url")
	}

	newMessageType := "METADATA"
	newHeaders := []sarama.RecordHeader{
		{Key: []byte("type"), Value: []byte(newMessageType)},
	}

	modifiedValue, err := json.Marshal(metadataValue)
	if err != nil {
		log.Error().Err(err).Msg("Could not marshal modified message value")
		return nil, nil, err
	}

	return modifiedValue, newHeaders, nil
}
