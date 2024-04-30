package kafka

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"golaris/config"
	"time"
)

type Handler struct {
	consumer sarama.Consumer
	producer sarama.SyncProducer
}

func NewKafkaHandler() (*Handler, error) {
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

func (kafkaHandler Handler) PickMessage(topic string, partition int32, offset int64) (*sarama.ConsumerMessage, error) {
	log.Debug().Msgf("Picking message at partition %d with offset %d", partition, offset)

	consumer, err := kafkaHandler.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		log.Debug().Msgf("While kafkaPick, consumePartiton for topic %s failed.", topic)
		return nil, err
	}

	message := <-consumer.Messages()

	return message, nil
}

func (kafkaHandler Handler) RepublishMessage(message *sarama.ConsumerMessage) error {
	modifiedValue, header, err := updateMessageMetadata(message)
	if err != nil {
		log.Error().Err(err).Msg("Could not update message metadata")
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic:     message.Topic,
		Headers:   header,
		Key:       sarama.StringEncoder(message.Key),
		Value:     sarama.ByteEncoder(modifiedValue),
		Timestamp: time.Now(),
	}

	_, _, err = kafkaHandler.producer.SendMessage(msg)

	if err != nil {
		log.Error().Err(err).Msgf("Could not send message with id %v to kafka", msg.Key)
		return err
	}
	log.Debug().Msgf("Message with id %v sent to kafka", msg.Key)

	return nil
}

func updateMessageMetadata(message *sarama.ConsumerMessage) ([]byte, []sarama.RecordHeader, error) {
	var messageValue map[string]any
	if err := json.Unmarshal(message.Value, &messageValue); err != nil {
		log.Error().Err(err).Msg("Could not unmarshal message value")
		return nil, nil, err
	}

	// ToDo: If there are changes, we have to adjust the data here so that the current data is written to the kafka
	// --> DeliveryType
	// --> CallbackUrl
	// --> circuitBreakerOptOut?
	// --> HttpMethod?

	var metadataValue = map[string]any{
		"uuid": messageValue["uuid"],
		"event": map[string]any{
			"id": messageValue["event"].(map[string]any)["id"],
		},
		"status": "PROCESSED",
	}

	// Add the messageType to METADATA?
	// ToDo: Check if this is right here or do we need a message metaType?
	// ToDo: if we only update the metadata, retentionTime does not restart
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
