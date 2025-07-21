// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/config"
	"time"
)

type Picker struct {
	consumer sarama.Consumer
}

type MessagePicker interface {
	Close()
	Pick(status *message.StatusMessage) (*sarama.ConsumerMessage, error)
}

var NewPicker = func() (MessagePicker, error) {
	var saramaConfig = sarama.NewConfig()
	consumer, err := sarama.NewConsumer(config.Current.Kafka.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}
	return &Picker{consumer}, nil
}

func (p *Picker) Close() {
	if err := p.consumer.Close(); err != nil {
		log.Error().Err(err).Msg("Could not close picker gracefully")
	}
}

func (p *Picker) Pick(status *message.StatusMessage) (*sarama.ConsumerMessage, error) {

	// ToDo Performance tracking: Record start time
	startime := time.Now()

	var partition, offset = *status.Coordinates.Partition, *status.Coordinates.Offset
	partConsumer, err := p.consumer.ConsumePartition(status.Topic, partition, offset)

	// ToDo Performance tracking: Record elapsed time
	log.Debug().Msgf("Sarama Kafka Pick: Creating consumer duration: %v", time.Since(startime))
	if err != nil {
		return nil, err
	}

	defer func() {

		// ToDo Performance tracking: Record start time
		startime = time.Now()

		if err := partConsumer.Close(); err != nil {
			log.Error().Err(err).Msg("Could not close picker gracefully")
		}

		// ToDo Performance tracking: Record elapsed time
		log.Debug().Msgf("Sarama Kafka Pick: Closing consumer duration: %v", time.Since(startime))
	}()
	// ToDo Performance tracking: Record start time
	startime = time.Now()

	consumerMessage := <-partConsumer.Messages()

	// ToDo Performance tracking: Record elapsed time
	log.Debug().Msgf("Sarama Kafka Pick: Reading message from kafka: %v", time.Since(startime))
	return consumerMessage, nil
}
