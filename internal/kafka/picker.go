// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"pubsub-horizon-golaris/internal/config"
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
	var partition, offset = *status.Coordinates.Partition, *status.Coordinates.Offset
	partConsumer, err := p.consumer.ConsumePartition(status.Topic, partition, offset)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err := partConsumer.Close(); err != nil {
			log.Error().Err(err).Msg("Could not close picker gracefully")
		}
	}()

	return <-partConsumer.Messages(), nil
}
