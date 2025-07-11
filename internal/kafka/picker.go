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
	startime := time.Now()
	var partition, offset = *status.Coordinates.Partition, *status.Coordinates.Offset
	partConsumer, err := p.consumer.ConsumePartition(status.Topic, partition, offset)
	elapsedtime := time.Since(startime)
	log.Debug().Msgf("Kafka Pick: Creating consumer duration: %v", elapsedtime)
	if err != nil {
		return nil, err
	}

	defer func() {
		startime = time.Now()
		if err := partConsumer.Close(); err != nil {
			log.Error().Err(err).Msg("Could not close picker gracefully")
		}
		elapsedtime = time.Since(startime)
		log.Debug().Msgf("Kafka Pick: Closing consumer duration: %v", elapsedtime)
	}()
	startime = time.Now()
	consumerMessage := <-partConsumer.Messages()
	elapsedtime = time.Since(startime)
	log.Debug().Msgf("Kafka Pick: Reading message from kafka: %v", elapsedtime)
	return consumerMessage, nil
}
