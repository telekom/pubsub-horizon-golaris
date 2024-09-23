// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
	"github.com/telekom/pubsub-horizon-go/message"
	"net"
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
	defer func() {
		if err := partConsumer.Close(); err != nil {
			log.Error().Err(err).Msg("Could not close picker gracefully")
		}
	}()

	if err != nil {
		var nErr *net.OpError
		if errors.As(err, &nErr) {
			return nil, err
		}
		var errorList = []error{
			sarama.ErrEligibleLeadersNotAvailable,
			sarama.ErrPreferredLeaderNotAvailable,
			sarama.ErrUnknownLeaderEpoch,
			sarama.ErrFencedLeaderEpoch,
			sarama.ErrNotLeaderForPartition,
			sarama.ErrLeaderNotAvailable,
		}
		for _, e := range errorList {
			if errors.Is(err, e) {
				return nil, err
			}
		}

		log.Warn().Err(err).Msgf("Could not fetch message from kafka for subscriptionId %s", status.SubscriptionId)

		return nil, nil
	}

	return <-partConsumer.Messages(), nil
}
