// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"github.com/rs/zerolog/log"
	"pubsub-horizon-golaris/internal/config"
)

// InitializeWithConfluent initializes Kafka components using Confluent Kafka
func InitializeWithConfluent() {
	log.Info().Msg("Initializing Kafka with Confluent Kafka Go client")
	
	// Replace the default Sarama picker with Confluent implementation
	NewPicker = NewConfluentPicker
	
	// Create and set the handler
	handler, err := NewConfluentHandler()
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing Confluent Kafka handler")
	}
	
	CurrentHandler = handler
	log.Info().Msg("Successfully initialized Confluent Kafka client")
}

// InitializeWithSarama initializes Kafka components using Sarama (legacy)
func InitializeWithSarama() {
	log.Info().Msg("Initializing Kafka with Sarama client (legacy)")
	Initialize() // Use the existing Sarama initialization
}

// InitializeKafka determines which client to use based on configuration
func InitializeKafka() {
	// Use configuration to determine which client to use
	if config.Current.Kafka.UseConfluent {
		InitializeWithConfluent()
	} else {
		InitializeWithSarama()
	}
}