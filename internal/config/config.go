// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"strings"
)

var Current Configuration

func Load() {
	configureViper()
	setDefaults()
	readConfiguration()

	if err := viper.Unmarshal(&Current); err != nil {
		log.Fatal().Err(err).Msg("Could not unmarshal current configuration!")
	}
}

func Initialize() error {
	configureViper()
	setDefaults()
	return viper.SafeWriteConfig()
}

func configureViper() {
	viper.SetConfigName("config")
	viper.SetConfigType("yml")
	viper.AddConfigPath(".")
	viper.SetEnvPrefix("golaris")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

func setDefaults() {
	// General
	viper.SetDefault("logLevel", "info")
	viper.SetDefault("port", 8080)

	// Processes
	viper.SetDefault("circuitBreaker.openCheckInterval", "30s")
	viper.SetDefault("circuitBreaker.openLoopDetectionPeriod", "75m")
	viper.SetDefault("circuitBreaker.exponentialBackoffBase", "1000ms")
	viper.SetDefault("circuitBreaker.exponentialBackoffMax", "60m")
	viper.SetDefault("healthCheck.successfulResponseCodes", []int{200, 201, 202, 204})
	viper.SetDefault("healthCheck.coolDownTime", "30s")
	viper.SetDefault("republishing.checkInterval", "30s")
	viper.SetDefault("republishing.batchSize", 10)
	viper.SetDefault("republishing.throttlingIntervalTime", "10s")
	viper.SetDefault("republishing.deliveringStatesOffset", "15m")

	// Caches
	viper.SetDefault("hazelcast.caches.subscriptionCache", "subscriptions.subscriber.horizon.telekom.de.v1")
	viper.SetDefault("hazelcast.caches.circuitBreakerCache", "circuit-breakers")
	viper.SetDefault("hazelcast.caches.healthCheckCache", "health-check-cache")
	viper.SetDefault("hazelcast.caches.republishingCache", "republishing-cache")

	// Hazelcast
	viper.SetDefault("hazelcast.clusterName", "dev")
	viper.SetDefault("hazelcast.serviceDNS", "localhost:5701")
	viper.SetDefault("hazelcast.logLevel", "info")

	// Kafka
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.topics", []string{"status"})

	// Metrics
	viper.SetDefault("metrics.enabled", true)

	// Mongo
	viper.SetDefault("mongo.url", "mongodb://localhost:27017")
	viper.SetDefault("mongo.database", "horizon")
	viper.SetDefault("mongo.collection", "status")
	viper.SetDefault("mongo.bulkSize", 50)

	// Security
	viper.SetDefault("security.enabled", true)
	viper.SetDefault("security.url", "iris")
	viper.SetDefault("security.clientId", "clientId")
	viper.SetDefault("security.clientSecret", []string{"realm=clientSecret"})

	// Tracing
	viper.SetDefault("tracing.enabled", true)
	viper.SetDefault("tracing.collectorEndpoint", "http://localhost:4318")
	viper.SetDefault("tracing.debugEnabled", false)

	// Handler
	viper.SetDefault("handler.delivering", "deliveringHandler")
	viper.SetDefault("handler.failed", "failedHandler")

}

func readConfiguration() *Configuration {
	if err := viper.ReadInConfig(); err != nil {
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			log.Info().Msg("Configuration file not found but environment variables will be taken into account!")
		}
	}

	viper.AutomaticEnv()

	var config Configuration
	if err := viper.Unmarshal(&config); err != nil {
		log.Fatal().Err(err).Msg("Could not unmarshal current configuration!")
	}

	return &config
}
