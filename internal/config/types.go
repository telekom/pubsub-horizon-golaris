// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package config

import "time"

type Configuration struct {
	LogLevel             string `mapstructure:"logLevel"`
	Port                 int    `mapstructure:"port"`
	MockCbSubscriptionId string `mapstructure:"mockCbSubscriptionId"`

	SuccessfulResponseCodes []int         `mapstructure:"successfulResponseCodes"`
	RequestCooldownTime     time.Duration `mapstructure:"requestCooldownResetTime"`
	RepublishingBatchSize   int64         `mapstructure:"republishingBatchSize"`

	Polling Polling `mapstructure:"polling"`

	Security   Security   `mapstructure:"security"`
	Tracing    Tracing    `mapstructure:"tracing"`
	Kubernetes Kubernetes `mapstructure:"kubernetes"`

	Hazelcast Hazelcast `mapstructure:"hazelcast"`
	Kafka     Kafka     `mapstructure:"kafka"`
	Mongo     Mongo     `mapstructure:"mongo"`
}

type Security struct {
	Url          string `mapstructure:"url"`
	ClientId     string `mapstructure:"clientId"`
	ClientSecret string `mapstructure:"clientSecret"`
}

type Tracing struct {
	CollectorEndpoint string `mapstructure:"collectorEndpoint"`
	Https             bool   `mapstructure:"https"`
	DebugEnabled      bool   `mapstructure:"debugEnabled"`
	Enabled           bool   `mapstructure:"enabled"`
}

type Kubernetes struct {
	Namespace string `mapstructure:"namespace"`
}

type Hazelcast struct {
	ServiceDNS          string `mapstructure:"serviceDNS"`
	ClusterName         string `mapstructure:"clusterName"`
	Caches              Caches `mapstructure:"caches"`
	CustomLoggerEnabled bool   `mapstructure:"customLoggerEnabled"`
}

type Polling struct {
	OpenCbMessageInterval                 time.Duration `mapstructure:"openCbMessageInterval"`
	RepublishingOrCheckingMessageInterval time.Duration `mapstructure:"republishingOrCheckingMessageInterval"`
}

type Caches struct {
	SubscriptionCache   string `mapstructure:"subscription-cache"`
	CircuitBreakerCache string `mapstructure:"circuit-breaker-cache"`
	HealthCheckCache    string `mapstructure:"health-check-cache"`
	RepublishingCache   string `mapstructure:"republishing-cache"`
}

type Kafka struct {
	Brokers []string `mapstructure:"brokers"`
	Topics  []string `mapstructure:"topics"`
}

type Mongo struct {
	Url        string `mapstructure:"url"`
	Database   string `mapstructure:"database"`
	Collection string `mapstructure:"collection"`
	BulkSize   int    `mapstructure:"bulkSize"`
}
