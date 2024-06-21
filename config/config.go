package config

import "time"

type Configuration struct {
	LogLevel string `mapstructure:"logLevel"`
	Port     int    `mapstructure:"port"`

	SuccessfulResponseCodes []int         `mapstructure:"successfulResponseCodes"`
	RequestCooldownTime     time.Duration `mapstructure:"requestCooldownResetTime"`
	RepublishingBatchSize   int64         `mapstructure:"republishingBatchSize"`

	Polling Polling `mapstructure:"polling"`

	Security Security `mapstructure:"security"`
	Tracing  Tracing  `mapstructure:"tracing"`

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

type Hazelcast struct {
	ServiceDNS  string `mapstructure:"serviceDNS"`
	ClusterName string `mapstructure:"clusterName"`
	Caches      Caches `mapstructure:"caches"`
}

type Polling struct {
	OpenCbMessageInterval                 time.Duration `mapstructure:"openCbMessageInterval"`
	RepublishingOrCheckingMessageInterval time.Duration `mapstructure:"republishingOrCheckingMessageInterval"`
}

type Caches struct {
	SubscriptionCache   string `mapstructure:"subscription-cache"`
	CircuitBreakerCache string `mapstructure:"circuit-breaker-cache"`
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
