package config

type Configuration struct {
	LogLevel string `mapstructure:"logLevel"`

	Port int `mapstructure:"port"`

	Security Security `mapstructure:"security"`
	Tracing  Tracing  `mapstructure:"tracing"`

	Hazelcast Hazelcast `mapstructure:"hazelcast"`
	Kafka     Kafka     `mapstructure:"kafka"`
	Mongo     Mongo     `mapstructure:"mongo"`
}

type Security struct {
	Enabled        bool     `mapstructure:"enabled"`
	TrustedIssuers []string `mapstructure:"trustedIssuers"`
}

type Tracing struct {
	Enabled           bool   `mapstructure:"enabled"`
	CollectorEndpoint string `mapstructure:"collectorEndpoint"`
	Https             bool   `mapstructure:"https"`
	DebugEnabled      bool   `mapstructure:"debugEnabled"`
}

type Hazelcast struct {
	ServiceDNS string `mapstructure:"serviceDNS"`
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
