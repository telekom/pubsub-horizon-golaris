package config

import (
	"errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
	"os"
	"strings"
)

var Current Configuration

func LoadConfiguration() {
	configureViper()
	setDefaults()
	readConfiguration()

	if err := viper.Unmarshal(&Current); err != nil {
		log.Fatal().Err(err).Msg("Could not unmarshal current configuration!")
	}

	applyLogLevel(Current.LogLevel)
}

func InitConfig() error {
	configureViper()
	setDefaults()
	return viper.SafeWriteConfig()
}

func configureViper() {
	viper.SetConfigName("config")
	viper.SetConfigType("yml")
	viper.AddConfigPath(".")
	viper.SetEnvPrefix("service")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

func setDefaults() {
	viper.SetDefault("logLevel", "info")
	viper.SetDefault("port", 8080)

	viper.SetDefault("successfulResponseCodes", []int{200, 201, 202, 204})
	viper.SetDefault("republishingBatchSize", 10)

	// Polling
	viper.SetDefault("polling.openCbMessageInterval", "10ms")
	viper.SetDefault("polling.republishingOrCheckingMessageInterval", "10ms")

	// Security
	viper.SetDefault("security.url", "iris")
	viper.SetDefault("security.clientId", "clientId")
	viper.SetDefault("security.clientSecret", "clientSecret")

	// Tracing
	viper.SetDefault("tracing.enabled", true)
	viper.SetDefault("tracing.collectorEndpoint", "http://localhost:4318")
	viper.SetDefault("tracing.https", true)
	viper.SetDefault("tracing.debugEnabled", false)

	// Kubernetes
	viper.SetDefault("kubernetes.namespace", "default")

	// Hazelcast
	viper.SetDefault("hazelcast.serviceDNS", "localhost:5701")
	viper.SetDefault("hazelcast.clusterName", "dev")

	// Caches
	viper.SetDefault("hazelcast.caches.subscription-cache", "subscriptions.subscriber.horizon.telekom.de.v1")
	viper.SetDefault("hazelcast.caches.circuit-breaker-cache", "circuit-breakers")
	viper.SetDefault("hazelcast.caches.health-check-cache", "health-checks")

	// Kafka
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.topics", []string{"status"})

	// Mongo
	viper.SetDefault("mongo.url", "mongodb://localhost:27017")
	viper.SetDefault("mongo.database", "horizon")
	viper.SetDefault("mongo.collection", "status")
	viper.SetDefault("mongo.bulkSize", 50)
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

func applyLogLevel(level string) {
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
		log.Info().Msgf("Invalid log level %s. Info log level is used", logLevel)
	}

	log.Logger = zerolog.New(os.Stdout).Level(logLevel).With().Timestamp().Logger()
	if logLevel == zerolog.DebugLevel {
		log.Logger = log.Logger.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}
}
