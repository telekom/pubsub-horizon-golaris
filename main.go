package main

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golaris/cmd"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/kafka"
	"golaris/internal/mock"
	"golaris/internal/mongo"
	"os"
)

func init() {
	config.Load()

	cache.Initialize()
	mongo.Initialize()
	kafka.Initialize()

	// TODO Mock cb-messages until comet is adapted
	mock.CreateMockedCircuitBreakerMessages(1)

	setLogLevel(config.Current.LogLevel)
}

func setLogLevel(level string) {
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

func main() {
	cmd.Execute()
}
