package service

import (
	"eni.telekom.de/horizon2go/pkg/util"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"golaris/config"
	"golaris/health_check"
	"golaris/kafka"
	"golaris/metrics"
	"golaris/mock"
	"golaris/mongo"
	"golaris/tracing"
	"golaris/utils"
)

var (
	app  *fiber.App
	deps utils.Dependencies
)

func InitializeService() {
	var err error
	app = fiber.New()

	app.Use(tracing.Middleware())
	app.Use(healthcheck.New())

	app.Get("/metrics", metrics.NewPrometheusMiddleware())
	app.Get("/api/v1/circuit-breakers/:subscriptionId", getCircuitBreakerMessage)

	cacheConfig := configureHazelcast()
	deps, err = configureCaches(cacheConfig)
	if err != nil {
		log.Panic().Err(err).Msg("Error while configuring caches")
	}

	deps.MongoConn, err = mongo.NewMongoConnection(&config.Current.Mongo)
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing MongoDB connection")
	}

	deps.KafkaHandler, err = kafka.NewKafkaHandler()
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing Kafka Picker")
	}

	health_check.InitializeScheduler(deps)

	// TODO Mock cb-messages until comet is adapted
	mock.CreateMockedCircuitBreakerMessages(deps.CbCache, 1)
}

func configureHazelcast() hazelcast.Config {
	cacheConfig := hazelcast.NewConfig()

	cacheConfig.Cluster.Name = config.Current.Hazelcast.ClusterName
	cacheConfig.Cluster.Network.SetAddresses(config.Current.Hazelcast.ServiceDNS)
	cacheConfig.Logger.CustomLogger = new(util.HazelcastZerologLogger)

	return cacheConfig
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)
	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

}
