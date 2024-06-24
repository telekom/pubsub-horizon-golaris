package service

import (
	"eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"eni.telekom.de/horizon2go/pkg/util"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"golaris/internal/api"
	"golaris/internal/config"
	"golaris/internal/health_check"
	"golaris/internal/kafka"
	"golaris/internal/metrics"
	"golaris/internal/mock"
	"golaris/internal/mongo"
	"golaris/internal/tracing"
	"golaris/internal/utils"
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
	app.Get("/api/v1/circuit-breakers/:subscriptionId", api.GetCircuitBreakerMessage)

	cacheConfig := configureHazelcast()
	//Todo Refactor use of dependencies
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

	StartScheduler(deps)

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

func configureCaches(config hazelcast.Config) (utils.Dependencies, error) {
	var err error

	deps.SubCache, err = cache.NewCache[resource.SubscriptionResource](config)
	if err != nil {
		return deps, fmt.Errorf("error initializing Hazelcast subscription health cache: %v", err)
	}

	deps.CbCache, err = cache.NewCache[message.CircuitBreakerMessage](config)
	if err != nil {
		return deps, fmt.Errorf("error initializing CircuitBreaker health cache: %v", err)
	}

	deps.HealthCache, err = health_check.NewHealthCheckCache(config)
	if err != nil {
		return deps, fmt.Errorf("error initializing HealthCheck cache: %v", err)
	}

	return deps, nil
}

func Listen(port int) {
	log.Info().Msgf("Listening on port %d", port)
	err := app.Listen(fmt.Sprintf(":%d", port))
	if err != nil {
		panic(err)
	}

}
