package service

import (
	"eni.telekom.de/horizon2go/pkg/cache"
	"eni.telekom.de/horizon2go/pkg/message"
	"eni.telekom.de/horizon2go/pkg/resource"
	"eni.telekom.de/horizon2go/pkg/util"
	"fmt"
	jwtware "github.com/gofiber/contrib/jwt"
	"github.com/gofiber/fiber/v2"
	"github.com/hazelcast/hazelcast-go-client"
	"github.com/rs/zerolog/log"
	"golaris/config"
	"golaris/kafka"
	"golaris/metrics"
	"golaris/mongo"
	"golaris/tracing"
)

var (
	subCacheInstance *cache.Cache[resource.SubscriptionResource]
	cbCacheInstance  *cache.Cache[message.StatusMessage]
	kafkaHandler     *kafka.Handler
	app              *fiber.App
	mongoConnection  *mongo.Connection
)

func InitializeService() {
	var err error
	app = fiber.New()

	app.Use(configureSecurity())
	app.Use(tracing.Middleware())

	app.Get("/metrics", metrics.NewPrometheusMiddleware())
	app.Get("/api/v1/circuit-breakers/:subscriptionId", getCircuitBreakerMessage)

	cacheConfig := configureHazelcast()
	subCacheInstance, err = cache.NewCache[resource.SubscriptionResource](cacheConfig)
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing Hazelcast cache")
	}

	cbCacheInstance, err = cache.NewCache[message.StatusMessage](cacheConfig)
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing CircuitBreaker cache")
	}

	mongoConnection, err = mongo.NewMongoConnection(&config.Current.Mongo)
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing MongoDB connection")
	}

	kafkaHandler, err = kafka.NewKafkaHandler()
	if err != nil {
		log.Panic().Err(err).Msg("Error while initializing Kafka Picker")
	}
}

func configureSecurity() fiber.Handler {
	return jwtware.New(jwtware.Config{
		Filter: func(ctx *fiber.Ctx) bool {
			return !config.Current.Security.Enabled || ctx.Path() == "/metrics"
		},
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
				"error": "Unauthorized",
			})
		},
		JWKSetURLs: config.Current.Security.TrustedIssuers,
	})
}

func configureHazelcast() hazelcast.Config {
	cacheConfig := hazelcast.NewConfig()

	// ToDO: Configure cluster name in config
	cacheConfig.Cluster.Name = "dev"
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
