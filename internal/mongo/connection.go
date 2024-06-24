package mongo

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/config"
)

type Connection struct {
	client *mongo.Client
	config *config.Mongo
}

func NewMongoConnection(config *config.Mongo) (*Connection, error) {
	log.Debug().Msg("Connecting to mongoDB")
	connection, err := mongo.Connect(context.Background(), options.Client().ApplyURI(config.Url))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to mongoDB")
		return nil, err
	}

	if err = pingMongoNode(connection); err != nil {
		return nil, err
	}

	return &Connection{
		client: connection,
		config: config,
	}, nil
}

func pingMongoNode(connection *mongo.Client) error {
	log.Debug().Msg("Sending ping to mongoDB")

	if err := connection.Ping(context.TODO(), nil); err != nil {
		log.Error().Err(err).Msg("Could not reach primary mongoDB node")
		return err
	}

	log.Info().Msg("Connected to MongoDB established")
	return nil
}
