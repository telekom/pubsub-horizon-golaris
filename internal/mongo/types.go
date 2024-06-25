package mongo

import (
	"go.mongodb.org/mongo-driver/mongo"
	"golaris/internal/config"
)

type Connection struct {
	client *mongo.Client
	config *config.Mongo
}
