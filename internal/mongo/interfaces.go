package mongo

import (
	"github.com/telekom/pubsub-horizon-go/message"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type HandlerInterface interface {
	FindWaitingMessages(timestamp time.Time, pageable *options.FindOptions, subscriptionId string) ([]message.StatusMessage, error)
	FindFailedMessagesWithXYZException(status string, timestamp time.Time, pageable options.FindOptions) ([]message.StatusMessage, error)
	FindDeliveringMessagesByDeliveryType(status string, timestamp time.Time, pageable options.FindOptions, deliveryType string) ([]message.StatusMessage, error)
}
