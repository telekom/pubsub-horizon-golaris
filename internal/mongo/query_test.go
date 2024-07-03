package mongo

import (
	"eni.telekom.de/horizon2go/pkg/enum"
	"eni.telekom.de/horizon2go/pkg/message"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golaris/internal/config"
	"testing"
	"time"
)

func TestConnection_FindWaitingMessages(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		connection := Connection{
			client: mt.Client,
			config: &config.Mongo{
				Database:   "testdb",
				Collection: "testcollection",
			},
		}

		expectedMessage := message.StatusMessage{
			Status:         enum.StatusWaiting,
			SubscriptionId: "sub123",
		}

		mt.AddMockResponses(mtest.CreateCursorResponse(0, "testdb.testcollection", mtest.FirstBatch, bson.D{
			{"status", expectedMessage.Status},
			{"subscriptionId", expectedMessage.SubscriptionId},
		}))

		opts := options.Find()
		messages, err := connection.FindWaitingMessages(time.Now(), opts, expectedMessage.SubscriptionId)
		assert.NoError(t, err)
		assert.Len(t, messages, 1)
		assert.Equal(t, expectedMessage, messages[0])
	})
}
func TestConnection_FindDeliveringMessagesByDeliveryType(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("success", func(mt *mtest.T) {
		connection := Connection{
			client: mt.Client,
			config: &config.Mongo{
				Database:   "testdb",
				Collection: "testcollection",
			},
		}

		expectedMessage := message.StatusMessage{
			Status:         enum.StatusDelivering,
			DeliveryType:   enum.DeliveryTypeCallback,
			SubscriptionId: "sub123",
		}

		mt.AddMockResponses(mtest.CreateCursorResponse(0, "testdb.testcollection", mtest.FirstBatch, bson.D{
			{"status", expectedMessage.Status},
			{"deliveryType", expectedMessage.DeliveryType},
			{"subscriptionId", expectedMessage.SubscriptionId},
		}))

		opts := *options.Find()
		messages, err := connection.FindDeliveringMessagesByDeliveryType(string(enum.StatusDelivering), time.Now(), opts, enum.DeliveryTypeCallback)

		assert.NoError(t, err)
		assert.Len(t, messages, 1)
		assert.Equal(t, expectedMessage.Status, messages[0].Status)
		assert.Equal(t, expectedMessage.DeliveryType, messages[0].DeliveryType)
		assert.Equal(t, expectedMessage.SubscriptionId, messages[0].SubscriptionId)

	})

}
