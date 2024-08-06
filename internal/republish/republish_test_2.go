// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import (
	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/enum"
	"github.com/telekom/pubsub-horizon-go/resource"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	"pubsub-horizon-golaris/internal/config"
	"pubsub-horizon-golaris/internal/kafka"
	"pubsub-horizon-golaris/internal/mongo"
	"testing"
)

var mockHandler *kafka.Handler

func TestRepublishPendingEvents(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().ClientType(mtest.Mock))

	mt.Run("test republish pending events", func(mt *mtest.T) {
		mongo.CurrentConnection = &mongo.Connection{
			Client: mt.Client,
			Config: &config.Mongo{
				Database:   "testdb",
				Collection: "testcollection",
			},
		}

		config.Current.Republishing.BatchSize = 2

		subscription := &resource.SubscriptionResource{
			Spec: struct {
				Subscription resource.Subscription `json:"subscription"`
				Environment  string                `json:"environment"`
			}{
				Subscription: resource.Subscription{
					SubscriptionId: "sub123",
					DeliveryType:   enum.DeliveryTypeSse,
					Callback:       "http://new-callbackUrl/callback",
				},
			},
		}

		republishEntry := RepublishingCacheEntry{
			OldDeliveryType: "callback",
		}

		messages := []bson.D{
			{
				{"status", enum.StatusWaiting},
				{"subscriptionId", "sub123"},
				{"deliveryType", "sse"},
				{"topic", "test-topic"},
				{"coordinates", bson.D{
					{"partition", int32(0)},
					{"offset", int64(100)},
				}},
			},
			{
				{"status", enum.StatusWaiting},
				{"subscriptionId", "sub123"},
				{"deliveryType", "sse"},
				{"topic", "test-topic"},
				{"coordinates", bson.D{
					{"partition", int32(0)},
					{"offset", int64(101)},
				}},
			},
		}

		mt.AddMockResponses(
			mtest.CreateCursorResponse(1, "testdb.testcollection", mtest.FirstBatch, messages...),
			mtest.CreateCursorResponse(0, "testdb.testcollection", mtest.NextBatch),
		)

		mockConfig := mocks.NewTestConfig()
		mockConfig.Net.MaxOpenRequests = 1
		mockConfig.Version = sarama.V0_11_0_0

		mockProducer := mocks.NewSyncProducer(t, mockConfig)
		mockProducer.ExpectSendMessageAndSucceed()

		mockConsumer := mocks.NewConsumer(t, mockConfig)
		mockConsumer.ExpectConsumePartition("test-topic", 0, 100).YieldMessage(&sarama.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
			Key:       []byte("test-key"),
			Value:     []byte(`{"uuid": "123456", "event": {"id": "789"}}`),
		})

		mockHandler = &kafka.Handler{
			Consumer: mockConsumer,
			Producer: mockProducer,
		}
		kafka.CurrentHandler = mockHandler

		RepublishPendingEvents(subscription, republishEntry)

		for _, msg := range messages {
			assert.NotNil(t, msg)
		}

		assert.Equal(t, 2, len(messages))
	})
}
