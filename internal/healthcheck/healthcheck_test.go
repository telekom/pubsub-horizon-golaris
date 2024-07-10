// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck

import (
	"context"
	"fmt"
	"github.com/jarcoal/httpmock"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/cache"
	"golaris/internal/config"
	"golaris/internal/test"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	test.DockerMutex.Lock()
	test.SetupDocker(&test.Options{
		MongoDb:   false,
		Hazelcast: true,
	})
	config.Current = test.BuildTestConfig()
	cache.Initialize()
	code := m.Run()

	test.TeardownDocker()
	time.Sleep(5000 * time.Millisecond)
	test.DockerMutex.Unlock()
	os.Exit(code)
}

func Test_CheckConsumerHealthSuccess(t *testing.T) {
	var assertions = assert.New(t)

	config.Current.Security.Url = "https://tokenUrl"

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock exact URL match
	httpmock.RegisterResponder("HEAD", "https://test.com",
		httpmock.NewStringResponder(200, `{}`))

	httpmock.RegisterResponder("POST", "https://tokenUrl",
		httpmock.NewStringResponder(200, `{"access_token": "token"}`))

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				PublisherId:           "pub-123",
				SubscriberId:          "sub-456",
				EnforceGetHealthCheck: false,
			},
		},
	}
	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "https://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	preparedHealthCheck, _ := PrepareHealthCheck(testSubscriptionResource)

	// call the function under test
	err := CheckConsumerHealth(preparedHealthCheck, subscription)

	healthCheckEntry, _ := cache.HealthCheckCache.Get(context.Background(), preparedHealthCheck.HealthCheckKey)
	castedHealthCheckEntry := healthCheckEntry.(HealthCheck)

	assertions.Equal(2, len(httpmock.GetCallCountInfo()))
	assertions.NoError(err)
	assertions.Equal(200, castedHealthCheckEntry.LastCheckedStatus)
	assertions.NotNil(castedHealthCheckEntry.LastChecked)
}

func Test_ExecuteHealthRequestWithToken(t *testing.T) {
	var assertions = assert.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock exact URL match
	httpmock.RegisterResponder("GET", "https://consumerUrl",
		httpmock.NewStringResponder(200, `{}`))

	httpmock.RegisterResponder("HEAD", "https://consumerUrl",
		httpmock.NewStringResponder(200, `{}`))

	subscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				PublisherId:  "pub-123",
				SubscriberId: "sub-456",
			},
		},
	}

	token := "dummy_token"

	for _, method := range []string{"GET", "HEAD"} {
		response, err := executeHealthRequestWithToken("https://consumerUrl", method, subscription, token)
		if err != nil {
			t.Errorf("Error executing %s request: %v", method, err)
			continue
		}

		assertions.NotNil(response)
		assertions.Equal(200, response.StatusCode)
	}
}

func Test_ExecuteHealthRequestWithToken_ErrorCases(t *testing.T) {
	var assertions = assert.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock exact URL match
	httpmock.RegisterResponder("GET", "https://invalidConsumerUrl",
		httpmock.NewStringResponder(400, `{"error": "url not found"}`))

	httpmock.RegisterResponder("HEAD", "https://invalidConsumerUrl",
		httpmock.NewStringResponder(400, `{"error": "url not found"}`))

	mockSubscription := &resource.SubscriptionResource{
		Spec: struct {
			Subscription resource.Subscription `json:"subscription"`
			Environment  string                `json:"environment"`
		}{
			Subscription: resource.Subscription{
				PublisherId:  "pub-123",
				SubscriberId: "sub-456",
			},
		},
	}

	token := "dummy_token"

	for _, method := range []string{"GET", "HEAD"} {
		callbackUrl := "https://invalidConsumerUrl"
		response, err := executeHealthRequestWithToken(callbackUrl, method, mockSubscription, token)
		assertions.Equal(400, response.StatusCode)
		if err != nil {
			assertions.Nil(response)
			assertions.Error(err)

			expectedErrMsg := fmt.Sprintf("Failed to perform %s request to %s:", method, callbackUrl)
			assertions.Contains(err.Error(), expectedErrMsg)

			log.Info().Msgf("Error: %v", err)
			continue
		}
	}

}

func TestPrepareHealthCheck_ExistingEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	healthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	healthCheckEntry := NewHealthCheckEntry(testSubscriptionResource, GetHttpMethod(testSubscriptionResource))
	healthCheckEntry.LastChecked = time.Now()
	err := cache.HealthCheckCache.Set(context.Background(), healthCheckKey, healthCheckEntry)

	assertions.NoError(err)

	// call the function under test
	preparedHealthCheck, err := PrepareHealthCheck(testSubscriptionResource)

	// assert the result
	assertions.NoError(err)
	assertions.NotNil(preparedHealthCheck)
	assertions.NotNil(preparedHealthCheck.Ctx)
	assertions.NotNil(preparedHealthCheck.HealthCheckEntry.LastChecked)
	assertions.Equal(healthCheckKey, preparedHealthCheck.HealthCheckKey)
	assertions.True(preparedHealthCheck.IsAcquired)
	assertions.True(cache.HealthCheckCache.IsLocked(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey))

	// Unlock the cache entry
	defer cache.HealthCheckCache.Unlock(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey)
}

func TestGetHttpMethod_HeadMethod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testSubscriptionResource.Spec.Subscription.EnforceGetHealthCheck = false

	// call the function under test
	httpMethod := GetHttpMethod(testSubscriptionResource)

	// assert the result
	assertions.Equal("HEAD", httpMethod)
}

func TestGetHttpMethod_GetMethod(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	testSubscriptionResource.Spec.Subscription.EnforceGetHealthCheck = true

	// call the function under test
	httpMethod := GetHttpMethod(testSubscriptionResource)

	// assert the result
	assertions.Equal("GET", httpMethod)
}

func TestPrepareHealthCheck_NewEntry(t *testing.T) {
	defer test.ClearCaches()
	var assertions = assert.New(t)

	// Prepare test data
	testSubscriptionId := "testSubscriptionId"
	testEnvironment := "test"
	testCallbackUrl := "http://test.com"

	testSubscriptionResource := test.NewTestSubscriptionResource(testSubscriptionId, testCallbackUrl, testEnvironment)
	healthCheckKey := fmt.Sprintf("%s:%s:%s", testEnvironment, GetHttpMethod(testSubscriptionResource), testCallbackUrl)

	// call the function under test
	preparedHealthCheck, err := PrepareHealthCheck(testSubscriptionResource)

	// assert the result
	assertions.NoError(err)
	assertions.NotNil(preparedHealthCheck)
	assertions.NotNil(preparedHealthCheck.Ctx)
	assertions.Equal(healthCheckKey, preparedHealthCheck.HealthCheckKey)
	assertions.True(preparedHealthCheck.IsAcquired)
	assertions.True(cache.HealthCheckCache.IsLocked(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey))

	// Unlock the cache entry
	defer cache.HealthCheckCache.Unlock(preparedHealthCheck.Ctx, preparedHealthCheck.HealthCheckKey)
}
