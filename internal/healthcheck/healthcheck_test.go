package healthcheck

import (
	"fmt"
	"github.com/jarcoal/httpmock"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/telekom/pubsub-horizon-go/resource"
	"golaris/internal/auth"
	"net/http"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	auth.Client = &http.Client{}

	exitVal := m.Run()
	os.Exit(exitVal)
}

func Test_ExecuteHealthRequestWithToken(t *testing.T) {
	var assertions = assert.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Mock exact URL match
	httpmock.RegisterResponder("GET", "https://validTokenUrl",
		httpmock.NewStringResponder(200, `{"access_token": "token"}`))

	httpmock.RegisterResponder("HEAD", "https://validTokenUrl",
		httpmock.NewStringResponder(200, `{"access_token": "token"}`))

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
		response, err := executeHealthRequestWithToken("https://validTokenUrl", method, subscription, token)
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
	httpmock.RegisterResponder("GET", "https://invalidTokenUrl",
		httpmock.NewStringResponder(400, `{"error": "url not found"}`))

	httpmock.RegisterResponder("HEAD", "https://invalidTokenUrl",
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
		callbackUrl := "https://invalidTokenUrl"
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
