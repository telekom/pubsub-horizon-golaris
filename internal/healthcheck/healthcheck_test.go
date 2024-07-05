package healthcheck

import (
	"fmt"
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
		response, err := executeHealthRequestWithToken("https://example.com", method, subscription, token)
		if err != nil {
			t.Errorf("Error executing %s request: %v", method, err)
			continue
		}

		assert.NotNil(t, response)
		assert.Equal(t, 200, response.StatusCode)
	}
}

func Test_ExecuteHealthRequestWithToken_ErrorCases(t *testing.T) {
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
		callbackUrl := "https://invalid-url"
		response, err := executeHealthRequestWithToken(callbackUrl, method, mockSubscription, token)
		if err != nil {
			assert.Nil(t, response)
			assert.Error(t, err)

			expectedErrMsg := fmt.Sprintf("Failed to perform %s request to %s:", method, callbackUrl)
			assert.Contains(t, err.Error(), expectedErrMsg)

			log.Info().Msgf("Error: %v", err)
			continue
		}
	}

}
