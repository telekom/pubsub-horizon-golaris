package auth

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	Client = &http.Client{}

	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestRetrieveToken_Success(t *testing.T) {
	token, err := RetrieveToken("https://httpstat.us/200", "client_id", "client_secret")

	assert.NoError(t, err)
	assert.NotNil(t, token)

}

func TestRetrieveToken_RequestError(t *testing.T) {
	token, err := RetrieveToken("http://invalid-url", "client_id", "client_secret")

	assert.Error(t, err)
	assert.Equal(t, "", token)
}
