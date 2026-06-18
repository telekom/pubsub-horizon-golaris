// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"net/http"
	"os"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	Client = &http.Client{}

	exitVal := m.Run()
	os.Exit(exitVal)
}

func TestRetrieveToken_Success(t *testing.T) {
	assertions := assert.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Exact URL match
	httpmock.RegisterResponder("POST", "https://validTokenUrl",
		httpmock.NewStringResponder(200, `{"access_token": "token"}`))

	token, err := RetrieveToken("https://validTokenUrl", "client_id", "client_secret")

	assertions.NoError(err)
	assertions.NotNil(token)
}

func TestRetrieveToken_RequestError(t *testing.T) {
	assertions := assert.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	// Exact URL match
	httpmock.RegisterResponder("POST", "https://invalidTokenUrl",
		httpmock.NewStringResponder(401, `{"error": "unauthorized"}`))

	token, err := RetrieveToken("http://invalidTokenUrl", "client_id", "client_secret")

	assertions.Error(err)
	assertions.Equal("", token)
}
