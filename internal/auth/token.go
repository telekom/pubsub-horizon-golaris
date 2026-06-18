// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

var Client = &http.Client{Timeout: 30 * time.Second}

func RetrieveToken(url, clientId, clientSecret string) (string, error) {
	requestBody := bytes.NewBufferString("grant_type=client_credentials")

	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, requestBody)
	if err != nil {
		return "", err
	}

	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth(clientId, clientSecret)

	response, err := Client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	log.Debug().Msgf("RetrieveToken statusCode is: %d", response.StatusCode)
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status code: %d", response.StatusCode)
	}

	var responseBody struct {
		AccessToken string `json:"access_token"`
	}

	if err = json.NewDecoder(response.Body).Decode(&responseBody); err != nil {
		return "", err
	}

	return responseBody.AccessToken, nil
}
