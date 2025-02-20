// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"eni.telekom.de/galileo/client/options"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"time"
)

type Configuration struct {
	LogLevel       string         `mapstructure:"logLevel"`
	Port           int            `mapstructure:"port"`
	CircuitBreaker CircuitBreaker `mapstructure:"circuitBreaker"`
	HealthCheck    HealthCheck    `mapstructure:"healthCheck"`
	Republishing   Republishing   `mapstructure:"republishing"`
	WaitingHandler WaitingHandler `mapstructure:"waitingHandler"`
	Hazelcast      Hazelcast      `mapstructure:"hazelcast"`
	Kafka          Kafka          `mapstructure:"kafka"`
	Metrics        Metrics        `mapstructure:"metrics"`
	Mongo          Mongo          `mapstructure:"mongo"`
	Security       Security       `mapstructure:"security"`
	Tracing        Tracing        `mapstructure:"tracing"`
	Handler        Handler        `mapstructure:"handler"`
	Notifications  Notifications  `mapstructure:"notifications"`
}

type CircuitBreaker struct {
	OpenCheckInterval       time.Duration `mapstructure:"openCheckInterval"`
	OpenLoopDetectionPeriod time.Duration `mapstructure:"openLoopDetectionPeriod"`
	ExponentialBackoffBase  time.Duration `mapstructure:"exponentialBackoffBase"`
	ExponentialBackoffMax   time.Duration `mapstructure:"exponentialBackoffMax"`
}

type WaitingHandler struct {
	CheckInterval time.Duration `mapstructure:"checkInterval"`
	MinMessageAge time.Duration `mapstructure:"minMessageAge"`
	MaxMessageAge time.Duration `mapstructure:"maxMessageAge"`
}

type HealthCheck struct {
	SuccessfulResponseCodes []int         `mapstructure:"successfulResponseCodes"`
	CoolDownTime            time.Duration `mapstructure:"coolDownTime"`
}

type Republishing struct {
	CheckInterval          time.Duration `mapstructure:"checkInterval"`
	BatchSize              int64         `mapstructure:"batchSize"`
	ThrottlingIntervalTime time.Duration `mapstructure:"throttlingIntervalTime"`
	DeliveringStatesOffset time.Duration `mapstructure:"deliveringStatesOffset"`
}

type Hazelcast struct {
	ServiceDNS  string `mapstructure:"serviceDNS"`
	ClusterName string `mapstructure:"clusterName"`
	Caches      Caches `mapstructure:"caches"`
	LogLevel    string `mapstructure:"logLevel"`
}

type Caches struct {
	SubscriptionCache   string `mapstructure:"subscriptionCache"`
	CircuitBreakerCache string `mapstructure:"circuitBreakerCache"`
	HealthCheckCache    string `mapstructure:"healthCheckCache"`
	RepublishingCache   string `mapstructure:"republishingCache"`
	HandlerCache        string `mapstructure:"handlerCache"`
}

type Kafka struct {
	Brokers []string `mapstructure:"brokers"`
	Topics  []string `mapstructure:"topics"`
}

type Metrics struct {
	Enabled bool `mapstructure:"enabled"`
}

type Mongo struct {
	Url        string `mapstructure:"url"`
	Database   string `mapstructure:"database"`
	Collection string `mapstructure:"collection"`
	BulkSize   int    `mapstructure:"bulkSize"`
}

type Security struct {
	Enabled      bool     `mapstructure:"enabled"`
	Url          string   `mapstructure:"url"`
	ClientId     string   `mapstructure:"clientId"`
	ClientSecret []string `mapstructure:"clientSecret"`
}

type Tracing struct {
	CollectorEndpoint string `mapstructure:"collectorEndpoint"`
	DebugEnabled      bool   `mapstructure:"debugEnabled"`
	Enabled           bool   `mapstructure:"enabled"`
}

type Handler struct {
	Delivering string `mapstructure:"delivering"`
	Failed     string `mapstructure:"failed"`
}

type Notifications struct {
	Enabled    bool   `mapstructure:"enabled"`
	BaseUrl    string `mapstructure:"baseUrl"`
	LoopModulo int    `mapstructure:"loopModulo"`
	Mail       struct {
		Subject struct {
			OpenCircuitBreaker string `mapstructure:"openCircuitBreaker"`
			LoopDetected       string `mapstructure:"loopDetected"`
		} `mapstructure:"subject"`
		Sender     string `mapstructure:"sender"`
		SenderName string `mapstructure:"senderName"`
		Templates  struct {
			OpenCircuitBreaker string `mapstructure:"openCircuitBreaker"`
			LoopDetected       string `mapstructure:"loopDetected"`
		} `mapstructure:"templates"`
	} `mapstructure:"mail"`
	Auth struct {
		Enabled      bool   `mapstructure:"enabled"`
		Issuer       string `mapstructure:"issuer"`
		ClientId     string `mapstructure:"clientId"`
		ClientSecret string `mapstructure:"clientSecret"`
	} `mapstructure:"auth"`
}

func (n Notifications) Options() *options.ClientOptions {
	var opts = options.Client().
		SetBaseUrl(n.BaseUrl).
		SetDebug(log.Logger.GetLevel() == zerolog.DebugLevel).
		SetDryRun(false).
		SetTimeout(30 * time.Second).
		WithAuth(
			options.Auth().
				SetEnabled(n.Auth.Enabled).
				SetIssuer(n.Auth.Issuer).
				SetClientId(n.Auth.ClientId).
				SetClientSecret(n.Auth.ClientSecret),
		)

	return opts
}
