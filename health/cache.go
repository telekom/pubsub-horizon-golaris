package health

import (
	"context"
	"github.com/hazelcast/hazelcast-go-client"
	"time"
)

type HealthCheck struct {
	Environment         string    `json:"environment"`
	Method              string    `json:"method"`
	CallbackUrl         string    `json:"callbackUrl"`
	CheckingFor         string    `json:"checkingFor"`
	LastChecked         time.Time `json:"lastChecked"`
	LastedCheckedStatus int       `json:"lastCheckedStatus"`
}

func NewHealthCheckCache(config hazelcast.Config) (*hazelcast.Map, error) {
	hazelcastClient, err := hazelcast.StartNewClientWithConfig(context.Background(), config)
	if err != nil {
		return nil, err
	}

	mapName := "healthCheckEntries"
	HealthCheckMap, err := hazelcastClient.GetMap(context.Background(), mapName)
	if err != nil {
		return nil, err
	}

	return HealthCheckMap, nil
}
