package health_check

import "time"

type HealthCheck struct {
	Environment         string    `json:"environment"`
	Method              string    `json:"method"`
	CallbackUrl         string    `json:"callbackUrl"`
	RepublishingCount   int       `json:"republishingCount"`
	LastChecked         time.Time `json:"lastChecked"`
	LastedCheckedStatus int       `json:"lastCheckedStatus"`
}
