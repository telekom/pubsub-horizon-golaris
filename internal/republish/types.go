package republish

import "time"

type RepublishingCache struct {
	SubscriptionId   string    `json:"subscriptionId"`
	RepublishingUpTo time.Time `json:"republishingUpTo"`
	PostponedUntil   time.Time `json:"postponedUntil"`
}
