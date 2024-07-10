// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package republish

import "time"

type RepublishingCache struct {
	SubscriptionId   string    `json:"subscriptionId"`
	RepublishingUpTo time.Time `json:"republishingUpTo"`
	PostponedUntil   time.Time `json:"postponedUntil"`
}

type DBMessage struct {
	Topic       string
	Coordinates *Coordinates
}

type Coordinates struct {
	Partition int32
	Offset    int64
}

type KafkaMessage struct {
	Content string
}
