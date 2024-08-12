// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package kafka

import "github.com/IBM/sarama"

type Handler struct {
	Consumer sarama.Consumer
	Producer sarama.SyncProducer
}
