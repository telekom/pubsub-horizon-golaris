// Copyright 2025 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package utils

import "strings"

func ReplaceWithMap(s string, values map[string]string) string {
	for k, v := range values {
		s = strings.ReplaceAll(s, k, v)
	}
	return s
}
