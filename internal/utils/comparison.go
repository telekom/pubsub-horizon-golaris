// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package utils

func IfThenElse[T any](condition bool, thenValue T, elseValue T) T {
	if condition {
		return thenValue
	}
	return elseValue
}
