package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestContains_ElementExists(t *testing.T) {
	slice := []int{200, 201, 500}

	exists := Contains(slice, 200)
	assert.True(t, exists)
}

func TestContains_ElementDoesNotExist(t *testing.T) {
	slice := []int{200, 201, 500}

	exists := Contains(slice, 204)
	assert.False(t, exists)
}

func TestContains_EmptySlice(t *testing.T) {
	var slice []int

	exists := Contains(slice, 501)
	assert.False(t, exists)
}
