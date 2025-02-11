package utils

import "strings"

func ReplaceWithMap(s string, values map[string]string) string {
	for k, v := range values {
		s = strings.ReplaceAll(s, k, v)
	}
	return s
}
