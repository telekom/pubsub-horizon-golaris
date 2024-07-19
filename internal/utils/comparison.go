package utils

func IfThenElse[T any](condition bool, thenValue T, elseValue T) T {
	if condition {
		return thenValue
	}
	return elseValue
}
