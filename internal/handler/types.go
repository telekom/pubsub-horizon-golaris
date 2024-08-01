package handler

import "encoding/gob"

func init() {
	gob.Register(HandlerEntry{})
}

type HandlerEntry struct {
	Name string `json:"name"`
}

func NewHandlerEntry(lockKey string) HandlerEntry {
	return HandlerEntry{
		Name: lockKey,
	}
}
