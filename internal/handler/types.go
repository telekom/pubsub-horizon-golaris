package handler

type HandlerEntry struct {
	Name string `json:"name"`
}

func NewHandlerEntry(lockKey string) HandlerEntry {
	return HandlerEntry{
		Name: lockKey,
	}
}
