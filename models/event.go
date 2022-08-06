package models

type Event struct {
	UserID  string `json:"user_id"`
	Payload string `json:"payload"`
}

func (Event) New(userId string, payload string) *Event {
	return &Event{userId, payload}
}