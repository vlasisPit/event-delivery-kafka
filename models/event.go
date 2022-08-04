package models

type Event struct {
	UserID  string `json:"user_id"`
	Payload string `json:"payload"`
}