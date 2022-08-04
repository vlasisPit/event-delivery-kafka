package models

import "time"

type KafkaMessage struct {
	Key       string
	Value     string
	Timestamp time.Time
}
