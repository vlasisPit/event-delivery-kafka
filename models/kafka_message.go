package models

import "time"

type KafkaMessage struct {
	Key       string
	Value     string
	Timestamp time.Time
}

func (KafkaMessage) New(key string, value string, timestamp time.Time) *KafkaMessage {
	return &KafkaMessage{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}
}
