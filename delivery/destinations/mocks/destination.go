package mocks

import (
	"event-delivery-kafka/models"
)

type Destination interface {
	Receive(event ...models.Event) error
	Name() string
}