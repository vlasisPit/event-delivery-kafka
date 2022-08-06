package mocks

import (
	"errors"
	"event-delivery-kafka/models"
	"fmt"
)

type PostgresMock struct {
	warehouse string
	database  string
	user      string
}

func (PostgresMock) New() *PostgresMock {
	return &PostgresMock{
		warehouse: "POSTGRES_WAREHOUSE",
		database:  "POSTGRES_DATABASE",
		user:      "POSTGRES_USER",
	}
}

func (postgresMock *PostgresMock) Receive(event ...models.Event) error {
	return errors.New(fmt.Sprintf("postgres: Can't receive event for userId %v ", event[0].UserID))
}

func (postgresMock *PostgresMock) Name() string {
	return "postgres"
}