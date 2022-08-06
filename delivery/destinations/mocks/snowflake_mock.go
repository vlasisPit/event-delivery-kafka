package mocks

import (
	"errors"
	"event-delivery-kafka/models"
	"fmt"
)

type SnowflakeMock struct {
	warehouse string
	database  string
	user      string
}

func (SnowflakeMock) New() *SnowflakeMock {
	return &SnowflakeMock{
		warehouse: "SNOWFLAKE_WAREHOUSE",
		database:  "SNOWFLAKE_DATABASE",
		user:      "SNOWFLAKE_USER",
	}
}

func (snowflakeMock *SnowflakeMock) Receive(event ...models.Event) error {
	return errors.New(fmt.Sprintf("snowflake: Can't receive event for userId %v ", event[0].UserID))
}

func (snowflakeMock *SnowflakeMock) Name() string {
	return "snowflake"
}