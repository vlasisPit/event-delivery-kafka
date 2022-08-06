package mocks

import (
	"event-delivery-kafka/models"
	"fmt"
	"log"
)

type BigqueryMock struct {
	warehouse string
	database  string
	user      string
}

func (BigqueryMock) New() *BigqueryMock {
	return &BigqueryMock{
		warehouse: "BIGQUERY_WAREHOUSE",
		database:  "BIGQUERY_DATABASE",
		user:      "BIGQUERY_USER",
	}
}

func (bigqueryMock *BigqueryMock) Receive(event ...models.Event) error {
	log.Println(fmt.Sprintf("bigquery: Received event successfully for userId %v ", event[0].UserID))
	return nil
}

func (bigqueryMock *BigqueryMock) Name() string {
	return "bigquery"
}
