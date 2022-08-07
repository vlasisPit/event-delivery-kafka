package mocks

import (
	"errors"
	"event-delivery-kafka/models"
	"fmt"
	"log"
	"math/rand"
	"time"
)

type RedshiftMock struct {
	warehouse string
	database  string
	user      string
}

func (RedshiftMock) New() *RedshiftMock {
	rand.Seed(time.Now().UnixNano())
	return &RedshiftMock{
		warehouse: "REDSHIFT_WAREHOUSE",
		database:  "REDSHIFT_DATABASE",
		user:      "REDSHIFT_USER",
	}
}

func (redshiftMock *RedshiftMock) Receive(event ...models.Event) error {
	min := 0
	max := 10
	randomNum := rand.Intn(max-min) + min //between 0 and 9

	if randomNum >= 7 {
		log.Println(fmt.Sprintf("redshift: Received event successfully for userId %v ", event[0].UserID))
	} else {
		return errors.New(fmt.Sprintf("redshift: Can't receive event for userId %v ", event[0].UserID))
	}

	return nil
}

func (redshiftMock *RedshiftMock) Name() string {
	return "redshift"
}
