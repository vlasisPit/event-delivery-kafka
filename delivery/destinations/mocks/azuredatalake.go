package mocks

import (
	"event-delivery-kafka/models"
	"time"
)

type AzureDataLakeMock struct {
	warehouse string
	database  string
	user      string
}

func (AzureDataLakeMock) New() *AzureDataLakeMock {
	return &AzureDataLakeMock{
		warehouse: "AZUREDATALAKE_WAREHOUSE",
		database:  "AZUREDATALAKE_DATABASE",
		user:      "AZUREDATALAKE_USER",
	}
}

func (azureDataLakeMock *AzureDataLakeMock) Receive(event ...models.Event) error {
	time.Sleep(1200 * time.Millisecond)
	return nil
}

func (azureDataLakeMock *AzureDataLakeMock) Name() string {
	return "azureDataLakeMock"
}
