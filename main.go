package main

import (
	"event-delivery-kafka/api"
	"event-delivery-kafka/delivery/destinations/mocks"
	"time"
)

func main() {
	destinations := []mocks.Destination{
		mocks.BigqueryMock{}.New(),
		mocks.PostgresMock{}.New(),
		mocks.SnowflakeMock{}.New(),
		mocks.AzureDataLakeMock{}.New(),
	}

	app := api.App{
		Port:               ":8080",
		Topic:              "event-log",
		BrokerAddress:      "localhost:9092",
		DestinationTimeout: 1 * time.Second,
		Destinations:       destinations,
	}
	app.Run()
}
