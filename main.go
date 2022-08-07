package main

import (
	"event-delivery-kafka/api"
	"event-delivery-kafka/delivery/destinations/mocks"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	err := godotenv.Load("config/.env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	destinations := []mocks.Destination{
		mocks.BigqueryMock{}.New(),
		mocks.PostgresMock{}.New(),
		mocks.SnowflakeMock{}.New(),
		mocks.AzureDataLakeMock{}.New(),
		mocks.RedshiftMock{}.New(),
	}

	app := api.App{
		Port:               os.Getenv("PORT"),
		Topic:              os.Getenv("TOPIC"),
		BrokerAddress:      os.Getenv("BROKER_ADDRESS"),
		DestinationTimeout: 1 * time.Second,
		Destinations:       destinations,
	}
	app.Run()
}
