package main

import (
	"event-delivery-kafka/api"
	"time"
)

func main() {
	app := api.App{
		Port:               ":8080",
		Topic:              "event-log",
		BrokerAddress:      "localhost:9092",
		DestinationTimeout: 1 * time.Second,
	}
	app.Run()
}
