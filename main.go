package main

import "event-delivery-kafka/api"

func main() {
	app := api.App{
		Port: ":8080",
		Topic: "event-log",
		BrokerAddress: "localhost:9092",
	}
	app.Run()
}
