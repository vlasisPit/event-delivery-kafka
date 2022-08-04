package main

import "event-delivery-kafka/api"

func main() {
	app := api.App{Port: ":8080"}
	app.Run()
}