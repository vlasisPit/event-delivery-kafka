package api

import (
	"event-delivery-kafka/api/server"
	"event-delivery-kafka/kafka/components"
	"net/http"
)

type App struct {
	Port string
	Producer *components.Producer
}

func (a *App) Run() {
	mux := http.NewServeMux()
	server := server.Server{
		Mux: mux,
		Producer: a.Producer,
	}
	server.Initialize(a.Port)
}