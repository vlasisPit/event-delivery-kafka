package api

import (
	"event-delivery-kafka/api/server"
	"net/http"
)

type App struct {
	Port string
}

func (a *App) Run() {
	mux := http.NewServeMux()
	server := server.Server{
		Mux: mux,
	}
	server.Initialize(a.Port)
}