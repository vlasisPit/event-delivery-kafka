package server

import (
	"encoding/json"
	"event-delivery-kafka/api/utils"
	"event-delivery-kafka/kafka/components"
	"event-delivery-kafka/models"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

/**
According to https://www.alexedwards.net/blog/a-recap-of-request-handling
you should not use "http.HandleFunc" because of a security vulnerability issue.
Use "mux := http.NewServeMux()" instead
So as a rule of thumb it's a good idea to avoid the DefaultServeMux, and instead
use your own locally-scoped ServeMux, like we have been so far.
Check section "The DefaultServeMux" on article.
*/
func (s *Server) Initialize(port string) {
	s.initializeRoutes()
	err := http.ListenAndServe(port, s.Mux)
	if err != nil {
		panic(err)
	}
}

type Server struct {
	Mux     *http.ServeMux
	Producer *components.Producer
}

/**
Handle requests with path "/events" like
PUT /events
*/
func (s *Server) events(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case "PUT":
		s.ingest(writer, request)
		return
	default:
		utils.ConstructErrorResponse(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) ingest(writer http.ResponseWriter, request *http.Request) {
	bodyBytes, err := ioutil.ReadAll(request.Body)
	defer request.Body.Close()
	if err != nil {
		utils.ConstructErrorResponse(writer, err.Error(), http.StatusInternalServerError)
		return
	}

	ct := request.Header.Get("content-type")
	if ct != "application/json" {
		utils.ConstructErrorResponse(writer, fmt.Sprintf("need content-type 'application/json', but got '%s'", ct), http.StatusUnsupportedMediaType)
		return
	}

	var event models.Event
	err = json.Unmarshal(bodyBytes, &event)
	if err != nil {
		utils.ConstructErrorResponse(writer, err.Error(), http.StatusBadRequest)
		return
	}

	kafkaMessage := models.KafkaMessage{}.New(event.UserID, event.Payload, time.Now())
	err = s.Producer.Send(request.Context(), *kafkaMessage)
	if err != nil {
		utils.ConstructErrorResponse(writer, err.Error(), http.StatusInternalServerError)
		return
	} else {
		utils.ConstructSuccessfulResponse(writer, http.StatusOK, []byte("Message received successfully"))
	}
}