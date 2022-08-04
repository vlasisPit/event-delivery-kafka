package server

import (
	"event-delivery-kafka/api/utils"
	"net/http"
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

}