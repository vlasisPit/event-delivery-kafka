package server

func (s *Server) initializeRoutes() {
	s.Mux.HandleFunc("/events", s.events)
}