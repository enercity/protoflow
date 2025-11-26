package runtime

import (
	"encoding/json"
	"net/http"
)

func (s *Service) StartWebUIServer() {
	if !s.Conf.WebUIEnabled {
		return
	}

	port := s.Conf.WebUIPort
	if port == 0 {
		port = 8081
	}

	s.RegisterHTTPHandler(port, "/api/handlers", http.HandlerFunc(s.handleGetHandlers))
}

func (s *Service) handleGetHandlers(w http.ResponseWriter, r *http.Request) {
	s.handlersMu.RLock()
	defer s.handlersMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	// Allow CORS for development convenience if needed, but maybe not for now.
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if err := json.NewEncoder(w).Encode(s.handlers); err != nil {
		s.Logger.Error(r.Context(), "Failed to encode handlers", err, nil)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}
