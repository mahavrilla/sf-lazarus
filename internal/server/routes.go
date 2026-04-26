package server

import (
	"log/slog"
	"net/http"
)

// NewMux registers all restore API routes on a new ServeMux and wraps it
// with logging and panic-recovery middleware.
func NewMux(h *Handler, log *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /restore/preview", h.Preview)
	mux.HandleFunc("POST /restore/execute", h.Execute)
	mux.HandleFunc("GET /restore/status/", h.Status)

	return logging(log, recovery(log, mux))
}
