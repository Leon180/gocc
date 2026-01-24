package server

import (
	"io"
	"net/http"
	"time"
)

// HealthHandler returns a simple 200 OK.
func HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// EchoHandler reads the request body and writes it back to the response.
func EchoHandler(w http.ResponseWriter, r *http.Request) {
	// Copy body to response
	if _, err := io.Copy(w, r.Body); err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
}

// SlowHandler simulates a long-running process to test timeouts.
// We'll add this to the router later when we implement timeout middleware.
func SlowHandler(w http.ResponseWriter, r *http.Request) {
	select {
	case <-time.After(2 * time.Second):
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Finished slow work"))
	case <-r.Context().Done():
		// Request cancelled (client disconnected or timeout)
		// No need to write response, just stop work
	}
}
