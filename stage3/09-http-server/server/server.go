package server

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// Server wraps the http.Server to provide a clean Start/Stop interface.
type Server struct {
	httpServer *http.Server
}

// New creates a new HTTP server on the specified address.
func New(addr string) *Server {
	mux := http.NewServeMux()

	// Register handlers
	mux.HandleFunc("GET /health", HealthHandler)
	mux.HandleFunc("POST /echo", EchoHandler)
	mux.HandleFunc("GET /slow", SlowHandler)

	// Apply middleware
	// Order: Recovery -> MaxConcurrency -> Logging -> Timeout -> Mux
	handler := Chain(mux,
		RecoveryMiddleware,
		MaxConcurrencyMiddleware(10), // Limit to 10 concurrent requests
		LoggingMiddleware,
		TimeoutMiddleware(1*time.Second), // Global 1s timeout
	)

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: handler,
			// Production readiness settings
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// Start runs the server in a non-blocking way.
// It returns a channel that will receive any startup error.
func (s *Server) Start() <-chan error {
	errChan := make(chan error, 1)
	go func() {
		fmt.Printf("Starting server on %s\n", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
		close(errChan)
	}()
	return errChan
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	fmt.Println("Shutting down server...")
	return s.httpServer.Shutdown(ctx)
}
