package server

import (
	"log"
	"net/http"
	"runtime/debug"
	"time"
)

// Middleware is a function that wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares to a handler.
// The first middleware in the list is the "outermost" one (executed first).
func Chain(h http.Handler, m ...Middleware) http.Handler {
	// Apply in reverse order so the first in list is the outer-most
	for i := len(m) - 1; i >= 0; i-- {
		h = m[i](h)
	}
	return h
}

// LoggingMiddleware logs request details and duration.
func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap ResponseWriter to capture status code
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			duration := time.Since(start)
			log.Printf("REQ: %s %s | STATUS: %d | DURATION: %v",
				r.Method, r.URL.Path, ww.status, duration)
		}()

		next.ServeHTTP(ww, r)
	})
}

// RecoveryMiddleware recovers from panics and returns 500.
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("PANIC: %v\nStack: %s", err, debug.Stack())
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}
