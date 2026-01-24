package server

import (
	"log"
	"net/http"
)

// MaxConcurrencyMiddleware limits the number of concurrent requests.
func MaxConcurrencyMiddleware(limit int) Middleware {
	// Semaphore using buffered channel
	sem := make(chan struct{}, limit)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case sem <- struct{}{}:
				// Acquired token
				defer func() { <-sem }() // Release token
				next.ServeHTTP(w, r)
			default:
				// Limit reached
				log.Printf("Limit reached! Rejecting request from %s", r.RemoteAddr)
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Too many requests"))
			}
		})
	}
}
