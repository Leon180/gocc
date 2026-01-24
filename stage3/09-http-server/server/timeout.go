package server

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// TimeoutMiddleware adds a deadline to the request context and returns 503 if exceeded.
func TimeoutMiddleware(timeout time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			r = r.WithContext(ctx)

			done := make(chan struct{})
			panicChan := make(chan any, 1)

			tw := &timeoutWriter{
				w: w,
				h: make(http.Header),
			}

			go func() {
				defer func() {
					if err := recover(); err != nil {
						panicChan <- err
					}
					close(done)
				}()
				next.ServeHTTP(tw, r)
			}()

			select {
			case p := <-panicChan:
				panic(p)
			case <-done:
				tw.mu.Lock()
				defer tw.mu.Unlock()
				// If handler finished, write the result
				// Copy headers
				for k, vv := range tw.h {
					for _, v := range vv {
						w.Header().Add(k, v)
					}
				}
				if tw.code != 0 {
					w.WriteHeader(tw.code)
				}
				w.Write(tw.buf)

			case <-ctx.Done():
				// Timed out
				tw.mu.Lock()
				defer tw.mu.Unlock()
				// We don't write to tw, we write to w directly
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("Request timed out"))
			}
		})
	}
}

// timeoutWriter buffers the response until we know we haven't timed out.
// This is a simplified version; for large bodies, you'd want streaming or a pipe.
type timeoutWriter struct {
	w    http.ResponseWriter
	h    http.Header
	code int
	buf  []byte
	mu   sync.Mutex
}

func (tw *timeoutWriter) Header() http.Header {
	return tw.h
}

func (tw *timeoutWriter) Write(p []byte) (int, error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.buf = append(tw.buf, p...)
	return len(p), nil
}

func (tw *timeoutWriter) WriteHeader(statusCode int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.code = statusCode
}
