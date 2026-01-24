package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMiddleware_Chain(t *testing.T) {
	// Simple middleware that appends a header
	headerMW := func(val string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add("X-Test", val)
				next.ServeHTTP(w, r)
			})
		}
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Chain: implementation -> MW2 -> MW1 (outer)
	// So execution is MW1 -> MW2 -> implementation
	chained := Chain(handler, headerMW("1"), headerMW("2"))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	chained.ServeHTTP(rec, req)

	// Check headers (order depends on implementation, but both should be present)
	vals := rec.Header()["X-Test"]
	if len(vals) != 2 {
		t.Errorf("Expected 2 X-Test headers, got %v", vals)
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	// Create a handler that takes 500ms
	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	// Timeout = 100ms (should fail)
	mw := TimeoutMiddleware(100 * time.Millisecond)
	handler := mw(slowHandler)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected 503 Service Unavailable, got %d", rec.Code)
	}
}

func TestTimeoutMiddleware_Success(t *testing.T) {
	// Fast handler
	fastHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Timeout = 1s (should pass)
	mw := TimeoutMiddleware(1 * time.Second)
	handler := mw(fastHandler)

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("Expected body 'ok', got '%s'", rec.Body.String())
	}
}

func TestMaxConcurrencyMiddleware(t *testing.T) {
	// Limit = 2
	mw := MaxConcurrencyMiddleware(2)

	// Handler that blocks until released
	block := make(chan struct{})
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
		w.WriteHeader(http.StatusOK)
	}))

	// Start 3 requests concurrenty
	var wg sync.WaitGroup
	codes := make(chan int, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("GET", "/", nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			codes <- rec.Code
		}()
	}

	// Give time for concurrent requests to hit the limit
	time.Sleep(50 * time.Millisecond)

	// Unblock handlers
	close(block)
	wg.Wait()
	close(codes)

	// We expect 2 OKs and 1 TooManyRequests (429)
	okCount := 0
	limitCount := 0
	for c := range codes {
		switch c {
		case http.StatusOK:
			okCount++
		case http.StatusTooManyRequests:
			limitCount++
		}
	}

	if okCount != 2 || limitCount != 1 {
		t.Errorf("Expected 2 OK and 1 TooManyRequests, got %d OK and %d Limit", okCount, limitCount)
	}
}

func TestLoggingMiddleware(t *testing.T) {
	// Capture logs? It prints to std log, harder to test without capturing output.
	// For now just ensure it doesn't panic and calls next.
	handler := LoggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Errorf("Expected 418 Teapot, got %d", rec.Code)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	// Handler that panics
	handler := RecoveryMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()

	// Should recover and return 500
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Middleware did not recover from panic")
			}
		}()
		handler.ServeHTTP(rec, req)
	}()

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 Internal Server Error, got %d", rec.Code)
	}
}

func TestServer_Lifecycle(t *testing.T) {
	// Pick a random port to avoid conflicts
	srv := New(":0")

	// Start server
	errChan := srv.Start()

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-errChan:
		t.Fatalf("Server failed to start: %v", err)
	default:
		// Healthy
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}
