package pool

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestHTTPPool_Basic(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Hello"))
	}))
	defer server.Close()

	pool := NewHTTPPool(HTTPPoolConfig{
		BaseURL: server.URL,
		MinSize: 1,
		MaxSize: 3,
	})
	defer pool.Close()

	// Warm up
	pool.WarmUp(context.Background())

	// Make a request
	resp, err := pool.Get(context.Background(), "/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPPool_Concurrent(t *testing.T) {
	requestCount := 0
	mu := sync.Mutex{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond) // Slow response
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pool := NewHTTPPool(HTTPPoolConfig{
		BaseURL: server.URL,
		MinSize: 2,
		MaxSize: 5,
	})
	defer pool.Close()

	var wg sync.WaitGroup

	// 20 concurrent requests
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := pool.Get(context.Background(), "/")
			if err != nil {
				t.Logf("Request error: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}

	wg.Wait()

	mu.Lock()
	count := requestCount
	mu.Unlock()

	if count != 20 {
		t.Errorf("Expected 20 requests, got %d", count)
	}

	stats := pool.Stats()
	t.Logf("Pool stats: total=%d, acquired=%d", stats.Total, stats.Acquired)

	// Should not exceed max size
	if stats.Total > 5 {
		t.Errorf("Exceeded max size: %d", stats.Total)
	}
}

func TestHTTPPool_WithConn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pool := NewHTTPPool(HTTPPoolConfig{
		BaseURL: server.URL,
		MaxSize: 3,
	})
	defer pool.Close()

	err := pool.WithConn(context.Background(), func(conn *HTTPConn) error {
		resp, err := conn.Get(context.Background(), "/")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return nil
	})

	if err != nil {
		t.Errorf("WithConn failed: %v", err)
	}
}

func TestHTTPConn_IsValid(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pool := NewHTTPPool(HTTPPoolConfig{
		BaseURL: server.URL,
		MaxSize: 1,
	})
	defer pool.Close()

	conn, _ := pool.Acquire(context.Background())

	if !conn.IsValid() {
		t.Error("New connection should be valid")
	}

	conn.Close()

	if conn.IsValid() {
		t.Error("Closed connection should be invalid")
	}
}
