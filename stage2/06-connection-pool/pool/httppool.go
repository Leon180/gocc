package pool

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// HTTPConn wraps an HTTP client connection for pooling.
// This demonstrates pooling persistent connections to a specific host.
type HTTPConn struct {
	client    *http.Client
	transport *http.Transport
	baseURL   string
	createdAt time.Time
	mu        sync.Mutex
	closed    bool
}

// Close closes the HTTP connection.
func (c *HTTPConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.transport.CloseIdleConnections()
	return nil
}

// IsValid checks if the connection is still usable.
func (c *HTTPConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

// Do executes an HTTP request using this connection.
func (c *HTTPConn) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// Get performs an HTTP GET request.
func (c *HTTPConn) Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+url, nil)
	if err != nil {
		return nil, err
	}
	return c.client.Do(req)
}

// HTTPPool is a connection pool for HTTP connections to a specific host.
type HTTPPool struct {
	*ManagedPool
	baseURL string
}

// HTTPPoolConfig holds configuration for HTTPPool.
type HTTPPoolConfig struct {
	BaseURL             string        // Base URL for the target host
	MinSize             int           // Minimum connections
	MaxSize             int           // Maximum connections
	MaxIdleTime         time.Duration // Max idle time
	MaxLifetime         time.Duration // Max connection lifetime
	Timeout             time.Duration // Request timeout
	HealthCheckInterval time.Duration // Health check interval
}

// DefaultHTTPPoolConfig returns sensible defaults.
func DefaultHTTPPoolConfig(baseURL string) HTTPPoolConfig {
	return HTTPPoolConfig{
		BaseURL:             baseURL,
		MinSize:             2,
		MaxSize:             10,
		MaxIdleTime:         5 * time.Minute,
		MaxLifetime:         30 * time.Minute,
		Timeout:             30 * time.Second,
		HealthCheckInterval: 1 * time.Minute,
	}
}

// NewHTTPPool creates a new HTTP connection pool.
func NewHTTPPool(config HTTPPoolConfig) *HTTPPool {
	factory := func(ctx context.Context) (Conn, error) {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          1,
			MaxIdleConnsPerHost:   1,
			IdleConnTimeout:       config.MaxIdleTime,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		client := &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
		}

		return &HTTPConn{
			client:    client,
			transport: transport,
			baseURL:   config.BaseURL,
			createdAt: time.Now(),
		}, nil
	}

	pool := NewManaged(factory, ManagedConfig{
		MinSize:             config.MinSize,
		MaxSize:             config.MaxSize,
		MaxIdleTime:         config.MaxIdleTime,
		MaxLifetime:         config.MaxLifetime,
		HealthCheckInterval: config.HealthCheckInterval,
	})

	return &HTTPPool{
		ManagedPool: pool,
		baseURL:     config.BaseURL,
	}
}

// Acquire gets an HTTP connection from the pool.
func (p *HTTPPool) Acquire(ctx context.Context) (*HTTPConn, error) {
	conn, err := p.ManagedPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Conn.(*HTTPConn), nil
}

// Get performs a GET request using a pooled connection.
// The connection is automatically released after the request.
func (p *HTTPPool) Get(ctx context.Context, path string) (*http.Response, error) {
	conn, err := p.ManagedPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	return conn.Conn.(*HTTPConn).Get(ctx, path)
}

// WithConn executes a function with a pooled HTTP connection.
func (p *HTTPPool) WithConn(ctx context.Context, fn func(*HTTPConn) error) error {
	conn, err := p.ManagedPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return fn(conn.Conn.(*HTTPConn))
}
