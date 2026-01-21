package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed is returned when operating on a closed pool.
	ErrPoolClosed = errors.New("pool is closed")
	// ErrAcquireTimeout is returned when acquire times out.
	ErrAcquireTimeout = errors.New("acquire timeout")
	// ErrInvalidConn is returned when a connection fails validation.
	ErrInvalidConn = errors.New("invalid connection")
)

// Conn represents a pooled connection.
// Implementations must be safe for concurrent use.
type Conn interface {
	// Close closes the underlying connection.
	Close() error
	// IsValid checks if the connection is still usable.
	IsValid() bool
}

// Factory creates new connections.
type Factory func(ctx context.Context) (Conn, error)

// Pool is a generic connection pool.
//
// The pool uses a buffered channel to store idle connections.
// This provides natural blocking behavior when the pool is empty.
type Pool struct {
	factory   Factory
	conns     chan Conn
	semaphore chan struct{} // Controls max connections created
	mu        sync.Mutex
	closed    bool

	// Configuration
	maxSize     int
	maxIdleTime time.Duration

	// Statistics
	acquired  int64
	released  int64
	destroyed int64
}

// Config holds pool configuration.
type Config struct {
	MaxSize     int           // Maximum connections in pool
	MaxIdleTime time.Duration // Max time a connection can be idle (0 = no limit)
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxSize:     10,
		MaxIdleTime: 5 * time.Minute,
	}
}

// New creates a new connection pool.
func New(factory Factory, config Config) *Pool {
	if config.MaxSize <= 0 {
		config.MaxSize = 10
	}

	return &Pool{
		factory:     factory,
		conns:       make(chan Conn, config.MaxSize),
		semaphore:   make(chan struct{}, config.MaxSize), // Limit total connections
		maxSize:     config.MaxSize,
		maxIdleTime: config.MaxIdleTime,
	}
}

// Acquire gets a connection from the pool.
// Blocks until a connection is available or context is cancelled.
func (p *Pool) Acquire(ctx context.Context) (Conn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()

	// First, try to get an idle connection (non-blocking)
	select {
	case conn := <-p.conns:
		if conn.IsValid() {
			atomic.AddInt64(&p.acquired, 1)
			return conn, nil
		}
		// Invalid: destroy and release semaphore slot
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		<-p.semaphore // Free the slot
		return p.Acquire(ctx)
	default:
	}

	// No idle connection, try to acquire a semaphore slot to create new
	select {
	case p.semaphore <- struct{}{}: // Got a slot
		conn, err := p.factory(ctx)
		if err != nil {
			<-p.semaphore // Release slot on error
			return nil, err
		}
		atomic.AddInt64(&p.acquired, 1)
		return conn, nil
	default:
		// Pool is at capacity, must wait for idle connection
	}

	// Wait for either idle connection or context cancellation
	select {
	case conn := <-p.conns:
		if conn.IsValid() {
			atomic.AddInt64(&p.acquired, 1)
			return conn, nil
		}
		// Invalid: destroy and release slot
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		<-p.semaphore
		return p.Acquire(ctx)
	case <-ctx.Done():
		return nil, ErrAcquireTimeout
	}
}

// AcquireWithTimeout gets a connection with a timeout.
func (p *Pool) AcquireWithTimeout(timeout time.Duration) (Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.Acquire(ctx)
}

// Release returns a connection to the pool.
// If the connection is invalid, it will be destroyed instead.
func (p *Pool) Release(conn Conn) error {
	if conn == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		return ErrPoolClosed
	}
	p.mu.Unlock()

	// Validate before returning to pool
	if !conn.IsValid() {
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		return ErrInvalidConn
	}

	// Try to return to pool (non-blocking)
	select {
	case p.conns <- conn:
		atomic.AddInt64(&p.released, 1)
		return nil
	default:
		// Pool is full, destroy the connection
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		return nil
	}
}

// Close closes the pool and all connections.
func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	p.mu.Unlock()

	// Drain and close all connections
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
	}

	return nil
}

// Stats holds pool statistics.
type Stats struct {
	Total     int   // Total active connections (created - destroyed)
	Acquired  int64 // Total successful acquires
	Released  int64 // Total successful releases
	Destroyed int64 // Total connections destroyed
	Idle      int   // Current idle connections
	InUse     int   // Current connections in use
}

// Stats returns current pool statistics.
func (p *Pool) Stats() Stats {
	destroyed := atomic.LoadInt64(&p.destroyed)
	released := atomic.LoadInt64(&p.released)
	acquired := atomic.LoadInt64(&p.acquired)

	// Semaphore length = number of connections created (slots taken)
	total := len(p.semaphore)
	idle := len(p.conns)
	inUse := total - idle

	return Stats{
		Total:     total,
		Acquired:  acquired,
		Released:  released,
		Destroyed: destroyed,
		Idle:      idle,
		InUse:     inUse,
	}
}

// Len returns the number of idle connections.
func (p *Pool) Len() int {
	return len(p.conns)
}

// MaxSize returns the maximum pool size.
func (p *Pool) MaxSize() int {
	return p.maxSize
}
