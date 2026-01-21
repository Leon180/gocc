package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// PooledConn wraps a connection with metadata for lifecycle management.
type PooledConn struct {
	Conn
	pool       *ManagedPool
	createdAt  time.Time
	lastUsedAt time.Time
	useCount   int64
	released   bool // Prevents double-release
	mu         sync.Mutex
}

// Release returns the connection to the pool.
// Safe to call multiple times (duplicate calls are ignored).
func (pc *PooledConn) Release() error {
	pc.mu.Lock()
	if pc.released {
		pc.mu.Unlock()
		return nil // Already released
	}
	pc.released = true
	pc.lastUsedAt = time.Now()
	pc.mu.Unlock()

	return pc.pool.release(pc)
}

// Age returns how long since the connection was created.
func (pc *PooledConn) Age() time.Duration {
	return time.Since(pc.createdAt)
}

// IdleTime returns how long since the connection was last used.
func (pc *PooledConn) IdleTime() time.Duration {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return time.Since(pc.lastUsedAt)
}

// UseCount returns how many times this connection has been acquired.
func (pc *PooledConn) UseCount() int64 {
	return atomic.LoadInt64(&pc.useCount)
}

// ManagedPool is a connection pool with lifecycle management.
// Features:
//   - Background health checks
//   - Idle connection cleanup
//   - Connection age limits
//   - Duplicate release protection
//   - Min/Max size with warm-up
type ManagedPool struct {
	factory   Factory
	conns     chan *PooledConn
	semaphore chan struct{}
	mu        sync.Mutex
	closed    bool
	wg        sync.WaitGroup

	// Configuration
	minSize        int
	maxSize        int
	maxIdleTime    time.Duration
	maxLifetime    time.Duration
	healthCheckInt time.Duration

	// Lifecycle control
	ctx    context.Context
	cancel context.CancelFunc

	// Statistics
	acquired   int64
	released   int64
	destroyed  int64
	warmedUp   int64
	warmUpErrs int64
}

// ManagedConfig holds configuration for ManagedPool.
type ManagedConfig struct {
	MinSize             int           // Minimum connections to keep (for warm-up)
	MaxSize             int           // Maximum connections
	MaxIdleTime         time.Duration // Max idle time before cleanup (0 = no limit)
	MaxLifetime         time.Duration // Max connection age (0 = no limit)
	HealthCheckInterval time.Duration // Interval for health checks (0 = disabled)
}

// DefaultManagedConfig returns sensible defaults.
func DefaultManagedConfig() ManagedConfig {
	return ManagedConfig{
		MinSize:             0,
		MaxSize:             10,
		MaxIdleTime:         5 * time.Minute,
		MaxLifetime:         30 * time.Minute,
		HealthCheckInterval: 1 * time.Minute,
	}
}

// NewManaged creates a new managed connection pool.
func NewManaged(factory Factory, config ManagedConfig) *ManagedPool {
	if config.MaxSize <= 0 {
		config.MaxSize = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &ManagedPool{
		factory:        factory,
		conns:          make(chan *PooledConn, config.MaxSize),
		semaphore:      make(chan struct{}, config.MaxSize),
		minSize:        config.MinSize,
		maxSize:        config.MaxSize,
		maxIdleTime:    config.MaxIdleTime,
		maxLifetime:    config.MaxLifetime,
		healthCheckInt: config.HealthCheckInterval,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start background health check if configured
	if config.HealthCheckInterval > 0 {
		p.wg.Add(1)
		go p.healthCheckLoop()
	}

	return p
}

// healthCheckLoop runs periodic health checks on idle connections.
func (p *ManagedPool) healthCheckLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.healthCheckInt)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections removes connections that are too old or idle.
func (p *ManagedPool) cleanupIdleConnections() {
	// Collect all idle connections
	var toCheck []*PooledConn
	for {
		select {
		case conn := <-p.conns:
			toCheck = append(toCheck, conn)
		default:
			goto done
		}
	}
done:

	// Check each connection and put back healthy ones
	for _, conn := range toCheck {
		if p.shouldDestroy(conn) {
			p.destroyConn(conn)
		} else {
			// Put back healthy connection
			select {
			case p.conns <- conn:
			default:
				// Pool became full, destroy
				p.destroyConn(conn)
			}
		}
	}
}

// shouldDestroy checks if a connection should be destroyed.
func (p *ManagedPool) shouldDestroy(conn *PooledConn) bool {
	// Check validity
	if !conn.IsValid() {
		return true
	}

	// Check max lifetime
	if p.maxLifetime > 0 && conn.Age() > p.maxLifetime {
		return true
	}

	// Check max idle time
	if p.maxIdleTime > 0 && conn.IdleTime() > p.maxIdleTime {
		return true
	}

	return false
}

// destroyConn closes a connection and releases its semaphore slot.
func (p *ManagedPool) destroyConn(conn *PooledConn) {
	conn.Close()
	<-p.semaphore
	atomic.AddInt64(&p.destroyed, 1)
}

// Acquire gets a connection from the pool.
func (p *ManagedPool) Acquire(ctx context.Context) (*PooledConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()

	// Try to get an idle connection
	select {
	case conn := <-p.conns:
		if p.shouldDestroy(conn) {
			p.destroyConn(conn)
			return p.Acquire(ctx) // Retry
		}
		conn.mu.Lock()
		conn.released = false // Mark as in-use
		conn.mu.Unlock()
		atomic.AddInt64(&conn.useCount, 1)
		atomic.AddInt64(&p.acquired, 1)
		return conn, nil
	default:
	}

	// Try to create new connection
	select {
	case p.semaphore <- struct{}{}: // Got a slot
		rawConn, err := p.factory(ctx)
		if err != nil {
			<-p.semaphore
			return nil, err
		}
		now := time.Now()
		conn := &PooledConn{
			Conn:       rawConn,
			pool:       p,
			createdAt:  now,
			lastUsedAt: now,
			useCount:   1,
		}
		atomic.AddInt64(&p.acquired, 1)
		return conn, nil
	default:
	}

	// Wait for idle connection or timeout
	select {
	case conn := <-p.conns:
		if p.shouldDestroy(conn) {
			p.destroyConn(conn)
			return p.Acquire(ctx)
		}
		conn.mu.Lock()
		conn.released = false
		conn.mu.Unlock()
		atomic.AddInt64(&conn.useCount, 1)
		atomic.AddInt64(&p.acquired, 1)
		return conn, nil
	case <-ctx.Done():
		return nil, ErrAcquireTimeout
	}
}

// release returns a connection to the pool (called by PooledConn.Release).
func (p *ManagedPool) release(conn *PooledConn) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.destroyConn(conn)
		return ErrPoolClosed
	}
	p.mu.Unlock()

	// Check if connection should be destroyed
	if p.shouldDestroy(conn) {
		p.destroyConn(conn)
		return ErrInvalidConn
	}

	// Try to return to pool
	select {
	case p.conns <- conn:
		atomic.AddInt64(&p.released, 1)
		return nil
	default:
		// Pool full
		p.destroyConn(conn)
		return nil
	}
}

// Close shuts down the pool and all connections.
func (p *ManagedPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.closed = true
	p.mu.Unlock()

	// Stop background goroutines
	p.cancel()
	p.wg.Wait()

	// Drain and close all connections
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
	}

	return nil
}

// ManagedStats holds managed pool statistics.
type ManagedStats struct {
	MinSize    int   // Configured minimum connections
	MaxSize    int   // Configured maximum connections
	Total      int   // Current total connections
	Acquired   int64 // Total successful acquires
	Released   int64 // Total successful releases
	Destroyed  int64 // Total connections destroyed
	WarmedUp   int64 // Connections created during warm-up
	WarmUpErrs int64 // Warm-up errors
	Idle       int   // Current idle connections
	InUse      int   // Current connections in use
}

// Stats returns current pool statistics.
func (p *ManagedPool) Stats() ManagedStats {
	total := len(p.semaphore)
	idle := len(p.conns)

	return ManagedStats{
		MinSize:    p.minSize,
		MaxSize:    p.maxSize,
		Total:      total,
		Acquired:   atomic.LoadInt64(&p.acquired),
		Released:   atomic.LoadInt64(&p.released),
		Destroyed:  atomic.LoadInt64(&p.destroyed),
		WarmedUp:   atomic.LoadInt64(&p.warmedUp),
		WarmUpErrs: atomic.LoadInt64(&p.warmUpErrs),
		Idle:       idle,
		InUse:      total - idle,
	}
}

// Len returns the number of idle connections.
func (p *ManagedPool) Len() int {
	return len(p.conns)
}

// MinSize returns the configured minimum pool size.
func (p *ManagedPool) MinSize() int {
	return p.minSize
}

// MaxSize returns the configured maximum pool size.
func (p *ManagedPool) MaxSize() int {
	return p.maxSize
}

// WarmUp pre-creates connections up to the minimum size.
// This is useful for reducing latency on first requests.
// Returns the number of connections created and any error encountered.
func (p *ManagedPool) WarmUp(ctx context.Context) (int, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return 0, ErrPoolClosed
	}
	p.mu.Unlock()

	created := 0
	for i := 0; i < p.minSize; i++ {
		// Check context
		select {
		case <-ctx.Done():
			return created, ctx.Err()
		default:
		}

		// Try to get a semaphore slot
		select {
		case p.semaphore <- struct{}{}:
			// Create connection
			rawConn, err := p.factory(ctx)
			if err != nil {
				<-p.semaphore // Release slot
				atomic.AddInt64(&p.warmUpErrs, 1)
				return created, err
			}

			now := time.Now()
			conn := &PooledConn{
				Conn:       rawConn,
				pool:       p,
				createdAt:  now,
				lastUsedAt: now,
				released:   true, // It goes directly to idle pool
			}

			// Add to idle pool
			select {
			case p.conns <- conn:
				created++
				atomic.AddInt64(&p.warmedUp, 1)
			default:
				// Pool became full (shouldn't happen during warmup)
				conn.Close()
				<-p.semaphore
			}
		default:
			// Pool already at capacity
			return created, nil
		}
	}

	return created, nil
}

// EnsureMinConnections ensures at least minSize connections exist.
// This can be called periodically to maintain minimum connections.
func (p *ManagedPool) EnsureMinConnections(ctx context.Context) (int, error) {
	current := len(p.semaphore)
	if current >= p.minSize {
		return 0, nil
	}

	needed := p.minSize - current
	created := 0

	for i := 0; i < needed; i++ {
		select {
		case <-ctx.Done():
			return created, ctx.Err()
		case p.semaphore <- struct{}{}:
			rawConn, err := p.factory(ctx)
			if err != nil {
				<-p.semaphore
				atomic.AddInt64(&p.warmUpErrs, 1)
				return created, err
			}

			now := time.Now()
			conn := &PooledConn{
				Conn:       rawConn,
				pool:       p,
				createdAt:  now,
				lastUsedAt: now,
				released:   true,
			}

			select {
			case p.conns <- conn:
				created++
				atomic.AddInt64(&p.warmedUp, 1)
			default:
				conn.Close()
				<-p.semaphore
			}
		default:
			return created, nil
		}
	}

	return created, nil
}
