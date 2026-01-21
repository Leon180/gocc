package pool

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// DBConn wraps a database connection for pooling.
type DBConn struct {
	db        *sql.DB
	conn      *sql.Conn
	createdAt time.Time
	mu        sync.Mutex
	closed    bool
}

// Close closes the database connection.
func (c *DBConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	return c.conn.Close()
}

// IsValid checks if the connection is still usable.
func (c *DBConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}

	// Ping to check connection health
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return c.conn.PingContext(ctx) == nil
}

// Conn returns the underlying sql.Conn for executing queries.
func (c *DBConn) Conn() *sql.Conn {
	return c.conn
}

// Query executes a query and returns rows.
func (c *DBConn) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.conn.QueryContext(ctx, query, args...)
}

// Exec executes a statement.
func (c *DBConn) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.conn.ExecContext(ctx, query, args...)
}

// DBPool is a connection pool for database connections.
type DBPool struct {
	*ManagedPool
	db *sql.DB
}

// DBPoolConfig holds configuration for DBPool.
type DBPoolConfig struct {
	DSN                 string        // Database connection string
	MinSize             int           // Minimum connections
	MaxSize             int           // Maximum connections
	MaxIdleTime         time.Duration // Max idle time
	MaxLifetime         time.Duration // Max connection lifetime
	HealthCheckInterval time.Duration // Health check interval
}

// DefaultDBPoolConfig returns sensible defaults.
func DefaultDBPoolConfig(dsn string) DBPoolConfig {
	return DBPoolConfig{
		DSN:                 dsn,
		MinSize:             2,
		MaxSize:             10,
		MaxIdleTime:         5 * time.Minute,
		MaxLifetime:         30 * time.Minute,
		HealthCheckInterval: 1 * time.Minute,
	}
}

// NewDBPool creates a new database connection pool.
// Note: This requires a database driver to be imported (e.g., _ "github.com/lib/pq")
func NewDBPool(config DBPoolConfig) (*DBPool, error) {
	db, err := sql.Open("postgres", config.DSN) // Change driver as needed
	if err != nil {
		return nil, err
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	factory := func(ctx context.Context) (Conn, error) {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		return &DBConn{
			db:        db,
			conn:      conn,
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

	return &DBPool{
		ManagedPool: pool,
		db:          db,
	}, nil
}

// Acquire gets a database connection from the pool.
func (p *DBPool) Acquire(ctx context.Context) (*DBConn, error) {
	conn, err := p.ManagedPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Conn.(*DBConn), nil
}

// Close closes the pool and the underlying database connection.
func (p *DBPool) Close() error {
	if err := p.ManagedPool.Close(); err != nil && !errors.Is(err, ErrPoolClosed) {
		return err
	}
	return p.db.Close()
}

// WithConn executes a function with a pooled connection.
// The connection is automatically released after the function returns.
func (p *DBPool) WithConn(ctx context.Context, fn func(*DBConn) error) error {
	conn, err := p.ManagedPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return fn(conn.Conn.(*DBConn))
}
