package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockConn is a test connection.
type mockConn struct {
	id     int
	valid  bool
	closed bool
	mu     sync.Mutex
}

func (c *mockConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *mockConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.valid && !c.closed
}

func (c *mockConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// mockFactory creates mock connections.
func mockFactory() Factory {
	var counter int32
	return func(ctx context.Context) (Conn, error) {
		id := atomic.AddInt32(&counter, 1)
		return &mockConn{id: int(id), valid: true}, nil
	}
}

func TestPool_AcquireRelease(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 3})
	defer p.Close()

	// Acquire a connection
	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	// Release it back
	err = p.Release(conn)
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	// Should be 1 idle connection now
	if p.Len() != 1 {
		t.Errorf("Expected 1 idle, got %d", p.Len())
	}
}

func TestPool_MaxSize(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 2})
	defer p.Close()

	// Acquire 2 connections (max)
	conn1, _ := p.Acquire(context.Background())
	conn2, _ := p.Acquire(context.Background())

	// Third should block (use timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := p.Acquire(ctx)
	if err != ErrAcquireTimeout {
		t.Errorf("Expected timeout, got %v", err)
	}

	// Release one
	p.Release(conn1)

	// Now should succeed
	conn3, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Should acquire after release: %v", err)
	}

	p.Release(conn2)
	p.Release(conn3)
}

func TestPool_AcquireWithTimeout(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 1})
	defer p.Close()

	// Acquire the only connection
	conn, _ := p.Acquire(context.Background())

	// Timeout on second acquire
	_, err := p.AcquireWithTimeout(50 * time.Millisecond)
	if err != ErrAcquireTimeout {
		t.Errorf("Expected timeout, got %v", err)
	}

	p.Release(conn)
}

func TestPool_InvalidConnection(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 3})
	defer p.Close()

	conn, _ := p.Acquire(context.Background())

	// Mark as invalid
	mc := conn.(*mockConn)
	mc.mu.Lock()
	mc.valid = false
	mc.mu.Unlock()

	// Release should destroy it
	err := p.Release(conn)
	if err != ErrInvalidConn {
		t.Errorf("Expected ErrInvalidConn, got %v", err)
	}

	// Pool should be empty
	if p.Len() != 0 {
		t.Errorf("Expected 0 idle, got %d", p.Len())
	}
}

func TestPool_Close(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 3})

	// Create some connections
	conn1, _ := p.Acquire(context.Background())
	conn2, _ := p.Acquire(context.Background())
	p.Release(conn1)
	p.Release(conn2)

	// Close pool
	err := p.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Acquire should fail
	_, err = p.Acquire(context.Background())
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}

	// Double close should fail
	err = p.Close()
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed on double close, got %v", err)
	}
}

func TestPool_Stats(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 3})
	defer p.Close()

	// Acquire 2
	conn1, _ := p.Acquire(context.Background())
	conn2, _ := p.Acquire(context.Background())

	stats := p.Stats()
	if stats.Total != 2 {
		t.Errorf("Expected 2 total, got %d", stats.Total)
	}
	if stats.Acquired != 2 {
		t.Errorf("Expected 2 acquired, got %d", stats.Acquired)
	}
	if stats.InUse != 2 {
		t.Errorf("Expected 2 in use, got %d", stats.InUse)
	}

	// Release 1
	p.Release(conn1)

	stats = p.Stats()
	if stats.Released != 1 {
		t.Errorf("Expected 1 released, got %d", stats.Released)
	}
	if stats.Idle != 1 {
		t.Errorf("Expected 1 idle, got %d", stats.Idle)
	}
	if stats.InUse != 1 {
		t.Errorf("Expected 1 in use, got %d", stats.InUse)
	}

	p.Release(conn2)
}

func TestPool_ConcurrentAccess(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 5})
	defer p.Close()

	var wg sync.WaitGroup

	// 20 goroutines acquiring and releasing
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				conn, err := p.AcquireWithTimeout(100 * time.Millisecond)
				if err != nil {
					continue
				}
				time.Sleep(time.Millisecond) // Simulate work
				p.Release(conn)
			}
		}()
	}

	wg.Wait()

	stats := p.Stats()
	t.Logf("Stats: total=%d, acquired=%d, released=%d, destroyed=%d",
		stats.Total, stats.Acquired, stats.Released, stats.Destroyed)

	// Should have at most maxSize connections
	if stats.Total > 5 {
		t.Errorf("Created more than max: %d", stats.Total)
	}
}

func TestPool_ReuseConnection(t *testing.T) {
	p := New(mockFactory(), Config{MaxSize: 1})
	defer p.Close()

	// Acquire, release, acquire again - should be same connection
	conn1, _ := p.Acquire(context.Background())
	mc1 := conn1.(*mockConn)
	id1 := mc1.id

	p.Release(conn1)

	conn2, _ := p.Acquire(context.Background())
	mc2 := conn2.(*mockConn)
	id2 := mc2.id

	if id1 != id2 {
		t.Errorf("Expected same connection (id %d), got different (id %d)", id1, id2)
	}

	p.Release(conn2)
}

// Benchmark acquire/release
func BenchmarkPool_AcquireRelease(b *testing.B) {
	p := New(mockFactory(), Config{MaxSize: 10})
	defer p.Close()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := p.Acquire(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			p.Release(conn)
		}
	})
}
