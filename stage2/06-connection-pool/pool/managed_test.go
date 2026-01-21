package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagedPool_AcquireRelease(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{MaxSize: 3})
	defer p.Close()

	conn, err := p.Acquire(context.Background())
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}

	err = conn.Release()
	if err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	if p.Len() != 1 {
		t.Errorf("Expected 1 idle, got %d", p.Len())
	}
}

func TestManagedPool_DoubleRelease(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{MaxSize: 3})
	defer p.Close()

	conn, _ := p.Acquire(context.Background())

	// First release
	conn.Release()

	// Second release - should be ignored
	conn.Release()

	// Should still only have 1 connection
	if p.Len() != 1 {
		t.Errorf("Expected 1 idle (double release ignored), got %d", p.Len())
	}
}

func TestManagedPool_IdleTimeout(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MaxSize:             3,
		MaxIdleTime:         30 * time.Millisecond,
		HealthCheckInterval: 20 * time.Millisecond,
	})
	defer p.Close()

	// Acquire and release a connection
	conn, _ := p.Acquire(context.Background())
	conn.Release()

	// Wait for idle timeout + health check
	time.Sleep(100 * time.Millisecond)

	// Connection should be cleaned up
	if p.Len() != 0 {
		t.Errorf("Expected 0 idle (cleaned up), got %d", p.Len())
	}

	stats := p.Stats()
	if stats.Destroyed == 0 {
		t.Error("Expected at least 1 destroyed")
	}
}

func TestManagedPool_MaxLifetime(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MaxSize:             3,
		MaxLifetime:         50 * time.Millisecond,
		HealthCheckInterval: 20 * time.Millisecond,
	})
	defer p.Close()

	// Acquire and release a connection
	conn, _ := p.Acquire(context.Background())
	conn.Release()

	// Wait for lifetime to expire
	time.Sleep(100 * time.Millisecond)

	// Connection should be cleaned up
	if p.Len() != 0 {
		t.Errorf("Expected 0 idle (lifetime expired), got %d", p.Len())
	}
}

func TestManagedPool_ConnectionReuse(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{MaxSize: 1})
	defer p.Close()

	// Acquire, release, acquire again
	conn1, _ := p.Acquire(context.Background())
	id1 := conn1.Conn.(*mockConn).id
	conn1.Release()

	conn2, _ := p.Acquire(context.Background())
	id2 := conn2.Conn.(*mockConn).id

	if id1 != id2 {
		t.Errorf("Expected same connection (id %d), got different (id %d)", id1, id2)
	}

	// UseCount should be 2
	if conn2.UseCount() != 2 {
		t.Errorf("Expected use count 2, got %d", conn2.UseCount())
	}

	conn2.Release()
}

func TestManagedPool_InvalidConnectionOnAcquire(t *testing.T) {
	var connCount int32
	factory := func(ctx context.Context) (Conn, error) {
		id := atomic.AddInt32(&connCount, 1)
		return &mockConn{id: int(id), valid: true}, nil
	}

	p := NewManaged(factory, ManagedConfig{MaxSize: 3})
	defer p.Close()

	conn, _ := p.Acquire(context.Background())

	// Mark as invalid
	mc := conn.Conn.(*mockConn)
	mc.mu.Lock()
	mc.valid = false
	mc.mu.Unlock()

	conn.Release()

	// Next acquire should get a NEW connection
	conn2, _ := p.Acquire(context.Background())
	id2 := conn2.Conn.(*mockConn).id

	// Should be a new connection (id 2, not 1)
	if id2 != 2 {
		t.Errorf("Expected new connection (id 2), got id %d", id2)
	}

	conn2.Release()
}

func TestManagedPool_ConcurrentAccess(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{MaxSize: 5})
	defer p.Close()

	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				conn, err := p.Acquire(context.Background())
				if err != nil {
					continue
				}
				time.Sleep(time.Millisecond)
				conn.Release()
			}
		}()
	}

	wg.Wait()

	stats := p.Stats()
	t.Logf("Stats: total=%d, acquired=%d, released=%d, destroyed=%d",
		stats.Total, stats.Acquired, stats.Released, stats.Destroyed)

	if stats.Total > 5 {
		t.Errorf("Exceeded max size: %d", stats.Total)
	}
}

func TestManagedPool_Close(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MaxSize:             3,
		HealthCheckInterval: 10 * time.Millisecond,
	})

	conn, _ := p.Acquire(context.Background())
	conn.Release()

	err := p.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Acquire should fail
	_, err = p.Acquire(context.Background())
	if err != ErrPoolClosed {
		t.Errorf("Expected ErrPoolClosed, got %v", err)
	}
}

func TestPooledConn_Metadata(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{MaxSize: 1})
	defer p.Close()

	conn, _ := p.Acquire(context.Background())

	// Age should be very small
	if conn.Age() > time.Second {
		t.Errorf("Age too large: %v", conn.Age())
	}

	// Release and wait
	conn.Release()
	time.Sleep(30 * time.Millisecond)

	// Acquire again - should see idle time
	conn2, _ := p.Acquire(context.Background())

	// IdleTime measures since lastUsedAt (set when released)
	// But after acquire, the connection is in use
	// Check useCount instead
	if conn2.UseCount() != 2 {
		t.Errorf("Expected use count 2, got %d", conn2.UseCount())
	}

	// Age should still be small (same connection)
	if conn2.Age() < 20*time.Millisecond {
		t.Errorf("Age too small: %v", conn2.Age())
	}

	conn2.Release()
}

func TestManagedPool_WarmUp(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MinSize: 3,
		MaxSize: 5,
	})
	defer p.Close()

	// Warm up
	created, err := p.WarmUp(context.Background())
	if err != nil {
		t.Fatalf("WarmUp failed: %v", err)
	}

	if created != 3 {
		t.Errorf("Expected 3 connections created, got %d", created)
	}

	// Should have 3 idle connections
	if p.Len() != 3 {
		t.Errorf("Expected 3 idle, got %d", p.Len())
	}

	// Stats should show warm-up count
	stats := p.Stats()
	if stats.WarmedUp != 3 {
		t.Errorf("Expected WarmedUp=3, got %d", stats.WarmedUp)
	}

	// Acquire should be instant (no creation needed)
	conn, _ := p.Acquire(context.Background())
	conn.Release()
}

func TestManagedPool_WarmUpCancellation(t *testing.T) {
	// Factory that's slow
	factory := func(ctx context.Context) (Conn, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return &mockConn{id: 1, valid: true}, nil
		}
	}

	p := NewManaged(factory, ManagedConfig{
		MinSize: 10,
		MaxSize: 10,
	})
	defer p.Close()

	// Cancel quickly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := p.WarmUp(ctx)
	if err == nil {
		t.Error("Expected cancellation error")
	}
}

func TestManagedPool_EnsureMinConnections(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MinSize: 2,
		MaxSize: 5,
	})
	defer p.Close()

	// Initially no connections
	if p.Len() != 0 {
		t.Errorf("Expected 0 idle initially, got %d", p.Len())
	}

	// Ensure min connections
	created, err := p.EnsureMinConnections(context.Background())
	if err != nil {
		t.Fatalf("EnsureMinConnections failed: %v", err)
	}

	if created != 2 {
		t.Errorf("Expected 2 created, got %d", created)
	}

	// Call again - should create nothing
	created, _ = p.EnsureMinConnections(context.Background())
	if created != 0 {
		t.Errorf("Expected 0 created (already at min), got %d", created)
	}
}

func TestManagedPool_MinMaxConfig(t *testing.T) {
	p := NewManaged(mockFactory(), ManagedConfig{
		MinSize: 2,
		MaxSize: 5,
	})
	defer p.Close()

	if p.MinSize() != 2 {
		t.Errorf("Expected MinSize=2, got %d", p.MinSize())
	}

	if p.MaxSize() != 5 {
		t.Errorf("Expected MaxSize=5, got %d", p.MaxSize())
	}

	stats := p.Stats()
	if stats.MinSize != 2 || stats.MaxSize != 5 {
		t.Errorf("Stats don't show correct min/max: %+v", stats)
	}
}
