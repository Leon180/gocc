package cmap

import (
	"sync"
	"testing"
	"time"
)

func TestTTLMap_BasicOperations(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour) // Long cleanup interval
	defer m.Close()

	// Set and Get
	m.Set("key1", 100, time.Hour)
	v, ok := m.Get("key1")
	if !ok || v != 100 {
		t.Errorf("Get(key1) = %d, %v; want 100, true", v, ok)
	}

	// Get non-existent
	_, ok = m.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}

	// Delete
	m.Delete("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Error("Get(key1) after delete should return false")
	}
}

func TestTTLMap_Expiration(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.Set("key", 100, 50*time.Millisecond)

	// Should exist immediately
	v, ok := m.Get("key")
	if !ok || v != 100 {
		t.Errorf("Get(key) before expiry = %d, %v; want 100, true", v, ok)
	}

	// Wait for expiration
	time.Sleep(60 * time.Millisecond)

	// Should be expired
	_, ok = m.Get("key")
	if ok {
		t.Error("Get(key) after expiry should return false")
	}
}

func TestTTLMap_Touch(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.Set("key", 100, 50*time.Millisecond)

	// Touch should extend TTL
	time.Sleep(30 * time.Millisecond)
	if !m.Touch("key", 100*time.Millisecond) {
		t.Error("Touch should return true for existing key")
	}

	// Should still exist after original expiry
	time.Sleep(30 * time.Millisecond)
	v, ok := m.Get("key")
	if !ok || v != 100 {
		t.Errorf("Get(key) after touch = %d, %v; want 100, true", v, ok)
	}

	// Touch on non-existent
	if m.Touch("nonexistent", time.Hour) {
		t.Error("Touch should return false for non-existent key")
	}
}

func TestTTLMap_GetOrSet(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	// First call should set
	v, loaded := m.GetOrSet("key", 100, time.Hour)
	if loaded || v != 100 {
		t.Errorf("GetOrSet(key, 100) = %d, %v; want 100, false", v, loaded)
	}

	// Second call should get existing
	v, loaded = m.GetOrSet("key", 200, time.Hour)
	if !loaded || v != 100 {
		t.Errorf("GetOrSet(key, 200) = %d, %v; want 100, true", v, loaded)
	}
}

func TestTTLMap_GetOrSet_Expired(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.Set("key", 100, 50*time.Millisecond)
	time.Sleep(60 * time.Millisecond)

	// After expiry, GetOrSet should set new value
	v, loaded := m.GetOrSet("key", 200, time.Hour)
	if loaded || v != 200 {
		t.Errorf("GetOrSet after expiry = %d, %v; want 200, false", v, loaded)
	}
}

func TestTTLMap_ActiveLen(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.Set("a", 1, time.Hour)
	m.Set("b", 2, 50*time.Millisecond)
	m.Set("c", 3, time.Hour)

	if m.ActiveLen() != 3 {
		t.Errorf("ActiveLen() = %d; want 3", m.ActiveLen())
	}

	time.Sleep(60 * time.Millisecond)

	if m.ActiveLen() != 2 {
		t.Errorf("ActiveLen() after expiry = %d; want 2", m.ActiveLen())
	}
}

func TestTTLMap_BackgroundCleanup(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, 50*time.Millisecond)
	defer m.Close()

	m.Set("a", 1, 30*time.Millisecond)
	m.Set("b", 2, 30*time.Millisecond)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Entries should be cleaned up
	if m.Len() != 0 {
		t.Errorf("After cleanup, Len() = %d; want 0", m.Len())
	}
}

func TestTTLMap_Range(t *testing.T) {
	m := NewTTLMapWithOptions[int, int](8, time.Hour)
	defer m.Close()

	m.Set(1, 10, time.Hour)
	m.Set(2, 20, 1*time.Millisecond) // Will expire
	m.Set(3, 30, time.Hour)

	time.Sleep(10 * time.Millisecond)

	sum := 0
	m.Range(func(k, v int) bool {
		sum += v
		return true
	})

	// Should only sum non-expired entries (10 + 30)
	if sum != 40 {
		t.Errorf("Range sum = %d; want 40", sum)
	}
}

func TestTTLMap_Stats(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.Set("a", 1, time.Hour)
	m.Set("b", 2, 1*time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	stats := m.Stats()
	if stats.TotalEntries != 2 {
		t.Errorf("Stats.TotalEntries = %d; want 2", stats.TotalEntries)
	}
	if stats.ActiveEntries != 1 {
		t.Errorf("Stats.ActiveEntries = %d; want 1", stats.ActiveEntries)
	}
	if stats.ExpiredEntries != 1 {
		t.Errorf("Stats.ExpiredEntries = %d; want 1", stats.ExpiredEntries)
	}
}

func TestTTLMap_Concurrent(t *testing.T) {
	m := NewTTLMapWithOptions[int, int](32, time.Minute)
	defer m.Close()

	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m.Set(n, n*2, time.Minute)
		}(i)
	}
	wg.Wait()

	// Concurrent reads and writes
	for i := 0; i < 500; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			m.Get(n % 100)
		}(i)
		go func(n int) {
			defer wg.Done()
			m.Set(n%100, n, 100*time.Millisecond)
		}(i)
	}
	wg.Wait()
}

func TestTTLMap_SetNoExpire(t *testing.T) {
	m := NewTTLMapWithOptions[string, int](8, time.Hour)
	defer m.Close()

	m.SetNoExpire("key", 100)

	v, ok := m.Get("key")
	if !ok || v != 100 {
		t.Errorf("Get(key) = %d, %v; want 100, true", v, ok)
	}

	// Should still exist after some time (effectively no expiry)
	time.Sleep(50 * time.Millisecond)
	v, ok = m.Get("key")
	if !ok || v != 100 {
		t.Errorf("Get(key) after delay = %d, %v; want 100, true", v, ok)
	}
}
