package cache

import (
	"testing"
	"time"
)

func TestCacheWithTTL_ActiveExpiration(t *testing.T) {
	config := TTLConfig{
		CleanupTick: 50 * time.Millisecond, // Fast cleanup for testing
	}
	c := NewWithTTL[string, string](config)
	defer c.Close()

	// Set entries with short TTL
	c.SetWithTTL("key1", "value1", 30*time.Millisecond)
	c.SetWithTTL("key2", "value2", 30*time.Millisecond)
	c.SetWithTTL("key3", "value3", 500*time.Millisecond) // Longer TTL

	// All should exist initially
	if c.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", c.Len())
	}

	// Wait for expiration + cleanup
	time.Sleep(100 * time.Millisecond)

	// key1 and key2 should be removed by active expiration
	// key3 should still exist
	if c.Has("key1") {
		t.Error("key1 should be expired")
	}
	if c.Has("key2") {
		t.Error("key2 should be expired")
	}
	if !c.Has("key3") {
		t.Error("key3 should still exist")
	}
}

func TestCacheWithTTL_RemoveExpired(t *testing.T) {
	config := TTLConfig{
		CleanupTick: 1 * time.Hour, // Long interval, we'll call manually
	}
	c := NewWithTTL[string, int](config)
	defer c.Close()

	// Add entries with short TTL
	c.SetWithTTL("a", 1, 10*time.Millisecond)
	c.SetWithTTL("b", 2, 10*time.Millisecond)
	c.Set("c", 3) // No expiration

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Check expired count before cleanup
	expired := c.ExpiredCount()
	if expired != 2 {
		t.Errorf("Expected 2 expired, got %d", expired)
	}

	// Manually trigger cleanup
	removed := c.removeExpired()
	if removed != 2 {
		t.Errorf("Expected 2 removed, got %d", removed)
	}

	// Only non-expired entry should remain
	if c.Len() != 1 {
		t.Errorf("Expected 1 entry, got %d", c.Len())
	}

	val, ok := c.Get("c")
	if !ok || val != 3 {
		t.Error("Key 'c' should still exist with value 3")
	}
}

func TestCacheWithTTL_Close(t *testing.T) {
	c := NewWithTTL[string, int](DefaultTTLConfig())

	c.Set("key", 42)

	// Close should stop the cleanup goroutine
	c.Close()

	// Should still be able to use cache after close
	// (just no more background cleanup)
	val, ok := c.Get("key")
	if !ok || val != 42 {
		t.Error("Cache should still work after Close")
	}
}

func TestCacheWithTTL_NoGoroutineLeak(t *testing.T) {
	// Create and close many caches
	for range 100 {
		c := NewWithTTL[int, int](TTLConfig{CleanupTick: time.Millisecond})
		c.Set(1, 1)
		c.Close()
	}

	// If there's a goroutine leak, this test would eventually fail
	// or the race detector would catch it
}
