package cache

import (
	"context"
	"sync"
	"time"
)

// CacheWithTTL is a cache with active TTL expiration.
// It extends the basic Cache with background cleanup.
type CacheWithTTL[K comparable, V any] struct {
	*Cache[K, V]
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	cleanupTick time.Duration
}

// TTLConfig holds configuration for TTL cache.
type TTLConfig struct {
	MaxSize     int           // Maximum entries (0 = unlimited)
	CleanupTick time.Duration // Interval for active expiration
	DefaultTTL  time.Duration // Default TTL if not specified
}

// DefaultTTLConfig returns sensible defaults.
func DefaultTTLConfig() TTLConfig {
	return TTLConfig{
		MaxSize:     0,
		CleanupTick: 1 * time.Minute,
		DefaultTTL:  5 * time.Minute,
	}
}

// NewWithTTL creates a cache with active expiration.
func NewWithTTL[K comparable, V any](config TTLConfig) *CacheWithTTL[K, V] {
	ctx, cancel := context.WithCancel(context.Background())

	c := &CacheWithTTL[K, V]{
		Cache:       New[K, V](Config{MaxSize: config.MaxSize}),
		ctx:         ctx,
		cancel:      cancel,
		cleanupTick: config.CleanupTick,
	}

	// Start background cleanup goroutine
	c.wg.Add(1)
	go c.cleanupLoop()

	return c
}

// cleanupLoop runs in the background and removes expired entries.
func (c *CacheWithTTL[K, V]) cleanupLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cleanupTick)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.removeExpired()
		}
	}
}

// removeExpired scans and removes all expired entries.
func (c *CacheWithTTL[K, V]) removeExpired() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	removed := 0
	for k, e := range c.data {
		if e.isExpired() {
			delete(c.data, k)
			removed++
		}
	}
	return removed
}

// ExpiredCount returns the number of expired entries (for monitoring).
func (c *CacheWithTTL[K, V]) ExpiredCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	count := 0
	for _, e := range c.data {
		if e.isExpired() {
			count++
		}
	}
	return count
}

// Close stops the background cleanup goroutine.
// Must be called to prevent goroutine leaks!
func (c *CacheWithTTL[K, V]) Close() {
	c.cancel()
	c.wg.Wait()
}
