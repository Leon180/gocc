package cache

import (
	"sync"
	"time"
)

// entry represents a single cache entry with metadata.
type entry[V any] struct {
	value       V
	expiration  time.Time // Zero means no expiration
	createdAt   time.Time
	accessedAt  time.Time
	accessCount int64
}

// isExpired checks if the entry has expired.
func (e *entry[V]) isExpired() bool {
	if e.expiration.IsZero() {
		return false
	}
	return time.Now().After(e.expiration)
}

// Cache is a thread-safe in-memory cache with generics support.
//
// Thread Safety:
//   - Uses RWMutex for high read concurrency
//   - Multiple readers can access simultaneously
//   - Writers have exclusive access
type Cache[K comparable, V any] struct {
	data    map[K]*entry[V]
	mu      sync.RWMutex
	maxSize int // 0 means unlimited

	// Statistics
	hits   int64
	misses int64
}

// Config holds cache configuration.
type Config struct {
	MaxSize int // Maximum number of entries (0 = unlimited)
}

// New creates a new cache with the given configuration.
func New[K comparable, V any](config Config) *Cache[K, V] {
	return &Cache[K, V]{
		data:    make(map[K]*entry[V]),
		maxSize: config.MaxSize,
	}
}

// Get retrieves a value from the cache.
// Returns the value and true if found and not expired, otherwise zero value and false.
//
// This is a read operation using RLock (multiple readers allowed).
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	e, exists := c.data[key]
	c.mu.RUnlock()

	if !exists {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		var zero V
		return zero, false
	}

	// Passive expiration: check on read
	if e.isExpired() {
		c.Delete(key) // Remove expired entry
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		var zero V
		return zero, false
	}

	// Update access stats (needs write lock)
	c.mu.Lock()
	e.accessedAt = time.Now()
	e.accessCount++
	c.hits++
	c.mu.Unlock()

	return e.value, true
}

// Set stores a value in the cache with no expiration.
func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a value with a time-to-live duration.
// If ttl is 0, the entry never expires.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var expiration time.Time
	if ttl > 0 {
		expiration = now.Add(ttl)
	}

	c.data[key] = &entry[V]{
		value:       value,
		expiration:  expiration,
		createdAt:   now,
		accessedAt:  now,
		accessCount: 0,
	}

	// Check if we need to evict (for LRU, implemented later)
	if c.maxSize > 0 && len(c.data) > c.maxSize {
		c.evictOne()
	}
}

// Delete removes a key from the cache.
func (c *Cache[K, V]) Delete(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.data[key]
	if exists {
		delete(c.data, key)
	}
	return exists
}

// Has checks if a key exists and is not expired.
func (c *Cache[K, V]) Has(key K) bool {
	c.mu.RLock()
	e, exists := c.data[key]
	c.mu.RUnlock()

	if !exists {
		return false
	}
	return !e.isExpired()
}

// Len returns the number of entries in the cache (including expired ones).
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[K]*entry[V])
}

// Keys returns all keys in the cache (including expired ones).
func (c *Cache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]K, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	return keys
}

// Stats returns cache statistics.
type Stats struct {
	Hits    int64
	Misses  int64
	Size    int
	HitRate float64
}

// Stats returns current cache statistics.
func (c *Cache[K, V]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return Stats{
		Hits:    c.hits,
		Misses:  c.misses,
		Size:    len(c.data),
		HitRate: hitRate,
	}
}

// evictOne removes one entry (placeholder for LRU implementation).
func (c *Cache[K, V]) evictOne() {
	// Simple eviction: remove first entry found
	// Will be replaced with LRU in Step 5.3
	for k := range c.data {
		delete(c.data, k)
		break
	}
}
