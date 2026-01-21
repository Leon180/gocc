package cache

import (
	"context"
	"sync"
)

// Singleflight deduplicates in-flight requests for the same key.
// This prevents cache stampede when many goroutines request the same
// missing key simultaneously.
//
// Pattern:
//
//	100 requests for "key" → only 1 actual fetch → result shared
type Singleflight[K comparable, V any] struct {
	mu       sync.Mutex
	inflight map[K]*call[V]
}

// call represents an in-flight or completed request.
type call[V any] struct {
	wg    sync.WaitGroup
	value V
	err   error
}

// NewSingleflight creates a new singleflight instance.
func NewSingleflight[K comparable, V any]() *Singleflight[K, V] {
	return &Singleflight[K, V]{
		inflight: make(map[K]*call[V]),
	}
}

// Do executes fn for the given key, deduplicating concurrent calls.
// If a call for the same key is already in progress, it waits for that
// call to complete and returns the same result.
//
// Example:
//
//	value, err := sf.Do("key", func() (string, error) {
//	    return expensiveFetch("key")
//	})
func (s *Singleflight[K, V]) Do(key K, fn func() (V, error)) (V, error) {
	s.mu.Lock()

	// Check if there's already an in-flight request
	if c, ok := s.inflight[key]; ok {
		s.mu.Unlock()
		c.wg.Wait() // Wait for the existing request
		return c.value, c.err
	}

	// Create a new call
	c := &call[V]{}
	c.wg.Add(1)
	s.inflight[key] = c
	s.mu.Unlock()

	// Execute the function
	c.value, c.err = fn()
	c.wg.Done()

	// Remove from inflight (allow future requests)
	s.mu.Lock()
	delete(s.inflight, key)
	s.mu.Unlock()

	return c.value, c.err
}

// CacheAside implements the Cache-Aside pattern.
// On cache miss, it fetches from the source and populates the cache.
type CacheAside[K comparable, V any] struct {
	cache *Cache[K, V]
	sf    *Singleflight[K, V]
	fetch func(context.Context, K) (V, error)
}

// NewCacheAside creates a cache-aside wrapper.
func NewCacheAside[K comparable, V any](
	cache *Cache[K, V],
	fetcher func(context.Context, K) (V, error),
) *CacheAside[K, V] {
	return &CacheAside[K, V]{
		cache: cache,
		sf:    NewSingleflight[K, V](),
		fetch: fetcher,
	}
}

// Get retrieves a value, fetching from source on cache miss.
// Uses singleflight to prevent cache stampede.
func (c *CacheAside[K, V]) Get(ctx context.Context, key K) (V, error) {
	// Try cache first
	if val, ok := c.cache.Get(key); ok {
		return val, nil
	}

	// Cache miss - fetch with singleflight
	val, err := c.sf.Do(key, func() (V, error) {
		// Double-check cache (another goroutine might have populated it)
		if val, ok := c.cache.Get(key); ok {
			return val, nil
		}

		// Fetch from source
		val, err := c.fetch(ctx, key)
		if err != nil {
			var zero V
			return zero, err
		}

		// Populate cache
		c.cache.Set(key, val)
		return val, nil
	})

	return val, err
}

// Set stores a value directly (write-through to cache only).
func (c *CacheAside[K, V]) Set(key K, value V) {
	c.cache.Set(key, value)
}

// Invalidate removes a key from the cache.
func (c *CacheAside[K, V]) Invalidate(key K) {
	c.cache.Delete(key)
}

// Stats returns cache statistics.
func (c *CacheAside[K, V]) Stats() Stats {
	return c.cache.Stats()
}
