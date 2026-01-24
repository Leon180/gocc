package cmap

import (
	"sync"
	"time"
)

// TTLEntry represents a value with an expiration time.
type TTLEntry[V any] struct {
	Value     V
	ExpiresAt time.Time
}

// IsExpired checks if the entry has expired.
func (e TTLEntry[V]) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

// TTLMap is a sharded map with TTL support.
//
// How it works:
//
//	Set(key, value, ttl=5s) ──► store {value, expiresAt: now+5s}
//	                           │
//	Get(key) ───────────────►  check expiresAt ──► expired? ──► delete & return nil
//	                                  │
//	                                  └──► not expired ──► return value
//
// Background cleanup runs periodically to remove expired entries.
type TTLMap[K comparable, V any] struct {
	shards    []*ttlShard[K, V]
	shardMask uint64
	hasher    func(K) uint64

	cleanupInterval time.Duration
	stopCleanup     chan struct{}
	wg              sync.WaitGroup
}

type ttlShard[K comparable, V any] struct {
	data map[K]TTLEntry[V]
	mu   sync.RWMutex
}

// NewTTLMap creates a new TTLMap with default settings.
func NewTTLMap[K comparable, V any]() *TTLMap[K, V] {
	return NewTTLMapWithOptions[K, V](DefaultShardCount, time.Minute)
}

// NewTTLMapWithOptions creates a TTLMap with custom shard count and cleanup interval.
func NewTTLMapWithOptions[K comparable, V any](shardCount int, cleanupInterval time.Duration) *TTLMap[K, V] {
	shardCount = nextPowerOf2(shardCount)

	shards := make([]*ttlShard[K, V], shardCount)
	for i := range shards {
		shards[i] = &ttlShard[K, V]{
			data: make(map[K]TTLEntry[V]),
		}
	}

	m := &TTLMap[K, V]{
		shards:          shards,
		shardMask:       uint64(shardCount - 1),
		hasher:          defaultHasher[K],
		cleanupInterval: cleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	// Start background cleanup
	m.wg.Add(1)
	go m.cleanupLoop()

	return m
}

// getShard returns the shard for a given key.
func (m *TTLMap[K, V]) getShard(key K) *ttlShard[K, V] {
	hash := m.hasher(key)
	idx := hash & m.shardMask
	return m.shards[idx]
}

// Get retrieves a value by key, returning false if expired or not found.
func (m *TTLMap[K, V]) Get(key K) (V, bool) {
	s := m.getShard(key)
	s.mu.RLock()
	entry, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		var zero V
		return zero, false
	}

	if entry.IsExpired() {
		// Lazy deletion
		m.Delete(key)
		var zero V
		return zero, false
	}

	return entry.Value, true
}

// Set stores a value with a TTL.
func (m *TTLMap[K, V]) Set(key K, value V, ttl time.Duration) {
	s := m.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = TTLEntry[V]{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
}

// SetNoExpire stores a value with no expiration (max time).
func (m *TTLMap[K, V]) SetNoExpire(key K, value V) {
	m.Set(key, value, time.Duration(1<<63-1)) // Max duration
}

// Delete removes a key.
func (m *TTLMap[K, V]) Delete(key K) {
	s := m.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Touch resets the TTL for an existing key.
func (m *TTLMap[K, V]) Touch(key K, ttl time.Duration) bool {
	s := m.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.data[key]
	if !ok || entry.IsExpired() {
		return false
	}

	entry.ExpiresAt = time.Now().Add(ttl)
	s.data[key] = entry
	return true
}

// GetOrSet returns existing value or sets new one with TTL.
func (m *TTLMap[K, V]) GetOrSet(key K, value V, ttl time.Duration) (V, bool) {
	s := m.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.data[key]; ok && !entry.IsExpired() {
		return entry.Value, true
	}

	s.data[key] = TTLEntry[V]{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	}
	return value, false
}

// Len returns total count (including expired but not yet cleaned).
func (m *TTLMap[K, V]) Len() int {
	total := 0
	for _, s := range m.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}

// ActiveLen returns count of non-expired entries.
func (m *TTLMap[K, V]) ActiveLen() int {
	total := 0
	now := time.Now()
	for _, s := range m.shards {
		s.mu.RLock()
		for _, entry := range s.data {
			if now.Before(entry.ExpiresAt) {
				total++
			}
		}
		s.mu.RUnlock()
	}
	return total
}

// Range iterates over non-expired items.
func (m *TTLMap[K, V]) Range(fn func(key K, value V) bool) {
	now := time.Now()
	for _, s := range m.shards {
		s.mu.RLock()
		for k, entry := range s.data {
			if now.Before(entry.ExpiresAt) {
				if !fn(k, entry.Value) {
					s.mu.RUnlock()
					return
				}
			}
		}
		s.mu.RUnlock()
	}
}

// cleanupLoop periodically removes expired entries.
func (m *TTLMap[K, V]) cleanupLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanup()
		case <-m.stopCleanup:
			return
		}
	}
}

// cleanup removes all expired entries.
func (m *TTLMap[K, V]) cleanup() {
	now := time.Now()
	for _, s := range m.shards {
		s.mu.Lock()
		for k, entry := range s.data {
			if now.After(entry.ExpiresAt) {
				delete(s.data, k)
			}
		}
		s.mu.Unlock()
	}
}

// Close stops the background cleanup goroutine.
func (m *TTLMap[K, V]) Close() {
	close(m.stopCleanup)
	m.wg.Wait()
}

// Stats returns statistics about the map.
func (m *TTLMap[K, V]) Stats() TTLMapStats {
	stats := TTLMapStats{
		ShardCount: len(m.shards),
	}

	now := time.Now()
	for _, s := range m.shards {
		s.mu.RLock()
		for _, entry := range s.data {
			stats.TotalEntries++
			if now.After(entry.ExpiresAt) {
				stats.ExpiredEntries++
			}
		}
		s.mu.RUnlock()
	}

	stats.ActiveEntries = stats.TotalEntries - stats.ExpiredEntries
	return stats
}

// TTLMapStats holds statistics about the TTLMap.
type TTLMapStats struct {
	ShardCount     int
	TotalEntries   int
	ActiveEntries  int
	ExpiredEntries int
}
