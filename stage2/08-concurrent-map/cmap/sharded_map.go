package cmap

import (
	"hash/fnv"
	"sync"
)

// DefaultShardCount is the default number of shards.
// Should be a power of 2 for efficient modulo via bitwise AND.
const DefaultShardCount = 32

// shard represents a single partition of the map.
type shard[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

// ShardedMap distributes keys across multiple shards to reduce lock contention.
//
// How it works:
//
//	Key "user:123" ──► hash(key) ──► shard index ──► [Lock + Map]
//
//	                    ┌─────────────────────────────────────┐
//	                    │           ShardedMap                │
//	                    │  ┌────┐ ┌────┐ ┌────┐     ┌────┐   │
//	   key ──► hash ───►│  │ 0  │ │ 1  │ │ 2  │ ... │ N-1│   │
//	                    │  └────┘ └────┘ └────┘     └────┘   │
//	                    └─────────────────────────────────────┘
//
// Best for: High-contention scenarios with many concurrent read/writes
// Trade-off: More memory, Range() must lock all shards
type ShardedMap[K comparable, V any] struct {
	shards    []*shard[K, V]
	shardMask uint64 // shardCount - 1 for fast modulo
	hasher    func(K) uint64
}

// NewShardedMap creates a new ShardedMap with default shard count.
func NewShardedMap[K comparable, V any]() *ShardedMap[K, V] {
	return NewShardedMapWithCount[K, V](DefaultShardCount)
}

// NewShardedMapWithCount creates a new ShardedMap with specified shard count.
// Count should be a power of 2; if not, it's rounded up.
func NewShardedMapWithCount[K comparable, V any](count int) *ShardedMap[K, V] {
	// Round up to next power of 2
	count = nextPowerOf2(count)

	shards := make([]*shard[K, V], count)
	for i := range shards {
		shards[i] = &shard[K, V]{
			data: make(map[K]V),
		}
	}

	return &ShardedMap[K, V]{
		shards:    shards,
		shardMask: uint64(count - 1),
		hasher:    defaultHasher[K],
	}
}

// nextPowerOf2 returns the next power of 2 >= n.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

// defaultHasher uses FNV-1a hash for any comparable type.
func defaultHasher[K comparable](key K) uint64 {
	h := fnv.New64a()
	// Convert to string representation for hashing
	// This is a simplification; in production, use type-specific hashing
	switch k := any(key).(type) {
	case string:
		h.Write([]byte(k))
	case int:
		h.Write([]byte{
			byte(k), byte(k >> 8), byte(k >> 16), byte(k >> 24),
			byte(k >> 32), byte(k >> 40), byte(k >> 48), byte(k >> 56),
		})
	case int64:
		h.Write([]byte{
			byte(k), byte(k >> 8), byte(k >> 16), byte(k >> 24),
			byte(k >> 32), byte(k >> 40), byte(k >> 48), byte(k >> 56),
		})
	case int32:
		h.Write([]byte{byte(k), byte(k >> 8), byte(k >> 16), byte(k >> 24)})
	default:
		// Fallback: use fmt.Sprintf (slower but always works)
		h.Write([]byte{byte(0)}) // placeholder
	}
	return h.Sum64()
}

// getShard returns the shard for a given key.
func (sm *ShardedMap[K, V]) getShard(key K) *shard[K, V] {
	hash := sm.hasher(key)
	idx := hash & sm.shardMask
	return sm.shards[idx]
}

// Get retrieves a value by key.
func (sm *ShardedMap[K, V]) Get(key K) (V, bool) {
	s := sm.getShard(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Set stores a value.
func (sm *ShardedMap[K, V]) Set(key K, value V) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Delete removes a key.
func (sm *ShardedMap[K, V]) Delete(key K) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// GetOrSet returns existing value or sets new one atomically.
func (sm *ShardedMap[K, V]) GetOrSet(key K, value V) (V, bool) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.data[key]; ok {
		return v, true
	}
	s.data[key] = value
	return value, false
}

// Len returns total count across all shards.
func (sm *ShardedMap[K, V]) Len() int {
	total := 0
	for _, s := range sm.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}

// Range iterates over all items.
// Note: This locks shards sequentially, not atomically.
func (sm *ShardedMap[K, V]) Range(fn func(key K, value V) bool) {
	for _, s := range sm.shards {
		s.mu.RLock()
		for k, v := range s.data {
			if !fn(k, v) {
				s.mu.RUnlock()
				return
			}
		}
		s.mu.RUnlock()
	}
}

// ShardStats returns the count of items in each shard (for debugging).
func (sm *ShardedMap[K, V]) ShardStats() []int {
	stats := make([]int, len(sm.shards))
	for i, s := range sm.shards {
		s.mu.RLock()
		stats[i] = len(s.data)
		s.mu.RUnlock()
	}
	return stats
}

// SetWithHasher allows setting a custom hash function.
func (sm *ShardedMap[K, V]) SetWithHasher(hasher func(K) uint64) {
	sm.hasher = hasher
}
