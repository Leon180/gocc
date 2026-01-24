package cmap

import "sync"

// SyncMap wraps sync.Map with type-safe generics.
//
// sync.Map is optimized for two specific use cases:
// 1. Entry written once, read many times (like a cache)
// 2. Multiple goroutines read/write disjoint key sets
//
// Internals:
//   - read map: lock-free reads for stable keys
//   - dirty map: stores new/modified keys
//   - promotion: dirty becomes read after misses
//
// Best for: Caches, append-mostly maps
// Drawback: Range is not atomic, type assertions overhead
//
// Workflow:
//
//	Get "foo" ──► read map ──► found? ──► return
//	                  │
//	                  ▼ (miss)
//	              dirty map ──► found? ──► return
type SyncMap[K comparable, V any] struct {
	m sync.Map
}

// NewSyncMap creates a new SyncMap.
func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{}
}

// Get retrieves a value by key.
func (sm *SyncMap[K, V]) Get(key K) (V, bool) {
	val, ok := sm.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

// Set stores a value.
func (sm *SyncMap[K, V]) Set(key K, value V) {
	sm.m.Store(key, value)
}

// Delete removes a key.
func (sm *SyncMap[K, V]) Delete(key K) {
	sm.m.Delete(key)
}

// GetOrSet loads existing value or stores new one.
func (sm *SyncMap[K, V]) GetOrSet(key K, value V) (V, bool) {
	actual, loaded := sm.m.LoadOrStore(key, value)
	return actual.(V), loaded
}

// GetAndDelete deletes and returns the previous value if any.
func (sm *SyncMap[K, V]) GetAndDelete(key K) (V, bool) {
	val, loaded := sm.m.LoadAndDelete(key)
	if !loaded {
		var zero V
		return zero, false
	}
	return val.(V), true
}

// Range iterates over all items.
// Note: Range does not snapshot - it may or may not observe concurrent updates.
func (sm *SyncMap[K, V]) Range(fn func(key K, value V) bool) {
	sm.m.Range(func(k, v any) bool {
		return fn(k.(K), v.(V))
	})
}

// CompareAndSwap swaps if current value matches old.
func (sm *SyncMap[K, V]) CompareAndSwap(key K, old, new V) bool {
	return sm.m.CompareAndSwap(key, old, new)
}

// CompareAndDelete deletes if current value matches old.
func (sm *SyncMap[K, V]) CompareAndDelete(key K, old V) bool {
	return sm.m.CompareAndDelete(key, old)
}
