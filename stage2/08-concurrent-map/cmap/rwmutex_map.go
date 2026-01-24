package cmap

import "sync"

// RWMutexMap is a concurrent map using sync.RWMutex.
// Allows multiple readers OR one writer at a time.
//
// Best for: Read-heavy workloads (90%+ reads)
// Drawback: Writes still block all reads, RWMutex overhead
//
// Workflow:
//
//	Read  ──► RLock ──► read ──► RUnlock  ─┐
//	Read  ──► RLock ──► read ──► RUnlock   ├── concurrent
//	Read  ──► RLock ──► read ──► RUnlock  ─┘
//
//	Write ──► Lock  ──► write ──► Unlock   ← exclusive
type RWMutexMap[K comparable, V any] struct {
	data map[K]V
	mu   sync.RWMutex
}

// NewRWMutexMap creates a new RWMutexMap.
func NewRWMutexMap[K comparable, V any]() *RWMutexMap[K, V] {
	return &RWMutexMap[K, V]{
		data: make(map[K]V),
	}
}

// Get retrieves a value by key (uses read lock).
func (m *RWMutexMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

// Set stores a value (uses write lock).
func (m *RWMutexMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete removes a key (uses write lock).
func (m *RWMutexMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Len returns the number of items.
func (m *RWMutexMap[K, V]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// Range iterates over all items (holds read lock).
func (m *RWMutexMap[K, V]) Range(fn func(key K, value V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}

// GetOrSet returns existing value or sets new one atomically.
func (m *RWMutexMap[K, V]) GetOrSet(key K, value V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.data[key]; ok {
		return v, true
	}
	m.data[key] = value
	return value, false
}
