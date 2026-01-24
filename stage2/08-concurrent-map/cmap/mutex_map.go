package cmap

import "sync"

// MutexMap is a simple concurrent map using sync.Mutex.
// All operations (read and write) are serialized.
//
// Best for: Small maps, balanced read/write workloads
// Drawback: All operations contend for the same lock
//
// Workflow:
//
//	Read  ──► Lock ──► read ──► Unlock
//	Write ──► Lock ──► write ──► Unlock
//	          ↑__________________|
//	              (same lock, serialized)
type MutexMap[K comparable, V any] struct {
	data map[K]V
	mu   sync.Mutex
}

// NewMutexMap creates a new MutexMap.
func NewMutexMap[K comparable, V any]() *MutexMap[K, V] {
	return &MutexMap[K, V]{
		data: make(map[K]V),
	}
}

// Get retrieves a value by key.
func (m *MutexMap[K, V]) Get(key K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	return v, ok
}

// Set stores a value.
func (m *MutexMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

// Delete removes a key.
func (m *MutexMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

// Len returns the number of items.
func (m *MutexMap[K, V]) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.data)
}

// Range iterates over all items.
func (m *MutexMap[K, V]) Range(fn func(key K, value V) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}
