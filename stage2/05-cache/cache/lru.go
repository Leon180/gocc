package cache

import (
	"sync"
	"time"
)

// LRUCache is a thread-safe cache with LRU (Least Recently Used) eviction.
// When the cache reaches capacity, the least recently accessed entry is evicted.
//
// Implementation uses a doubly-linked list for O(1) access order tracking.
type LRUCache[K comparable, V any] struct {
	capacity int
	data     map[K]*lruNode[K, V]
	head     *lruNode[K, V] // Most recently used
	tail     *lruNode[K, V] // Least recently used
	mu       sync.RWMutex

	// Statistics
	hits   int64
	misses int64
	evicts int64
}

// lruNode is a node in the doubly-linked list.
type lruNode[K comparable, V any] struct {
	key        K
	value      V
	expiration time.Time
	prev       *lruNode[K, V]
	next       *lruNode[K, V]
}

// NewLRU creates a new LRU cache with the given capacity.
func NewLRU[K comparable, V any](capacity int) *LRUCache[K, V] {
	if capacity <= 0 {
		capacity = 100
	}
	return &LRUCache[K, V]{
		capacity: capacity,
		data:     make(map[K]*lruNode[K, V]),
	}
}

// Get retrieves a value and moves it to the front (most recently used).
func (c *LRUCache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.data[key]
	if !exists {
		c.misses++
		var zero V
		return zero, false
	}

	// Check expiration
	if !node.expiration.IsZero() && time.Now().After(node.expiration) {
		c.removeNode(node)
		delete(c.data, key)
		c.misses++
		var zero V
		return zero, false
	}

	// Move to front (most recently used)
	c.moveToFront(node)
	c.hits++

	return node.value, true
}

// Set stores a value with no expiration.
func (c *LRUCache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, 0)
}

// SetWithTTL stores a value with a time-to-live.
func (c *LRUCache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var expiration time.Time
	if ttl > 0 {
		expiration = time.Now().Add(ttl)
	}

	// Update existing node
	if node, exists := c.data[key]; exists {
		node.value = value
		node.expiration = expiration
		c.moveToFront(node)
		return
	}

	// Evict if at capacity
	if len(c.data) >= c.capacity {
		c.evictLRU()
	}

	// Create new node
	node := &lruNode[K, V]{
		key:        key,
		value:      value,
		expiration: expiration,
	}
	c.data[key] = node
	c.addToFront(node)
}

// Delete removes a key from the cache.
func (c *LRUCache[K, V]) Delete(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, exists := c.data[key]
	if !exists {
		return false
	}

	c.removeNode(node)
	delete(c.data, key)
	return true
}

// Len returns the number of entries in the cache.
func (c *LRUCache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// Capacity returns the maximum capacity.
func (c *LRUCache[K, V]) Capacity() int {
	return c.capacity
}

// Clear removes all entries.
func (c *LRUCache[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[K]*lruNode[K, V])
	c.head = nil
	c.tail = nil
}

// LRUStats holds LRU cache statistics.
type LRUStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	Size      int
	Capacity  int
	HitRate   float64
}

// Stats returns cache statistics.
func (c *LRUCache[K, V]) Stats() LRUStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hits + c.misses
	var hitRate float64
	if total > 0 {
		hitRate = float64(c.hits) / float64(total)
	}

	return LRUStats{
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evicts,
		Size:      len(c.data),
		Capacity:  c.capacity,
		HitRate:   hitRate,
	}
}

// --- Internal linked list operations ---

// addToFront adds a node to the front of the list (most recently used).
func (c *LRUCache[K, V]) addToFront(node *lruNode[K, V]) {
	node.prev = nil
	node.next = c.head

	if c.head != nil {
		c.head.prev = node
	}
	c.head = node

	if c.tail == nil {
		c.tail = node
	}
}

// removeNode removes a node from the list.
func (c *LRUCache[K, V]) removeNode(node *lruNode[K, V]) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
}

// moveToFront moves an existing node to the front.
func (c *LRUCache[K, V]) moveToFront(node *lruNode[K, V]) {
	if node == c.head {
		return // Already at front
	}
	c.removeNode(node)
	c.addToFront(node)
}

// evictLRU removes the least recently used entry (tail).
func (c *LRUCache[K, V]) evictLRU() {
	if c.tail == nil {
		return
	}

	// Remove from map
	delete(c.data, c.tail.key)

	// Remove from list
	c.removeNode(c.tail)
	c.evicts++
}

// Keys returns all keys in order from most to least recently used.
func (c *LRUCache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]K, 0, len(c.data))
	for node := c.head; node != nil; node = node.next {
		keys = append(keys, node.key)
	}
	return keys
}
