package counter

import "sync"

// MutexCounter is a thread-safe counter using sync.Mutex.
// It safely handles concurrent access from multiple goroutines.
type MutexCounter struct {
	mu    sync.Mutex
	value int64
}

// NewCounter creates a new Counter with initial value 0.
func NewMutexCounter() *MutexCounter {
	return &MutexCounter{value: 0}
}

// Increment adds 1 to the counter.
func (c *MutexCounter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
}

// Decrement subtracts 1 from the counter.
func (c *MutexCounter) Decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value--
}

// GetValue returns the current value of the counter.
func (c *MutexCounter) GetValue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// Reset sets the counter value back to 0.
func (c *MutexCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = 0
}
