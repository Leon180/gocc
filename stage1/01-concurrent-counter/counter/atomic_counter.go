package counter

import "sync/atomic"

// AtomicCounter is a thread-safe counter using sync.Mutex.
// It safely handles concurrent access from multiple goroutines.
type AtomicCounter struct {
	value atomic.Int64
}

// NewCounter creates a new Counter with initial value 0.
func NewAtomicCounter() *AtomicCounter {
	return &AtomicCounter{value: atomic.Int64{}}
}

// Increment adds 1 to the counter.
func (c *AtomicCounter) Increment() {
	c.value.Add(1)
}

// Decrement subtracts 1 from the counter.
func (c *AtomicCounter) Decrement() {
	c.value.Add(-1)
}

// GetValue returns the current value of the counter.
func (c *AtomicCounter) GetValue() int64 {
	return c.value.Load()
}

// Reset sets the counter value back to 0.
func (c *AtomicCounter) Reset() {
	c.value.Store(0)
}
