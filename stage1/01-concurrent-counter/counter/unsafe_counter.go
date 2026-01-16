package counter

// Counter is a basic counter that is NOT thread-safe.
// This implementation demonstrates what happens when shared state
// is accessed by multiple goroutines without synchronization.
//
// WARNING: This counter has race conditions! Do not use in production.
// Use MutexCounter or AtomicCounter instead.
type Counter struct {
	value int64
}

// NewCounter creates a new Counter with initial value 0.
func NewCounter() *Counter {
	return &Counter{value: 0}
}

// Increment adds 1 to the counter.
// WARNING: This operation is not thread-safe!
func (c *Counter) Increment() {
	c.value++
}

// Decrement subtracts 1 from the counter.
// WARNING: This operation is not thread-safe!
func (c *Counter) Decrement() {
	c.value--
}

// GetValue returns the current value of the counter.
// WARNING: This operation is not thread-safe!
func (c *Counter) GetValue() int64 {
	return c.value
}

// Reset sets the counter value back to 0.
// WARNING: This operation is not thread-safe!
func (c *Counter) Reset() {
	c.value = 0
}
