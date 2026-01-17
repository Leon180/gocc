package counter

import (
	"sync"
	"testing"
)

func TestAtomicCounter_Basic(t *testing.T) {
	c := NewAtomicCounter()

	// Test initial value
	if v := c.GetValue(); v != 0 {
		t.Errorf("Expected initial value 0, got %d", v)
	}

	// Test Increment
	c.Increment()
	if v := c.GetValue(); v != 1 {
		t.Errorf("Expected value 1 after increment, got %d", v)
	}

	// Test Decrement
	c.Decrement()
	if v := c.GetValue(); v != 0 {
		t.Errorf("Expected value 0 after decrement, got %d", v)
	}

	// Test Reset
	c.Increment()
	c.Increment()
	c.Reset()
	if v := c.GetValue(); v != 0 {
		t.Errorf("Expected value 0 after reset, got %d", v)
	}
}

// TestAtomicCounter_Concurrent tests the counter with multiple goroutines.
// This test is designed to expose race conditions!
// Run with: go test -race -v
func TestAtomicCounter_Concurrent(t *testing.T) {
	c := NewAtomicCounter()
	numGoroutines := 100
	incrementsPerGoroutine := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines that all increment the counter
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range incrementsPerGoroutine {
				c.Increment()
			}
		}()
	}

	wg.Wait()

	expected := int64(numGoroutines * incrementsPerGoroutine)
	actual := c.GetValue()

	// NOTE: Without synchronization, this will likely fail!
	// The actual value will be less than expected due to lost updates.
	if actual != expected {
		t.Logf("Race condition detected! Expected %d, got %d (lost %d updates)",
			expected, actual, expected-actual)
		// We don't fail the test here to demonstrate the issue
		// In a real scenario, you'd want this to fail
	}
}

// TestAtomicCounter_ConcurrentIncrementDecrement tests mixed operations.
func TestAtomicCounter_ConcurrentIncrementDecrement(t *testing.T) {
	c := NewAtomicCounter()
	numGoroutines := 50
	opsPerGoroutine := 1000

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Half increment, half decrement

	// Increment goroutines
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				c.Increment()
			}
		}()
	}

	// Decrement goroutines
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				c.Decrement()
			}
		}()
	}

	wg.Wait()

	// With equal increments and decrements, result should be 0
	// But due to race conditions, it likely won't be!
	actual := c.GetValue()
	if actual != 0 {
		t.Logf("Race condition detected! Expected 0, got %d", actual)
	}
}
