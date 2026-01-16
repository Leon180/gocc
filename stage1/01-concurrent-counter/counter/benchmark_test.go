package counter

import "testing"

// =============================================================================
// Single-Goroutine Benchmarks (No Contention)
// =============================================================================

func BenchmarkUnsafeCounter_Increment(b *testing.B) {
	c := NewCounter()
	for i := 0; i < b.N; i++ {
		c.Increment()
	}
}

func BenchmarkMutexCounter_Increment(b *testing.B) {
	c := NewMutexCounter()
	for i := 0; i < b.N; i++ {
		c.Increment()
	}
}

func BenchmarkAtomicCounter_Increment(b *testing.B) {
	c := NewAtomicCounter()
	for i := 0; i < b.N; i++ {
		c.Increment()
	}
}

// =============================================================================
// Parallel Benchmarks (High Contention)
// These show the real difference between Mutex and Atomic under load!
// =============================================================================

func BenchmarkUnsafeCounter_Parallel(b *testing.B) {
	c := NewCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Increment()
		}
	})
}

func BenchmarkMutexCounter_Parallel(b *testing.B) {
	c := NewMutexCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Increment()
		}
	})
}

func BenchmarkAtomicCounter_Parallel(b *testing.B) {
	c := NewAtomicCounter()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Increment()
		}
	})
}

// =============================================================================
// Mixed Operations Benchmarks
// =============================================================================

func BenchmarkMutexCounter_MixedOps(b *testing.B) {
	c := NewMutexCounter()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				c.Increment()
			} else {
				c.GetValue()
			}
			i++
		}
	})
}

func BenchmarkAtomicCounter_MixedOps(b *testing.B) {
	c := NewAtomicCounter()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				c.Increment()
			} else {
				c.GetValue()
			}
			i++
		}
	})
}
