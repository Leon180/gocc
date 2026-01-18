package pool

import (
	"context"
	"testing"
	"time"
)

// BenchmarkWorkerPool_Scaling tests how pool scales with worker count.
func BenchmarkWorkerPool_1Worker(b *testing.B) {
	benchmarkWorkerPool(b, 1)
}

func BenchmarkWorkerPool_2Workers(b *testing.B) {
	benchmarkWorkerPool(b, 2)
}

func BenchmarkWorkerPool_4Workers(b *testing.B) {
	benchmarkWorkerPool(b, 4)
}

func BenchmarkWorkerPool_8Workers(b *testing.B) {
	benchmarkWorkerPool(b, 8)
}

func BenchmarkWorkerPool_16Workers(b *testing.B) {
	benchmarkWorkerPool(b, 16)
}

func benchmarkWorkerPool(b *testing.B, numWorkers int) {
	pool := New(numWorkers, b.N, func(job Job) Result {
		// Simulate some work
		sum := 0
		for i := range 1000 {
			sum += i
		}
		return Result{JobID: job.ID, Output: sum}
	})

	ctx := context.Background()
	pool.Start(ctx)

	b.ResetTimer()

	// Submit b.N jobs
	for i := 0; i < b.N; i++ {
		pool.Submit(Job{ID: i, Data: i})
	}

	pool.Close()

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	pool.Wait()
}

// BenchmarkWorkerPool_Throughput measures jobs processed per second.
func BenchmarkWorkerPool_Throughput(b *testing.B) {
	pool := New(8, b.N+100, func(job Job) Result {
		// Light work
		return Result{JobID: job.ID}
	})

	ctx := context.Background()
	pool.Start(ctx)

	// Start draining results in background
	go func() {
		for range pool.Results() {
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Submit(Job{ID: i})
	}

	pool.Close()
	pool.Wait()
}

// BenchmarkGracefulPool_WithTimeout benchmarks pool with timeout overhead.
func BenchmarkGracefulPool_WithPanicRecovery(b *testing.B) {
	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    4,
		MaxWorkers:    8,
		JobBufferSize: b.N,
		TaskTimeout:   time.Second,
	}, func(job Job) Result {
		return Result{JobID: job.ID}
	})

	pool.Start()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pool.Submit(Job{ID: i})
	}

	pool.Shutdown(5 * time.Second)
}
