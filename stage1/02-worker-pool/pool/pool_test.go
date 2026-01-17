package pool

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestWorkerPool_Basic(t *testing.T) {
	// Create pool with 3 workers
	pool := New(3, 10, func(job Job) Result {
		// Simple job: square the input
		n := job.Data.(int)
		return Result{JobID: job.ID, Output: n * n}
	})

	ctx := context.Background()
	pool.Start(ctx)

	// Submit 5 jobs
	for i := 1; i <= 5; i++ {
		pool.Submit(Job{ID: i, Data: i})
	}

	// Signal no more jobs
	pool.Close()

	// Collect results
	results := make(map[int]int)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range pool.Results() {
			results[result.JobID] = result.Output.(int)
		}
	}()

	// Wait for workers to finish
	pool.Wait()
	wg.Wait()

	// Verify results
	expected := map[int]int{1: 1, 2: 4, 3: 9, 4: 16, 5: 25}
	for id, exp := range expected {
		if got := results[id]; got != exp {
			t.Errorf("Job %d: expected %d, got %d", id, exp, got)
		}
	}
}

func TestWorkerPool_Concurrent(t *testing.T) {
	var processedCount int
	var mu sync.Mutex

	pool := New(5, 100, func(job Job) Result {
		mu.Lock()
		processedCount++
		mu.Unlock()
		return Result{JobID: job.ID}
	})

	ctx := context.Background()
	pool.Start(ctx)

	// Submit 100 jobs concurrently
	var submitWg sync.WaitGroup
	for i := range 100 {
		submitWg.Add(1)
		go func(id int) {
			defer submitWg.Done()
			pool.Submit(Job{ID: id})
		}(i)
	}
	submitWg.Wait()

	pool.Close()

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	pool.Wait()

	if processedCount != 100 {
		t.Errorf("Expected 100 jobs processed, got %d", processedCount)
	}
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	jobsStarted := 0
	var mu sync.Mutex

	pool := New(3, 10, func(job Job) Result {
		mu.Lock()
		jobsStarted++
		mu.Unlock()
		// Simulate slow job
		time.Sleep(100 * time.Millisecond)
		return Result{JobID: job.ID}
	})

	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)

	// Submit some jobs
	for i := range 10 {
		pool.Submit(Job{ID: i})
	}

	// Cancel after a short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	pool.Close()

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	pool.Wait()

	// Some jobs should have started but not all completed
	mu.Lock()
	started := jobsStarted
	mu.Unlock()

	t.Logf("Jobs started before cancellation: %d", started)
	// At least some should have started
	if started == 0 {
		t.Error("Expected some jobs to start")
	}
}

func TestWorkerPool_NoGoroutineLeak(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	for range 10 {
		pool := New(5, 10, func(job Job) Result {
			return Result{JobID: job.ID}
		})

		ctx := context.Background()
		pool.Start(ctx)

		for j := range 20 {
			pool.Submit(Job{ID: j})
		}

		pool.Close()
		go func() {
			for range pool.Results() {
			}
		}()
		pool.Wait()
	}

	// Give goroutines time to clean up
	time.Sleep(100 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()

	// Should be roughly the same (allow some variance for test framework)
	if finalGoroutines > initialGoroutines+5 {
		t.Errorf("Possible goroutine leak: started with %d, ended with %d",
			initialGoroutines, finalGoroutines)
	}
}
