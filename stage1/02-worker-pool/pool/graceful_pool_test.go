package pool

import (
	"sync"
	"testing"
	"time"
)

func TestGracefulPool_Basic(t *testing.T) {
	pool := NewGracefulPool(3, 10, func(job Job) Result {
		return Result{JobID: job.ID, Output: job.Data.(int) * 2}
	})

	pool.Start(3)

	// Submit jobs
	for i := 1; i <= 5; i++ {
		if !pool.Submit(Job{ID: i, Data: i}) {
			t.Errorf("Failed to submit job %d", i)
		}
	}

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

	pool.GracefulShutdown(5 * time.Second)
	wg.Wait()

	// Verify
	for i := 1; i <= 5; i++ {
		if results[i] != i*2 {
			t.Errorf("Job %d: expected %d, got %d", i, i*2, results[i])
		}
	}
}

func TestGracefulPool_DrainMode(t *testing.T) {
	pool := NewGracefulPool(2, 10, func(job Job) Result {
		time.Sleep(10 * time.Millisecond)
		return Result{JobID: job.ID}
	})

	pool.Start(2)

	// Submit some jobs
	for i := 1; i <= 5; i++ {
		pool.Submit(Job{ID: i})
	}

	// Enter drain mode
	pool.Drain()

	// Should reject new jobs
	if pool.Submit(Job{ID: 100}) {
		t.Error("Should reject job after drain")
	}

	if pool.State() != StateDraining {
		t.Errorf("Expected draining state, got %d", pool.State())
	}

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	pool.GracefulShutdown(5 * time.Second)

	if pool.State() != StateStopped {
		t.Errorf("Expected stopped state, got %d", pool.State())
	}
}

func TestGracefulPool_ShutdownCallback(t *testing.T) {
	callbackCalled := false

	pool := NewGracefulPool(2, 10, func(job Job) Result {
		return Result{JobID: job.ID}
	})

	pool.OnShutdown(func() {
		callbackCalled = true
	})

	pool.Start(2)
	pool.Submit(Job{ID: 1})

	go func() {
		for range pool.Results() {
		}
	}()

	pool.GracefulShutdown(5 * time.Second)

	if !callbackCalled {
		t.Error("Shutdown callback was not called")
	}
}

func TestGracefulPool_ForceShutdown(t *testing.T) {
	pool := NewGracefulPool(2, 10, func(job Job) Result {
		// Check context to exit early
		select {
		case <-time.After(100 * time.Millisecond):
		}
		return Result{JobID: job.ID}
	})

	pool.Start(2)

	// Submit jobs
	for i := 1; i <= 5; i++ {
		pool.Submit(Job{ID: i})
	}

	// State should change immediately
	go pool.ForceShutdown()

	// Give a moment for state to update
	time.Sleep(10 * time.Millisecond)

	if pool.State() != StateStopped {
		t.Error("Expected stopped state after force shutdown")
	}

	// Wait for cleanup to finish
	time.Sleep(200 * time.Millisecond)
}

func TestGracefulPool_ShutdownTimeout(t *testing.T) {
	pool := NewGracefulPool(2, 10, func(job Job) Result {
		time.Sleep(500 * time.Millisecond) // Slow job
		return Result{JobID: job.ID}
	})

	pool.Start(2)

	// Submit slow jobs
	for i := 1; i <= 5; i++ {
		pool.Submit(Job{ID: i})
	}

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	// Shutdown with short timeout
	err := pool.GracefulShutdown(50 * time.Millisecond)

	if err == nil {
		t.Error("Expected timeout error")
	}
}
