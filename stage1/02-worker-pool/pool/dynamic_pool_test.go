package pool

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDynamicPool_Basic(t *testing.T) {
	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    2,
		MaxWorkers:    5,
		JobBufferSize: 10,
	}, func(job Job) Result {
		n := job.Data.(int)
		return Result{JobID: job.ID, Output: n * 2}
	})

	pool.Start()

	// Submit jobs
	for i := 1; i <= 5; i++ {
		pool.Submit(Job{ID: i, Data: i})
	}

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

	pool.Shutdown(5 * time.Second)
	wg.Wait()

	// Verify
	expected := map[int]int{1: 2, 2: 4, 3: 6, 4: 8, 5: 10}
	for id, exp := range expected {
		if got := results[id]; got != exp {
			t.Errorf("Job %d: expected %d, got %d", id, exp, got)
		}
	}
}

func TestDynamicPool_Resize(t *testing.T) {
	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    1,
		MaxWorkers:    10,
		JobBufferSize: 100,
	}, func(job Job) Result {
		time.Sleep(10 * time.Millisecond)
		return Result{JobID: job.ID}
	})

	pool.Start()

	// Start with min workers
	if n := pool.NumWorkers(); n != 1 {
		t.Errorf("Expected 1 worker, got %d", n)
	}

	// Scale up
	pool.Resize(5)
	time.Sleep(50 * time.Millisecond) // Let workers spawn

	if n := pool.NumWorkers(); n != 5 {
		t.Errorf("Expected 5 workers after resize, got %d", n)
	}

	pool.Shutdown(5 * time.Second)
}

func TestDynamicPool_TaskTimeout(t *testing.T) {
	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    2,
		MaxWorkers:    5,
		JobBufferSize: 10,
		TaskTimeout:   50 * time.Millisecond,
	}, func(job Job) Result {
		// Simulate slow job
		time.Sleep(200 * time.Millisecond)
		return Result{JobID: job.ID, Output: "done"}
	})

	pool.Start()

	pool.Submit(Job{ID: 1})

	// Get result
	var result Result
	done := make(chan struct{})
	go func() {
		result = <-pool.Results()
		close(done)
	}()

	select {
	case <-done:
		// Should have timed out
		if result.Err != context.DeadlineExceeded {
			t.Errorf("Expected DeadlineExceeded, got %v", result.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Test timed out waiting for result")
	}

	pool.Shutdown(1 * time.Second)
}

func TestDynamicPool_PanicRecovery(t *testing.T) {
	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    2,
		MaxWorkers:    5,
		JobBufferSize: 10,
	}, func(job Job) Result {
		if job.ID == 2 {
			panic("intentional panic")
		}
		return Result{JobID: job.ID, Output: "ok"}
	})

	pool.Start()

	// Submit jobs, one will panic
	for i := 1; i <= 3; i++ {
		pool.Submit(Job{ID: i})
	}

	pool.Close()

	// Collect results
	results := make(map[int]Result)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for result := range pool.Results() {
			results[result.JobID] = result
		}
	}()

	pool.Shutdown(5 * time.Second)
	wg.Wait()

	// Job 2 should have panic error
	if results[2].Err == nil {
		t.Error("Expected error for panicked job")
	}
	if _, ok := results[2].Err.(*PanicError); !ok {
		t.Errorf("Expected PanicError, got %T", results[2].Err)
	}

	// Other jobs should succeed
	if results[1].Err != nil {
		t.Errorf("Job 1 should not have error: %v", results[1].Err)
	}
	if results[3].Err != nil {
		t.Errorf("Job 3 should not have error: %v", results[3].Err)
	}
}

func TestDynamicPool_HealthCheck(t *testing.T) {
	healthy := true

	pool := NewDynamicPool(DynamicPoolConfig{
		MinWorkers:    2,
		MaxWorkers:    5,
		JobBufferSize: 10,
		HealthCheck: func() bool {
			return healthy
		},
	}, func(job Job) Result {
		return Result{JobID: job.ID}
	})

	pool.Start()

	if !pool.IsHealthy() {
		t.Error("Pool should be healthy")
	}

	healthy = false

	if pool.IsHealthy() {
		t.Error("Pool should be unhealthy")
	}

	pool.Shutdown(1 * time.Second)
}
