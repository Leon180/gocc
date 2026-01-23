package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTaskWaiter_Wait(t *testing.T) {
	tw := NewTaskWaiter()

	var wg sync.WaitGroup

	// Start waiter in background
	wg.Add(1)
	var waitErr error
	go func() {
		defer wg.Done()
		waitErr = tw.Wait(context.Background(), "task1")
	}()

	// Give waiter time to start
	time.Sleep(50 * time.Millisecond)

	// Mark complete
	tw.MarkComplete("task1", nil)

	wg.Wait()

	if waitErr != nil {
		t.Errorf("Expected no error, got %v", waitErr)
	}
}

func TestTaskWaiter_WaitWithError(t *testing.T) {
	tw := NewTaskWaiter()

	var wg sync.WaitGroup
	wg.Add(1)

	var waitErr error
	go func() {
		defer wg.Done()
		waitErr = tw.Wait(context.Background(), "task1")
	}()

	time.Sleep(50 * time.Millisecond)

	// Mark complete with error
	expectedErr := errors.New("task failed")
	tw.MarkComplete("task1", expectedErr)

	wg.Wait()

	if waitErr != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, waitErr)
	}
}

func TestTaskWaiter_WaitContextCancel(t *testing.T) {
	tw := NewTaskWaiter()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := tw.Wait(ctx, "never-completes")

	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got %v", err)
	}
}

func TestTaskWaiter_AlreadyCompleted(t *testing.T) {
	tw := NewTaskWaiter()

	// Mark complete before waiting
	tw.MarkComplete("task1", nil)

	// Wait should return immediately
	start := time.Now()
	err := tw.Wait(context.Background(), "task1")
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("Wait took too long for completed task: %v", elapsed)
	}
}

func TestTaskWaiter_WaitAny(t *testing.T) {
	tw := NewTaskWaiter()

	var wg sync.WaitGroup
	wg.Add(1)

	var completedID TaskID
	go func() {
		defer wg.Done()
		completedID, _ = tw.WaitAny(context.Background(), "task1", "task2", "task3")
	}()

	time.Sleep(50 * time.Millisecond)

	// Complete task2 first
	tw.MarkComplete("task2", nil)

	wg.Wait()

	if completedID != "task2" {
		t.Errorf("Expected task2, got %s", completedID)
	}
}

func TestTaskWaiter_WaitAll(t *testing.T) {
	tw := NewTaskWaiter()

	var wg sync.WaitGroup
	wg.Add(1)

	var results map[TaskID]error
	go func() {
		defer wg.Done()
		results = tw.WaitAll(context.Background(), "task1", "task2")
	}()

	time.Sleep(50 * time.Millisecond)

	// Complete both
	tw.MarkComplete("task1", nil)
	tw.MarkComplete("task2", errors.New("task2 error"))

	wg.Wait()

	if results["task1"] != nil {
		t.Errorf("Expected nil for task1, got %v", results["task1"])
	}
	if results["task2"] == nil {
		t.Error("Expected error for task2")
	}
}

func TestTaskWaiter_MultipleWaiters(t *testing.T) {
	tw := NewTaskWaiter()

	var wg sync.WaitGroup
	numWaiters := 5

	for i := 0; i < numWaiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tw.Wait(context.Background(), "shared-task")
		}()
	}

	time.Sleep(50 * time.Millisecond)

	// Complete - all waiters should be notified
	tw.MarkComplete("shared-task", nil)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - all waiters completed
	case <-time.After(1 * time.Second):
		t.Error("Timeout waiting for all waiters")
	}
}

func TestDistributedLock_Acquire(t *testing.T) {
	dl := NewDistributedLock()

	// First acquire should succeed
	acquired := dl.Acquire(context.Background(), "key1", "holder1", 1*time.Minute)
	if !acquired {
		t.Error("Expected first acquire to succeed")
	}

	// Second acquire by different holder should fail
	acquired = dl.Acquire(context.Background(), "key1", "holder2", 1*time.Minute)
	if acquired {
		t.Error("Expected second acquire to fail")
	}

	// Same holder can re-acquire (renew)
	acquired = dl.Acquire(context.Background(), "key1", "holder1", 1*time.Minute)
	if !acquired {
		t.Error("Expected same holder to re-acquire")
	}
}

func TestDistributedLock_Release(t *testing.T) {
	dl := NewDistributedLock()

	dl.Acquire(context.Background(), "key1", "holder1", 1*time.Minute)

	// Wrong holder can't release
	released := dl.Release("key1", "holder2")
	if released {
		t.Error("Wrong holder should not release")
	}

	// Correct holder can release
	released = dl.Release("key1", "holder1")
	if !released {
		t.Error("Correct holder should release")
	}

	// Now another holder can acquire
	acquired := dl.Acquire(context.Background(), "key1", "holder2", 1*time.Minute)
	if !acquired {
		t.Error("Should acquire after release")
	}
}

func TestDistributedLock_Expiry(t *testing.T) {
	dl := NewDistributedLock()

	// Acquire with short TTL
	dl.Acquire(context.Background(), "key1", "holder1", 50*time.Millisecond)

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Another holder should now be able to acquire
	acquired := dl.Acquire(context.Background(), "key1", "holder2", 1*time.Minute)
	if !acquired {
		t.Error("Should acquire after expiry")
	}
}

func TestDistributedLock_IsLocked(t *testing.T) {
	dl := NewDistributedLock()

	if dl.IsLocked("key1") {
		t.Error("Should not be locked initially")
	}

	dl.Acquire(context.Background(), "key1", "holder1", 1*time.Minute)

	if !dl.IsLocked("key1") {
		t.Error("Should be locked after acquire")
	}

	dl.Release("key1", "holder1")

	if dl.IsLocked("key1") {
		t.Error("Should not be locked after release")
	}
}

func TestDistributedLock_Extend(t *testing.T) {
	dl := NewDistributedLock()

	dl.Acquire(context.Background(), "key1", "holder1", 50*time.Millisecond)

	// Extend before expiry
	extended := dl.Extend("key1", "holder1", 1*time.Minute)
	if !extended {
		t.Error("Should extend own lock")
	}

	// Wrong holder can't extend
	extended = dl.Extend("key1", "holder2", 1*time.Minute)
	if extended {
		t.Error("Should not extend others lock")
	}
}
