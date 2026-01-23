package scheduler

import (
	"context"
	"sync"
	"time"
)

// TaskWaiter provides a way to wait for task completion using sync.Cond.
//
// sync.Cond is useful when:
// - Multiple goroutines need to wait for a condition
// - You need to broadcast to wake all waiters at once
// - A channel would require creating one per waiter
//
// Workflow:
//
//	Waiter 1 ─┬─► Wait() ─► [blocked] ─► [signaled] ─► continue
//	          │                 ▲
//	Waiter 2 ─┼─► Wait() ─────────────►
//	          │                 │
//	Task ─────┴─► Complete ─► Broadcast()
type TaskWaiter struct {
	cond      *sync.Cond
	completed map[TaskID]bool
	errors    map[TaskID]error
	mu        sync.RWMutex
}

// NewTaskWaiter creates a new task waiter.
func NewTaskWaiter() *TaskWaiter {
	tw := &TaskWaiter{
		completed: make(map[TaskID]bool),
		errors:    make(map[TaskID]error),
	}
	tw.cond = sync.NewCond(&tw.mu)
	return tw
}

// Wait blocks until the specified task completes or context is cancelled.
// Returns the task's error (if any) or context error.
func (tw *TaskWaiter) Wait(ctx context.Context, taskID TaskID) error {
	// Check if already completed
	tw.mu.Lock()
	if tw.completed[taskID] {
		err := tw.errors[taskID]
		tw.mu.Unlock()
		return err
	}

	// Set up context cancellation
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tw.cond.Broadcast() // Wake all waiters to check context
		case <-done:
		}
	}()
	defer close(done)

	// Wait for completion
	for !tw.completed[taskID] {
		if ctx.Err() != nil {
			tw.mu.Unlock()
			return ctx.Err()
		}
		tw.cond.Wait() // Releases lock, waits, reacquires lock
	}

	err := tw.errors[taskID]
	tw.mu.Unlock()
	return err
}

// WaitAny waits for any of the specified tasks to complete.
// Returns the ID of the first completed task and its error.
func (tw *TaskWaiter) WaitAny(ctx context.Context, taskIDs ...TaskID) (TaskID, error) {
	tw.mu.Lock()

	// Check if any already completed
	for _, id := range taskIDs {
		if tw.completed[id] {
			err := tw.errors[id]
			tw.mu.Unlock()
			return id, err
		}
	}

	// Set up context cancellation
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tw.cond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

	// Wait for any completion
	for {
		if ctx.Err() != nil {
			tw.mu.Unlock()
			return "", ctx.Err()
		}

		for _, id := range taskIDs {
			if tw.completed[id] {
				err := tw.errors[id]
				tw.mu.Unlock()
				return id, err
			}
		}
		tw.cond.Wait()
	}
}

// WaitAll waits for all specified tasks to complete.
// Returns a map of task errors.
func (tw *TaskWaiter) WaitAll(ctx context.Context, taskIDs ...TaskID) map[TaskID]error {
	tw.mu.Lock()

	// Set up context cancellation
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			tw.cond.Broadcast()
		case <-done:
		}
	}()
	defer close(done)

	// Wait for all completions
	for {
		if ctx.Err() != nil {
			tw.mu.Unlock()
			return map[TaskID]error{"": ctx.Err()}
		}

		allDone := true
		for _, id := range taskIDs {
			if !tw.completed[id] {
				allDone = false
				break
			}
		}

		if allDone {
			results := make(map[TaskID]error)
			for _, id := range taskIDs {
				results[id] = tw.errors[id]
			}
			tw.mu.Unlock()
			return results
		}
		tw.cond.Wait()
	}
}

// MarkComplete marks a task as completed and wakes all waiters.
func (tw *TaskWaiter) MarkComplete(taskID TaskID, err error) {
	tw.mu.Lock()
	tw.completed[taskID] = true
	tw.errors[taskID] = err
	tw.mu.Unlock()
	tw.cond.Broadcast() // Wake ALL waiting goroutines
}

// IsCompleted checks if a task is completed.
func (tw *TaskWaiter) IsCompleted(taskID TaskID) bool {
	tw.mu.RLock()
	defer tw.mu.RUnlock()
	return tw.completed[taskID]
}

// Reset clears the completion state for a task (for retries).
func (tw *TaskWaiter) Reset(taskID TaskID) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	delete(tw.completed, taskID)
	delete(tw.errors, taskID)
}

// DistributedLock provides a simple interface for distributed locking.
// This is a local implementation for demonstration - in production,
// you'd use Redis, etcd, or ZooKeeper.
//
// Pattern:
//
//	if lock.Acquire(ctx, "task-123") {
//	    defer lock.Release("task-123")
//	    // execute task
//	}
type DistributedLock struct {
	locks map[string]lockInfo
	mu    sync.Mutex
}

type lockInfo struct {
	holder    string
	expiresAt time.Time
}

// NewDistributedLock creates a new distributed lock.
func NewDistributedLock() *DistributedLock {
	return &DistributedLock{
		locks: make(map[string]lockInfo),
	}
}

// Acquire attempts to acquire a lock with the given TTL.
// Returns true if lock was acquired, false if already held.
func (dl *DistributedLock) Acquire(ctx context.Context, key string, holder string, ttl time.Duration) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	now := time.Now()

	// Check if lock exists and is not expired
	if info, exists := dl.locks[key]; exists {
		if info.expiresAt.After(now) && info.holder != holder {
			return false // Lock held by someone else
		}
	}

	// Acquire or renew lock
	dl.locks[key] = lockInfo{
		holder:    holder,
		expiresAt: now.Add(ttl),
	}
	return true
}

// Release releases a lock if held by the specified holder.
func (dl *DistributedLock) Release(key string, holder string) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if info, exists := dl.locks[key]; exists {
		if info.holder == holder {
			delete(dl.locks, key)
			return true
		}
	}
	return false
}

// Extend extends the TTL of a lock if held by the specified holder.
func (dl *DistributedLock) Extend(key string, holder string, ttl time.Duration) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if info, exists := dl.locks[key]; exists {
		if info.holder == holder {
			dl.locks[key] = lockInfo{
				holder:    holder,
				expiresAt: time.Now().Add(ttl),
			}
			return true
		}
	}
	return false
}

// IsLocked checks if a key is currently locked.
func (dl *DistributedLock) IsLocked(key string) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if info, exists := dl.locks[key]; exists {
		return info.expiresAt.After(time.Now())
	}
	return false
}
