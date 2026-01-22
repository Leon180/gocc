package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// TaskID is a unique identifier for a task.
type TaskID string

// TaskFunc is the function signature for task execution.
type TaskFunc func(ctx context.Context) error

// TaskType defines how a task is scheduled.
type TaskType int

const (
	// TaskTypeOnce runs once after a delay.
	TaskTypeOnce TaskType = iota
	// TaskTypeInterval runs repeatedly at fixed intervals.
	TaskTypeInterval
	// TaskTypeCron runs based on a cron expression.
	TaskTypeCron
)

// TaskState represents the current state of a task.
type TaskState int

const (
	TaskStatePending TaskState = iota
	TaskStateRunning
	TaskStateCompleted
	TaskStateCancelled
	TaskStateFailed
)

func (s TaskState) String() string {
	switch s {
	case TaskStatePending:
		return "pending"
	case TaskStateRunning:
		return "running"
	case TaskStateCompleted:
		return "completed"
	case TaskStateCancelled:
		return "cancelled"
	case TaskStateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Task represents a scheduled task.
type Task struct {
	ID         TaskID
	Name       string
	Type       TaskType
	Delay      time.Duration // For Once: delay before execution
	Interval   time.Duration // For Interval: time between executions
	CronExpr   *CronExpr     // For Cron: parsed cron expression
	Func       TaskFunc
	CreatedAt  time.Time
	NextRunAt  time.Time
	LastRunAt  time.Time
	RunCount   int64
	ErrorCount int64
	LastError  error
	state      TaskState
	cancel     context.CancelFunc
	mu         sync.RWMutex
}

// State returns the current task state.
func (t *Task) State() TaskState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.state
}

// SetState sets the task state.
func (t *Task) SetState(s TaskState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.state = s
}

// IncrementRunCount safely increments the run count.
func (t *Task) IncrementRunCount() {
	atomic.AddInt64(&t.RunCount, 1)
}

// IncrementErrorCount safely increments the error count.
func (t *Task) IncrementErrorCount() {
	atomic.AddInt64(&t.ErrorCount, 1)
}

// Cancel cancels the task.
func (t *Task) Cancel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
	t.state = TaskStateCancelled
}

// TaskStats holds statistics for a task.
type TaskStats struct {
	ID         TaskID
	Name       string
	State      TaskState
	RunCount   int64
	ErrorCount int64
	NextRunAt  time.Time
	LastRunAt  time.Time
	LastError  error
}

// Stats returns current task statistics.
func (t *Task) Stats() TaskStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return TaskStats{
		ID:         t.ID,
		Name:       t.Name,
		State:      t.state,
		RunCount:   atomic.LoadInt64(&t.RunCount),
		ErrorCount: atomic.LoadInt64(&t.ErrorCount),
		NextRunAt:  t.NextRunAt,
		LastRunAt:  t.LastRunAt,
		LastError:  t.LastError,
	}
}
