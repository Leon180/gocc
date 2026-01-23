package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Common errors
var (
	ErrSchedulerClosed = errors.New("scheduler is closed")
	ErrTaskNotFound    = errors.New("task not found")
	ErrTaskExists      = errors.New("task already exists")
)

// Scheduler manages and executes scheduled tasks.
//
// Workflow:
//
//	                  ┌─────────────────┐
//	                  │    Scheduler    │
//	                  │                 │
//	Schedule() ──────►│  tasks map      │
//	                  │  run loop       │◄──── time.Timer
//	Cancel() ────────►│                 │
//	                  └────────┬────────┘
//	                           │
//	                  ┌────────┴────────┐
//	                  ▼                 ▼
//	            ┌──────────┐      ┌──────────┐
//	            │  Task A  │      │  Task B  │
//	            │  (Once)  │      │(Interval)│
//	            └──────────┘      └──────────┘
type Scheduler struct {
	tasks  map[TaskID]*Task
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	closed int32

	// Statistics
	totalScheduled int64
	totalCompleted int64
	totalFailed    int64
}

// New creates a new scheduler.
func New() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		tasks:  make(map[TaskID]*Task),
		ctx:    ctx,
		cancel: cancel,
	}
}

// ScheduleOnce schedules a task to run once after a delay.
func (s *Scheduler) ScheduleOnce(id TaskID, name string, delay time.Duration, fn TaskFunc) (*Task, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, ErrSchedulerClosed
	}

	s.mu.Lock()
	if _, exists := s.tasks[id]; exists {
		s.mu.Unlock()
		return nil, ErrTaskExists
	}

	now := time.Now()
	taskCtx, taskCancel := context.WithCancel(s.ctx)

	task := &Task{
		ID:        id,
		Name:      name,
		Type:      TaskTypeOnce,
		Delay:     delay,
		Func:      fn,
		CreatedAt: now,
		NextRunAt: now.Add(delay),
		state:     TaskStatePending,
		cancel:    taskCancel,
	}

	s.tasks[id] = task
	atomic.AddInt64(&s.totalScheduled, 1)
	s.mu.Unlock()

	// Start the task goroutine
	s.wg.Add(1)
	go s.runOnce(taskCtx, task)

	return task, nil
}

// ScheduleInterval schedules a task to run repeatedly at fixed intervals.
func (s *Scheduler) ScheduleInterval(id TaskID, name string, interval time.Duration, fn TaskFunc) (*Task, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, ErrSchedulerClosed
	}

	s.mu.Lock()
	if _, exists := s.tasks[id]; exists {
		s.mu.Unlock()
		return nil, ErrTaskExists
	}

	now := time.Now()
	taskCtx, taskCancel := context.WithCancel(s.ctx)

	task := &Task{
		ID:        id,
		Name:      name,
		Type:      TaskTypeInterval,
		Interval:  interval,
		Func:      fn,
		CreatedAt: now,
		NextRunAt: now.Add(interval),
		state:     TaskStatePending,
		cancel:    taskCancel,
	}

	s.tasks[id] = task
	atomic.AddInt64(&s.totalScheduled, 1)
	s.mu.Unlock()

	// Start the task goroutine
	s.wg.Add(1)
	go s.runInterval(taskCtx, task)

	return task, nil
}

// runOnce executes a one-time task after delay.
func (s *Scheduler) runOnce(ctx context.Context, task *Task) {
	defer s.wg.Done()

	timer := time.NewTimer(task.Delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		task.SetState(TaskStateCancelled)
		return
	case <-timer.C:
		s.executeTask(ctx, task)
		task.SetState(TaskStateCompleted)
		atomic.AddInt64(&s.totalCompleted, 1)
	}
}

// runInterval executes a task repeatedly at fixed intervals.
func (s *Scheduler) runInterval(ctx context.Context, task *Task) {
	defer s.wg.Done()

	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			task.SetState(TaskStateCancelled)
			return
		case <-ticker.C:
			s.executeTask(ctx, task)
			task.mu.Lock()
			task.NextRunAt = time.Now().Add(task.Interval)
			task.mu.Unlock()
		}
	}
}

// ScheduleCron schedules a task using a cron expression.
// Format: "second minute hour day month weekday"
// Examples:
//   - "0 * * * * *"    → Every minute at second 0
//   - "*/5 * * * * *"  → Every 5 seconds
//   - "0 30 9 * * 1-5" → 9:30 AM Monday-Friday
func (s *Scheduler) ScheduleCron(id TaskID, name string, cronExpr string, fn TaskFunc) (*Task, error) {
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil, ErrSchedulerClosed
	}

	cron, err := ParseCron(cronExpr)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if _, exists := s.tasks[id]; exists {
		s.mu.Unlock()
		return nil, ErrTaskExists
	}

	now := time.Now()
	taskCtx, taskCancel := context.WithCancel(s.ctx)

	task := &Task{
		ID:        id,
		Name:      name,
		Type:      TaskTypeCron,
		CronExpr:  cron,
		Func:      fn,
		CreatedAt: now,
		NextRunAt: cron.Next(now),
		state:     TaskStatePending,
		cancel:    taskCancel,
	}

	s.tasks[id] = task
	atomic.AddInt64(&s.totalScheduled, 1)
	s.mu.Unlock()

	s.wg.Add(1)
	go s.runCron(taskCtx, task)

	return task, nil
}

// runCron executes a task based on cron schedule.
func (s *Scheduler) runCron(ctx context.Context, task *Task) {
	defer s.wg.Done()

	for {
		// Calculate time until next run
		task.mu.RLock()
		nextRun := task.NextRunAt
		task.mu.RUnlock()

		delay := time.Until(nextRun)
		if delay < 0 {
			delay = 0
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			timer.Stop()
			task.SetState(TaskStateCancelled)
			return
		case <-timer.C:
			s.executeTask(ctx, task)

			// Calculate next run time
			task.mu.Lock()
			task.NextRunAt = task.CronExpr.Next(time.Now())
			task.mu.Unlock()
		}
	}
}

// executeTask runs the task function with retry and dependency support.
func (s *Scheduler) executeTask(ctx context.Context, task *Task) {
	// Check dependencies first
	if !s.areDependenciesMet(task) {
		return // Skip execution, will retry on next schedule
	}

	task.SetState(TaskStateRunning)
	task.IncrementRunCount()

	task.mu.Lock()
	task.LastRunAt = time.Now()
	task.mu.Unlock()

	err := task.Func(ctx)

	task.mu.Lock()
	task.LastError = err
	retry := task.Retry
	task.mu.Unlock()

	if err != nil {
		task.IncrementErrorCount()
		atomic.AddInt64(&s.totalFailed, 1)

		// Handle retry if configured
		if retry != nil && retry.RetryCount < retry.MaxRetries {
			s.scheduleRetry(ctx, task)
		}
	}
}

// areDependenciesMet checks if all dependent tasks are completed.
func (s *Scheduler) areDependenciesMet(task *Task) bool {
	task.mu.RLock()
	deps := task.DependsOn
	task.mu.RUnlock()

	if len(deps) == 0 {
		return true
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, depID := range deps {
		dep, exists := s.tasks[depID]
		if !exists {
			continue // Dependency doesn't exist, consider it met
		}
		if dep.State() != TaskStateCompleted {
			return false
		}
	}
	return true
}

// scheduleRetry schedules a retry for a failed task.
func (s *Scheduler) scheduleRetry(ctx context.Context, task *Task) {
	task.mu.Lock()
	defer task.mu.Unlock()

	task.Retry.RetryCount++

	// Calculate backoff delay
	delay := task.Retry.Delay
	if task.Retry.Multiplier > 0 {
		for i := 1; i < task.Retry.RetryCount; i++ {
			delay = time.Duration(float64(delay) * task.Retry.Multiplier)
		}
	}
	if task.Retry.MaxDelay > 0 && delay > task.Retry.MaxDelay {
		delay = task.Retry.MaxDelay
	}

	task.Retry.NextRetryAt = time.Now().Add(delay)
	task.state = TaskStatePending

	// Schedule retry in background
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.executeTask(ctx, task)
		}
	}()
}

// Cancel cancels a scheduled task.
func (s *Scheduler) Cancel(id TaskID) error {
	s.mu.RLock()
	task, exists := s.tasks[id]
	s.mu.RUnlock()

	if !exists {
		return ErrTaskNotFound
	}

	task.Cancel()
	return nil
}

// Remove cancels and removes a task from the scheduler.
func (s *Scheduler) Remove(id TaskID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[id]
	if !exists {
		return ErrTaskNotFound
	}

	task.Cancel()
	delete(s.tasks, id)
	return nil
}

// Get returns a task by ID.
func (s *Scheduler) Get(id TaskID) (*Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[id]
	if !exists {
		return nil, ErrTaskNotFound
	}
	return task, nil
}

// List returns all tasks.
func (s *Scheduler) List() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tasks := make([]*Task, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

// SchedulerStats holds scheduler statistics.
type SchedulerStats struct {
	TotalTasks     int
	TotalScheduled int64
	TotalCompleted int64
	TotalFailed    int64
}

// Stats returns scheduler statistics.
func (s *Scheduler) Stats() SchedulerStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return SchedulerStats{
		TotalTasks:     len(s.tasks),
		TotalScheduled: atomic.LoadInt64(&s.totalScheduled),
		TotalCompleted: atomic.LoadInt64(&s.totalCompleted),
		TotalFailed:    atomic.LoadInt64(&s.totalFailed),
	}
}

// Close shuts down the scheduler and cancels all tasks.
func (s *Scheduler) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return ErrSchedulerClosed
	}

	s.cancel()
	s.wg.Wait()

	return nil
}
