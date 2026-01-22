package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduler_ScheduleOnce(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	var executed int32

	task, err := s.ScheduleOnce("task1", "Test Task", 50*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("ScheduleOnce failed: %v", err)
	}

	if task.State() != TaskStatePending {
		t.Errorf("Expected pending state, got %v", task.State())
	}

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&executed) != 1 {
		t.Errorf("Expected 1 execution, got %d", executed)
	}

	if task.State() != TaskStateCompleted {
		t.Errorf("Expected completed state, got %v", task.State())
	}
}

func TestScheduler_ScheduleInterval(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	var count int32

	_, err := s.ScheduleInterval("task1", "Interval Task", 30*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("ScheduleInterval failed: %v", err)
	}

	// Wait for multiple executions
	time.Sleep(100 * time.Millisecond)

	executions := atomic.LoadInt32(&count)
	// Should have executed at least 2-3 times
	if executions < 2 {
		t.Errorf("Expected at least 2 executions, got %d", executions)
	}
}

func TestScheduler_CancelOnce(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	var executed int32

	task, _ := s.ScheduleOnce("task1", "Cancelled Task", 100*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&executed, 1)
		return nil
	})

	// Cancel before execution
	time.Sleep(30 * time.Millisecond)
	err := s.Cancel("task1")
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Wait past scheduled time
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&executed) != 0 {
		t.Errorf("Task should not have executed after cancel")
	}

	if task.State() != TaskStateCancelled {
		t.Errorf("Expected cancelled state, got %v", task.State())
	}
}

func TestScheduler_CancelInterval(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	var count int32

	_, err := s.ScheduleInterval("task1", "Interval Task", 20*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("ScheduleInterval failed: %v", err)
	}

	// Let it run a couple times
	time.Sleep(50 * time.Millisecond)
	countBeforeCancel := atomic.LoadInt32(&count)

	// Cancel
	_ = s.Cancel("task1")

	// Wait and check no more executions
	time.Sleep(50 * time.Millisecond)
	countAfterCancel := atomic.LoadInt32(&count)

	if countAfterCancel > countBeforeCancel+1 {
		t.Errorf("Task continued after cancel: before=%d, after=%d", countBeforeCancel, countAfterCancel)
	}
}

func TestScheduler_DuplicateTask(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	_, err := s.ScheduleOnce("task1", "First", 100*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("First schedule failed: %v", err)
	}

	_, err = s.ScheduleOnce("task1", "Duplicate", 100*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	if err != ErrTaskExists {
		t.Errorf("Expected ErrTaskExists, got %v", err)
	}
}

func TestScheduler_Remove(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	_, _ = s.ScheduleOnce("task1", "To Remove", 1*time.Hour, func(ctx context.Context) error {
		return nil
	})

	err := s.Remove("task1")
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	_, err = s.Get("task1")
	if err != ErrTaskNotFound {
		t.Errorf("Expected ErrTaskNotFound, got %v", err)
	}
}

func TestScheduler_List(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	_, _ = s.ScheduleOnce("task1", "Task 1", 1*time.Hour, func(ctx context.Context) error { return nil })
	_, _ = s.ScheduleOnce("task2", "Task 2", 1*time.Hour, func(ctx context.Context) error { return nil })
	_, _ = s.ScheduleInterval("task3", "Task 3", 1*time.Hour, func(ctx context.Context) error { return nil })

	tasks := s.List()
	if len(tasks) != 3 {
		t.Errorf("Expected 3 tasks, got %d", len(tasks))
	}
}

func TestScheduler_Stats(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	_, _ = s.ScheduleOnce("task1", "Task 1", 20*time.Millisecond, func(ctx context.Context) error { return nil })
	_, _ = s.ScheduleOnce("task2", "Task 2", 20*time.Millisecond, func(ctx context.Context) error { return nil })

	time.Sleep(50 * time.Millisecond)

	stats := s.Stats()
	if stats.TotalScheduled != 2 {
		t.Errorf("Expected TotalScheduled=2, got %d", stats.TotalScheduled)
	}
	if stats.TotalCompleted != 2 {
		t.Errorf("Expected TotalCompleted=2, got %d", stats.TotalCompleted)
	}
}

func TestScheduler_Close(t *testing.T) {
	s := New()

	_, _ = s.ScheduleInterval("task1", "Interval", 10*time.Millisecond, func(ctx context.Context) error { return nil })

	err := s.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should not be able to schedule after close
	_, err = s.ScheduleOnce("task2", "After Close", 10*time.Millisecond, func(ctx context.Context) error { return nil })
	if err != ErrSchedulerClosed {
		t.Errorf("Expected ErrSchedulerClosed, got %v", err)
	}
}

func TestTask_Stats(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	task, _ := s.ScheduleOnce("task1", "Stats Test", 20*time.Millisecond, func(ctx context.Context) error {
		return nil
	})

	time.Sleep(50 * time.Millisecond)

	stats := task.Stats()
	if stats.RunCount != 1 {
		t.Errorf("Expected RunCount=1, got %d", stats.RunCount)
	}
	if stats.State != TaskStateCompleted {
		t.Errorf("Expected completed state, got %v", stats.State)
	}
}

func TestScheduler_ScheduleCron(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	var count int32

	// Every second
	task, err := s.ScheduleCron("cron1", "Every Second", "* * * * * *", func(ctx context.Context) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	if err != nil {
		t.Fatalf("ScheduleCron failed: %v", err)
	}

	if task.Type != TaskTypeCron {
		t.Errorf("Expected TaskTypeCron, got %v", task.Type)
	}

	// Wait for a few executions
	time.Sleep(2500 * time.Millisecond)

	executions := atomic.LoadInt32(&count)
	// Should have executed at least 2 times
	if executions < 2 {
		t.Errorf("Expected at least 2 executions, got %d", executions)
	}
}

func TestScheduler_ScheduleCron_InvalidExpr(t *testing.T) {
	s := New()
	defer func() { _ = s.Close() }()

	_, err := s.ScheduleCron("cron1", "Invalid", "invalid", func(ctx context.Context) error {
		return nil
	})

	if err == nil {
		t.Error("Expected error for invalid cron expression")
	}
}
