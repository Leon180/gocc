package scheduler

import (
	"testing"
	"time"
)

func TestPriorityQueue_Basic(t *testing.T) {
	pq := NewPriorityQueue()

	if pq.Size() != 0 {
		t.Errorf("Expected empty queue, got size %d", pq.Size())
	}

	// Add tasks with different priorities
	task1 := &Task{ID: "low", Name: "Low Priority", Priority: PriorityLow}
	task2 := &Task{ID: "high", Name: "High Priority", Priority: PriorityHigh}
	task3 := &Task{ID: "normal", Name: "Normal Priority", Priority: PriorityNormal}

	pq.PushTask(task1)
	pq.PushTask(task2)
	pq.PushTask(task3)

	if pq.Size() != 3 {
		t.Errorf("Expected 3 tasks, got %d", pq.Size())
	}

	// Pop should return highest priority first
	popped := pq.PopTask()
	if popped.ID != "high" {
		t.Errorf("Expected high priority task first, got %s", popped.ID)
	}

	popped = pq.PopTask()
	if popped.ID != "normal" {
		t.Errorf("Expected normal priority task second, got %s", popped.ID)
	}

	popped = pq.PopTask()
	if popped.ID != "low" {
		t.Errorf("Expected low priority task last, got %s", popped.ID)
	}
}

func TestPriorityQueue_SamePriority_TimeOrdering(t *testing.T) {
	pq := NewPriorityQueue()

	now := time.Now()
	task1 := &Task{ID: "later", Priority: PriorityNormal, NextRunAt: now.Add(2 * time.Second)}
	task2 := &Task{ID: "earlier", Priority: PriorityNormal, NextRunAt: now.Add(1 * time.Second)}
	task3 := &Task{ID: "earliest", Priority: PriorityNormal, NextRunAt: now}

	pq.PushTask(task1)
	pq.PushTask(task2)
	pq.PushTask(task3)

	// Same priority: earlier NextRunAt comes first
	popped := pq.PopTask()
	if popped.ID != "earliest" {
		t.Errorf("Expected earliest task first, got %s", popped.ID)
	}

	popped = pq.PopTask()
	if popped.ID != "earlier" {
		t.Errorf("Expected earlier task second, got %s", popped.ID)
	}
}

func TestPriorityQueue_Peek(t *testing.T) {
	pq := NewPriorityQueue()

	// Peek on empty queue
	if pq.PeekTask() != nil {
		t.Error("Expected nil from empty queue")
	}

	task := &Task{ID: "test", Priority: PriorityHigh}
	pq.PushTask(task)

	// Peek should not remove
	peeked := pq.PeekTask()
	if peeked.ID != "test" {
		t.Errorf("Expected 'test', got %s", peeked.ID)
	}
	if pq.Size() != 1 {
		t.Errorf("Peek should not remove, size should be 1, got %d", pq.Size())
	}
}

func TestPriorityQueue_RemoveTask(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := &Task{ID: "keep", Priority: PriorityHigh}
	task2 := &Task{ID: "remove", Priority: PriorityNormal}

	pq.PushTask(task1)
	pq.PushTask(task2)

	removed := pq.RemoveTask("remove")
	if !removed {
		t.Error("Expected task to be removed")
	}

	if pq.Size() != 1 {
		t.Errorf("Expected 1 task remaining, got %d", pq.Size())
	}

	// Try to remove non-existent
	removed = pq.RemoveTask("nonexistent")
	if removed {
		t.Error("Should not remove non-existent task")
	}
}

func TestPriorityQueue_UpdatePriority(t *testing.T) {
	pq := NewPriorityQueue()

	task1 := &Task{ID: "task1", Priority: PriorityLow}
	task2 := &Task{ID: "task2", Priority: PriorityHigh}

	pq.PushTask(task1)
	pq.PushTask(task2)

	// task2 should be first
	if pq.PeekTask().ID != "task2" {
		t.Error("Expected task2 first initially")
	}

	// Update task1 to high priority
	updated := pq.UpdatePriority("task1", PriorityHigh)
	if !updated {
		t.Error("Expected update to succeed")
	}

	// Both are now high priority, but task1 was updated - order depends on heap
	// Just verify both tasks are still there
	if pq.Size() != 2 {
		t.Errorf("Expected 2 tasks, got %d", pq.Size())
	}
}

func TestPriorityQueue_GetReadyTasks(t *testing.T) {
	pq := NewPriorityQueue()

	now := time.Now()
	task1 := &Task{ID: "ready1", Priority: PriorityLow, NextRunAt: now.Add(-1 * time.Second), state: TaskStatePending}
	task2 := &Task{ID: "ready2", Priority: PriorityHigh, NextRunAt: now.Add(-2 * time.Second), state: TaskStatePending}
	task3 := &Task{ID: "future", Priority: PriorityHigh, NextRunAt: now.Add(1 * time.Hour), state: TaskStatePending}

	pq.PushTask(task1)
	pq.PushTask(task2)
	pq.PushTask(task3)

	ready := pq.GetReadyTasks(now)
	if len(ready) != 2 {
		t.Errorf("Expected 2 ready tasks, got %d", len(ready))
	}

	// First ready task should be highest priority
	if ready[0].ID != "ready2" {
		t.Errorf("Expected highest priority ready task first, got %s", ready[0].ID)
	}
}

func TestPriorityQueue_EmptyPop(t *testing.T) {
	pq := NewPriorityQueue()

	popped := pq.PopTask()
	if popped != nil {
		t.Error("Expected nil from empty queue")
	}
}
