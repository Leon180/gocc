package scheduler

import (
	"container/heap"
	"sync"
	"time"
)

// PriorityQueue implements a priority queue for tasks.
// Tasks are ordered by: Priority (higher first), then NextRunAt (earlier first).
//
// Workflow:
//
//	Push Task ──► [Heap] ──► Pop (highest priority, earliest time)
//	                │
//	                ▼
//	        Priority 2 (High)    ← Popped first
//	        Priority 1 (Normal)
//	        Priority 0 (Low)     ← Popped last
type PriorityQueue struct {
	items []*Task
	mu    sync.RWMutex
}

// NewPriorityQueue creates a new priority queue.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: make([]*Task, 0),
	}
	heap.Init(pq)
	return pq
}

// Len returns the number of items in the queue.
func (pq *PriorityQueue) Len() int {
	return len(pq.items)
}

// Less compares two tasks for ordering.
// Higher priority comes first; if equal, earlier NextRunAt comes first.
func (pq *PriorityQueue) Less(i, j int) bool {
	// Higher priority first
	if pq.items[i].Priority != pq.items[j].Priority {
		return pq.items[i].Priority > pq.items[j].Priority
	}
	// Earlier time first
	return pq.items[i].NextRunAt.Before(pq.items[j].NextRunAt)
}

// Swap swaps two elements.
func (pq *PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push adds an item to the queue. Use PushTask for thread-safety.
func (pq *PriorityQueue) Push(x any) {
	pq.items = append(pq.items, x.(*Task))
}

// Pop removes and returns the highest priority item. Use PopTask for thread-safety.
func (pq *PriorityQueue) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	pq.items = old[0 : n-1]
	return item
}

// PushTask adds a task to the queue (thread-safe).
func (pq *PriorityQueue) PushTask(task *Task) {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	heap.Push(pq, task)
}

// PopTask removes and returns the highest priority task (thread-safe).
// Returns nil if queue is empty.
func (pq *PriorityQueue) PopTask() *Task {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	if pq.Len() == 0 {
		return nil
	}
	return heap.Pop(pq).(*Task)
}

// PeekTask returns the highest priority task without removing it.
func (pq *PriorityQueue) PeekTask() *Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	if pq.Len() == 0 {
		return nil
	}
	return pq.items[0]
}

// Size returns the number of tasks in the queue (thread-safe).
func (pq *PriorityQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.Len()
}

// GetReadyTasks returns all tasks that are ready to run (NextRunAt <= now),
// sorted by priority.
func (pq *PriorityQueue) GetReadyTasks(now time.Time) []*Task {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	var ready []*Task
	for _, task := range pq.items {
		if !task.NextRunAt.After(now) && task.State() == TaskStatePending {
			ready = append(ready, task)
		}
	}

	// Already sorted by heap property (higher priority first)
	return ready
}

// RemoveTask removes a specific task from the queue.
func (pq *PriorityQueue) RemoveTask(id TaskID) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for i, task := range pq.items {
		if task.ID == id {
			heap.Remove(pq, i)
			return true
		}
	}
	return false
}

// UpdatePriority updates a task's priority and re-heapifies.
func (pq *PriorityQueue) UpdatePriority(id TaskID, priority TaskPriority) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for i, task := range pq.items {
		if task.ID == id {
			task.mu.Lock()
			task.Priority = priority
			task.mu.Unlock()
			heap.Fix(pq, i)
			return true
		}
	}
	return false
}
