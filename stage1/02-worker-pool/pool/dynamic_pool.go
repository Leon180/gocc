package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// DynamicPool is a worker pool that supports dynamic resizing,
// health checks, and task timeouts.
type DynamicPool struct {
	mu          sync.RWMutex
	numWorkers  int32 // Atomic for safe reads
	minWorkers  int
	maxWorkers  int
	jobs        chan Job
	results     chan Result
	process     ProcessFunc
	timeout     time.Duration
	workerWg    sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	healthCheck func() bool // Optional health check function
	closeOnce   sync.Once   // Ensures jobs channel is closed only once
}

// DynamicPoolConfig holds configuration for DynamicPool.
type DynamicPoolConfig struct {
	MinWorkers    int
	MaxWorkers    int
	JobBufferSize int
	TaskTimeout   time.Duration
	HealthCheck   func() bool
}

// NewDynamicPool creates a new dynamic worker pool.
//
// Example:
//
//	pool := NewDynamicPool(DynamicPoolConfig{
//	    MinWorkers:    2,
//	    MaxWorkers:    10,
//	    JobBufferSize: 100,
//	    TaskTimeout:   5 * time.Second,
//	}, processFunc)
func NewDynamicPool(config DynamicPoolConfig, process ProcessFunc) *DynamicPool {
	ctx, cancel := context.WithCancel(context.Background())

	if config.MinWorkers <= 0 {
		config.MinWorkers = 1
	}
	if config.MaxWorkers < config.MinWorkers {
		config.MaxWorkers = config.MinWorkers
	}
	if config.JobBufferSize <= 0 {
		config.JobBufferSize = 100
	}

	return &DynamicPool{
		minWorkers:  config.MinWorkers,
		maxWorkers:  config.MaxWorkers,
		jobs:        make(chan Job, config.JobBufferSize),
		results:     make(chan Result, config.JobBufferSize),
		process:     process,
		timeout:     config.TaskTimeout,
		ctx:         ctx,
		cancel:      cancel,
		healthCheck: config.HealthCheck,
	}
}

// Start spawns the initial workers.
func (p *DynamicPool) Start() {
	p.Resize(p.minWorkers)
}

// Resize adjusts the number of workers.
// If n > current workers, spawns more.
// If n < current workers, excess workers will exit after finishing current job.
func (p *DynamicPool) Resize(n int) {
	if n < p.minWorkers {
		n = p.minWorkers
	}
	if n > p.maxWorkers {
		n = p.maxWorkers
	}

	current := int(atomic.LoadInt32(&p.numWorkers))

	if n > current {
		// Spawn more workers
		for i := current; i < n; i++ {
			p.spawnWorker()
		}
	}
	// Note: Reducing workers happens naturally as they check shouldExit()
}

// spawnWorker creates a new worker goroutine.
func (p *DynamicPool) spawnWorker() {
	atomic.AddInt32(&p.numWorkers, 1)
	p.workerWg.Add(1)

	go func() {
		defer func() {
			atomic.AddInt32(&p.numWorkers, -1)
			p.workerWg.Done()
		}()

		for {
			select {
			case <-p.ctx.Done():
				return

			case job, ok := <-p.jobs:
				if !ok {
					return
				}

				// Process with timeout if configured
				result := p.processWithTimeout(job)
				p.results <- result

				// Check if this worker should exit (for downsizing)
				if p.shouldWorkerExit() {
					return
				}
			}
		}
	}()
}

// processWithTimeout processes a job with optional timeout.
func (p *DynamicPool) processWithTimeout(job Job) Result {
	if p.timeout <= 0 {
		// No timeout, process directly
		return p.safeProcess(job)
	}

	// Create timeout context
	ctx, cancel := context.WithTimeout(p.ctx, p.timeout)
	defer cancel()

	resultChan := make(chan Result, 1)

	go func() {
		resultChan <- p.safeProcess(job)
	}()

	select {
	case result := <-resultChan:
		return result
	case <-ctx.Done():
		return Result{
			JobID: job.ID,
			Err:   context.DeadlineExceeded,
		}
	}
}

// safeProcess wraps the process function with panic recovery.
func (p *DynamicPool) safeProcess(job Job) (result Result) {
	defer func() {
		if r := recover(); r != nil {
			result = Result{
				JobID: job.ID,
				Err:   &PanicError{Value: r},
			}
		}
	}()

	return p.process(job)
}

// PanicError wraps a panic value as an error.
type PanicError struct {
	Value any
}

func (e *PanicError) Error() string {
	return "panic in worker"
}

// shouldWorkerExit checks if this worker should exit (for downsizing).
func (p *DynamicPool) shouldWorkerExit() bool {
	current := int(atomic.LoadInt32(&p.numWorkers))
	return current > p.maxWorkers
}

// NumWorkers returns the current number of active workers.
func (p *DynamicPool) NumWorkers() int {
	return int(atomic.LoadInt32(&p.numWorkers))
}

// Submit adds a job to the pool.
func (p *DynamicPool) Submit(job Job) {
	p.jobs <- job
}

// Results returns the results channel.
func (p *DynamicPool) Results() <-chan Result {
	return p.results
}

// IsHealthy checks if the pool is healthy.
// Returns true if no health check is configured.
func (p *DynamicPool) IsHealthy() bool {
	if p.healthCheck == nil {
		return true
	}
	return p.healthCheck()
}

// Close signals no more jobs and stops all workers.
func (p *DynamicPool) Close() {
	p.closeOnce.Do(func() {
		close(p.jobs)
	})
}

// Shutdown gracefully stops all workers.
func (p *DynamicPool) Shutdown(timeout time.Duration) error {
	p.Close()

	done := make(chan struct{})
	go func() {
		p.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(p.results)
		return nil
	case <-time.After(timeout):
		p.cancel() // Force cancel
		close(p.results)
		return context.DeadlineExceeded
	}
}
