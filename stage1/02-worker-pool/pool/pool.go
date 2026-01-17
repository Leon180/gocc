package pool

import (
	"context"
	"sync"
)

// Job represents a unit of work to be processed by a worker.
type Job struct {
	ID   int
	Data any // The actual work data
}

// Result represents the outcome of processing a job.
type Result struct {
	JobID  int
	Output any   // The result of processing
	Err    error // Any error that occurred
}

// ProcessFunc is the function signature for job processing.
// Each worker will call this function for every job it receives.
type ProcessFunc func(job Job) Result

// WorkerPool manages a fixed number of worker goroutines.
type WorkerPool struct {
	numWorkers int
	jobs       chan Job
	results    chan Result
	process    ProcessFunc
	wg         sync.WaitGroup
}

// New creates a new WorkerPool with the specified number of workers.
//
// Parameters:
//   - numWorkers: Number of worker goroutines to spawn
//   - jobBufferSize: Size of the job channel buffer
//   - process: Function to process each job
//
// Example:
//
//	pool := New(5, 100, func(job Job) Result {
//	    // Process the job
//	    return Result{JobID: job.ID, Output: "done"}
//	})
func New(numWorkers, jobBufferSize int, process ProcessFunc) *WorkerPool {
	return &WorkerPool{
		numWorkers: numWorkers,
		jobs:       make(chan Job, jobBufferSize),
		results:    make(chan Result, jobBufferSize),
		process:    process,
	}
}

// Start spawns the worker goroutines and begins processing jobs.
// Workers will continue running until the jobs channel is closed.
func (p *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

// worker is the goroutine that processes jobs.
// It reads from the jobs channel and writes results to the results channel.
func (p *WorkerPool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, stop processing
			return
		case job, ok := <-p.jobs:
			if !ok {
				// Jobs channel closed, worker exits
				return
			}
			// Process the job and send result
			result := p.process(job)
			p.results <- result
		}
	}
}

// Submit adds a job to the pool for processing.
// This will block if the job buffer is full.
func (p *WorkerPool) Submit(job Job) {
	p.jobs <- job
}

// Results returns the results channel for reading processed results.
func (p *WorkerPool) Results() <-chan Result {
	return p.results
}

// Close signals that no more jobs will be submitted.
// Workers will finish processing remaining jobs and exit.
func (p *WorkerPool) Close() {
	close(p.jobs)
}

// Wait blocks until all workers have finished processing.
// Should be called after Close() to ensure clean shutdown.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
	close(p.results)
}
