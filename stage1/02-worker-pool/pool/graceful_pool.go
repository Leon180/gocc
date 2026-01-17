package pool

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// GracefulPool is a production-ready worker pool with
// graceful shutdown, signal handling, and drain mode.
type GracefulPool struct {
	numWorkers  int32
	jobs        chan Job
	results     chan Result
	process     ProcessFunc
	workerWg    sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closeOnce   sync.Once
	resultsOnce sync.Once // Ensures results channel is closed only once
	state       int32     // 0=running, 1=draining, 2=stopped
	onShutdown  []func()
}

// PoolState represents the current state of the pool.

const (
	StateRunning  int32 = 0
	StateDraining int32 = 1
	StateStopped  int32 = 2
)

// NewGracefulPool creates a pool with graceful shutdown support.
func NewGracefulPool(numWorkers, bufferSize int, process ProcessFunc) *GracefulPool {
	ctx, cancel := context.WithCancel(context.Background())

	return &GracefulPool{
		jobs:    make(chan Job, bufferSize),
		results: make(chan Result, bufferSize),
		process: process,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start spawns workers and optionally sets up signal handling.
func (p *GracefulPool) Start(numWorkers int) {
	for range numWorkers {
		p.spawnWorker()
	}
}

// StartWithSignalHandler starts the pool and handles OS signals.
// It will gracefully shutdown on SIGINT or SIGTERM.
func (p *GracefulPool) StartWithSignalHandler(numWorkers int, shutdownTimeout time.Duration) {
	p.Start(numWorkers)

	// Set up signal handling in a goroutine
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigChan
		fmt.Printf("\nReceived signal: %v, initiating graceful shutdown...\n", sig)

		if err := p.GracefulShutdown(shutdownTimeout); err != nil {
			fmt.Printf("Shutdown error: %v\n", err)
		} else {
			fmt.Println("Graceful shutdown completed")
		}
	}()
}

func (p *GracefulPool) spawnWorker() {
	atomic.AddInt32(&p.numWorkers, 1)
	p.workerWg.Add(1)

	go func() {
		defer func() {
			// Recover from any panic (e.g., send on closed channel during shutdown)
			recover()
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
				result := p.safeProcess(job)

				// Try to send result, but don't block if shutting down
				select {
				case p.results <- result:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}()
}

func (p *GracefulPool) safeProcess(job Job) (result Result) {
	defer func() {
		if r := recover(); r != nil {
			result = Result{
				JobID: job.ID,
				Err:   fmt.Errorf("panic: %v", r),
			}
		}
	}()
	return p.process(job)
}

// Submit adds a job. Returns false if pool is draining/stopped.
func (p *GracefulPool) Submit(job Job) bool {
	state := atomic.LoadInt32(&p.state)
	if state != StateRunning {
		return false
	}

	select {
	case p.jobs <- job:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// Results returns the results channel.
func (p *GracefulPool) Results() <-chan Result {
	return p.results
}

// State returns the current pool state.
func (p *GracefulPool) State() int32 {
	return atomic.LoadInt32(&p.state)
}

// NumWorkers returns active worker count.
func (p *GracefulPool) NumWorkers() int {
	return int(atomic.LoadInt32(&p.numWorkers))
}

// OnShutdown registers a callback to run during shutdown.
func (p *GracefulPool) OnShutdown(fn func()) {
	p.onShutdown = append(p.onShutdown, fn)
}

// Drain stops accepting new jobs but processes remaining ones.
func (p *GracefulPool) Drain() {
	atomic.StoreInt32(&p.state, StateDraining)
	p.closeOnce.Do(func() {
		close(p.jobs)
	})
}

// GracefulShutdown performs a graceful shutdown with timeout.
//
// 1. Stop accepting new jobs (drain mode)
// 2. Wait for workers to finish current jobs
// 3. Run shutdown callbacks
// 4. Force cancel if timeout exceeded
func (p *GracefulPool) GracefulShutdown(timeout time.Duration) error {
	// Phase 1: Enter drain mode
	p.Drain()

	// Phase 2: Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		p.workerWg.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
		// Workers finished cleanly
	case <-time.After(timeout):
		// Timeout - force cancel
		p.cancel()
		err = context.DeadlineExceeded
	}

	// Phase 3: Run shutdown callbacks
	for _, fn := range p.onShutdown {
		fn()
	}

	// Phase 4: Mark as stopped
	atomic.StoreInt32(&p.state, StateStopped)

	// Close results channel safely
	p.resultsOnce.Do(func() {
		close(p.results)
	})

	return err
}

// ForceShutdown immediately cancels all workers.
func (p *GracefulPool) ForceShutdown() {
	atomic.StoreInt32(&p.state, StateStopped)
	p.cancel() // Signal all workers to stop
	p.closeOnce.Do(func() {
		close(p.jobs)
	})

	// Wait for workers to exit before closing results
	p.workerWg.Wait()
	p.resultsOnce.Do(func() {
		close(p.results)
	})
}
