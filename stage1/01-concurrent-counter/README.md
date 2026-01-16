# Project 1: Concurrent Counter / Rate Limiter

## Overview

A thread-safe counter implementation demonstrating race condition detection and resolution using Go's synchronization primitives.

## Learning Objectives

- [ ] Understand what race conditions are and how to detect them
- [ ] Use `sync.Mutex` to protect shared resources
- [ ] Use `sync/atomic` for lock-free operations
- [ ] Compare performance between different approaches
- [ ] Implement rate limiting algorithms

## Key Concepts

### Race Condition
A race condition occurs when multiple goroutines access shared data concurrently, and at least one of them modifies the data. This leads to unpredictable behavior.

### Mutex (Mutual Exclusion)
`sync.Mutex` ensures only one goroutine can access the critical section at a time.

### Atomic Operations
`sync/atomic` provides low-level atomic memory operations for simple counters and flags.

## Project Structure

```
01-concurrent-counter/
├── README.md                    # This file
├── counter/
│   ├── unsafe_counter.go        # Step 1.1: Basic counter with race condition
│   ├── unsafe_counter_test.go   # Tests to detect race condition
│   ├── mutex_counter.go         # Step 1.2: Mutex-based solution
│   ├── mutex_counter_test.go    # Tests for mutex counter
│   ├── atomic_counter.go        # Step 1.3: Atomic-based solution
│   ├── atomic_counter_test.go   # Tests for atomic counter
│   └── benchmark_test.go        # Benchmark comparison
├── ratelimiter/
│   ├── token_bucket.go          # Step 1.4: Token Bucket algorithm
│   ├── leaky_bucket.go          # Step 1.4: Leaky Bucket algorithm
│   ├── sliding_window.go        # Step 1.4: Sliding Window algorithm
│   └── ratelimiter_test.go      # Tests for rate limiters
└── examples/
    └── main.go                  # Usage examples
```

## Usage

```bash
# Run tests
go test -v ./...

# Run with race detector
go test -race -v ./...

# Run benchmarks
go test -bench=. -benchmem ./counter/
```

## Progress

### Step 1.1: Basic Counter (Unsafe)
- [ ] Implement Counter struct with Increment, Decrement, GetValue, Reset
- [ ] Write concurrent tests
- [ ] Run `go test -race` and observe the data race

### Step 1.2: Fix with Mutex
- [ ] Add sync.Mutex to Counter
- [ ] Use defer for unlocking
- [ ] Verify no race conditions

### Step 1.3: Optimize with Atomic
- [ ] Create AtomicCounter using sync/atomic
- [ ] Write benchmark comparing Mutex vs Atomic
- [ ] Analyze results

### Step 1.4: Rate Limiter
- [ ] Implement Token Bucket
- [ ] Implement Leaky Bucket
- [ ] Implement Sliding Window
- [ ] Test rate limiting behavior

## Challenges & Solutions

<!-- Document challenges you face and how you solve them -->

## References

- [Go sync package](https://pkg.go.dev/sync)
- [Go sync/atomic package](https://pkg.go.dev/sync/atomic)
- [Race Detector](https://go.dev/doc/articles/race_detector)
