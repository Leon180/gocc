package ratelimiter

import (
	"sync"
	"time"
)

// SlidingWindowLog implements the Sliding Window Log rate limiting algorithm.
//
// How it works:
// - Maintains a log of all request timestamps
// - When checking, removes expired entries outside the window
// - Counts remaining entries to determine if under limit
//
// Pros:
// - Most accurate rate limiting
// - No boundary issues like fixed windows
//
// Cons:
// - Higher memory usage (stores all timestamps)
// - Cleanup overhead
//
// Use cases:
// - Accurate per-user API rate limiting
// - When precision matters more than memory
type SlidingWindowLog struct {
	mu         sync.Mutex
	timestamps []time.Time   // Log of request timestamps
	limit      int           // Maximum requests per window
	window     time.Duration // Time window size
}

// NewSlidingWindowLog creates a new Sliding Window Log rate limiter.
//
// Parameters:
//   - limit: Maximum requests allowed in the window
//   - window: Duration of the sliding window
//
// Example:
//
//	// Allow 100 requests per minute
//	limiter := NewSlidingWindowLog(100, time.Minute)
func NewSlidingWindowLog(limit int, window time.Duration) *SlidingWindowLog {
	return &SlidingWindowLog{
		timestamps: make([]time.Time, 0),
		limit:      limit,
		window:     window,
	}
}

// Allow checks if a request is allowed within the rate limit.
// Returns true if allowed, false if rate limited.
func (sw *SlidingWindowLog) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.cleanup(now)

	if len(sw.timestamps) < sw.limit {
		sw.timestamps = append(sw.timestamps, now)
		return true
	}
	return false
}

// AllowN checks if n requests are allowed within the rate limit.
// Returns true if allowed, false if rate limited.
func (sw *SlidingWindowLog) AllowN(n int) bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.cleanup(now)

	if len(sw.timestamps)+n <= sw.limit {
		for range n {
			sw.timestamps = append(sw.timestamps, now)
		}
		return true
	}
	return false
}

// Count returns the current number of requests in the window.
func (sw *SlidingWindowLog) Count() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.cleanup(time.Now())
	return len(sw.timestamps)
}

// cleanup removes timestamps outside the current window.
// Must be called with mutex held.
func (sw *SlidingWindowLog) cleanup(now time.Time) {
	windowStart := now.Add(-sw.window)

	// Find first timestamp within window
	validIndex := 0
	for i, ts := range sw.timestamps {
		if ts.After(windowStart) {
			validIndex = i
			break
		}
		// If we reach here on last iteration, all timestamps are expired
		if i == len(sw.timestamps)-1 {
			validIndex = len(sw.timestamps)
		}
	}

	// Remove expired timestamps
	if validIndex > 0 {
		sw.timestamps = sw.timestamps[validIndex:]
	}
}

// SlidingWindowCounter implements the Sliding Window Counter algorithm.
//
// How it works:
// - Divides time into fixed windows
// - Weights previous window count based on overlap
// - More memory efficient than log-based approach
//
// Formula:
//
//	count = previousWindow * overlapRatio + currentWindow
//
// Use cases:
// - Memory-efficient rate limiting
// - Approximate but good-enough accuracy
type SlidingWindowCounter struct {
	mu            sync.Mutex
	limit         int           // Maximum requests per window
	window        time.Duration // Window duration
	currentCount  int           // Requests in current window
	previousCount int           // Requests in previous window
	windowStart   time.Time     // Start of current window
}

// NewSlidingWindowCounter creates a new Sliding Window Counter rate limiter.
//
// Parameters:
//   - limit: Maximum requests allowed per window
//   - window: Duration of each window
//
// Example:
//
//	// Allow 100 requests per minute
//	limiter := NewSlidingWindowCounter(100, time.Minute)
func NewSlidingWindowCounter(limit int, window time.Duration) *SlidingWindowCounter {
	return &SlidingWindowCounter{
		limit:       limit,
		window:      window,
		windowStart: time.Now(),
	}
}

// Allow checks if a request is allowed within the rate limit.
// Returns true if allowed, false if rate limited.
func (sw *SlidingWindowCounter) Allow() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.advanceWindow(now)

	// Calculate weighted count
	elapsed := now.Sub(sw.windowStart)
	weight := float64(sw.window-elapsed) / float64(sw.window)
	count := float64(sw.previousCount)*weight + float64(sw.currentCount)

	if count < float64(sw.limit) {
		sw.currentCount++
		return true
	}
	return false
}

// advanceWindow moves to new windows if needed.
// Must be called with mutex held.
func (sw *SlidingWindowCounter) advanceWindow(now time.Time) {
	elapsed := now.Sub(sw.windowStart)

	if elapsed >= sw.window*2 {
		// More than 2 windows passed, reset everything
		sw.previousCount = 0
		sw.currentCount = 0
		sw.windowStart = now
	} else if elapsed >= sw.window {
		// Move to next window
		sw.previousCount = sw.currentCount
		sw.currentCount = 0
		sw.windowStart = sw.windowStart.Add(sw.window)
	}
}
