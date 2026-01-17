package ratelimiter

import (
	"sync"
	"time"
)

// LeakyBucket implements the Leaky Bucket rate limiting algorithm.
//
// How it works:
// - Requests fill a bucket
// - Bucket "leaks" (processes requests) at a constant rate
// - If bucket is full, new requests are rejected
// - Provides smooth, constant output rate
//
// Difference from Token Bucket:
// - Token Bucket allows bursts, Leaky Bucket smooths traffic
// - Token Bucket: tokens accumulate; Leaky Bucket: requests queue
//
// Use cases:
// - Traffic shaping (smooth output)
// - Preventing burst traffic
// - Network congestion control
type LeakyBucket struct {
	mu           sync.Mutex
	capacity     float64       // Maximum requests the bucket can hold
	remaining    float64       // Current space in bucket
	leakRate     float64       // Time between each leak (request processed)
	leakInterval time.Duration // Time interval between each leak
	lastLeakTime time.Time     // Last time bucket leaked
}

// NewLeakyBucket creates a new Leaky Bucket rate limiter.
//
// Parameters:
//   - capacity: Maximum requests that can be queued
//   - leakRate: Time interval between processing each request
//
// Example:
//
//	// Process 1 request every 100ms, queue up to 10
//	limiter := NewLeakyBucket(10, 100*time.Millisecond)
func NewLeakyBucket(capacity, leakRate float64, leakInterval time.Duration) *LeakyBucket {
	return &LeakyBucket{
		capacity:     capacity,
		remaining:    0, // Start empty (full capacity remaining)
		leakRate:     leakRate,
		leakInterval: leakInterval,
		lastLeakTime: time.Now(),
	}
}

// Allow checks if a request can be added to the bucket.
// Returns true if request is accepted, false if bucket is full.
func (lb *LeakyBucket) Allow() bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.leak()

	if (lb.remaining + 1) <= lb.capacity {
		lb.remaining++
		return true
	}
	return false
}

func (lb *LeakyBucket) AllowN(n int) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.leak()

	if (lb.remaining + float64(n)) <= lb.capacity {
		lb.remaining += float64(n)
		return true
	}
	return false
}

// Remaining returns the current remaining capacity in the bucket.
func (lb *LeakyBucket) Remaining() float64 {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.leak()
	return lb.remaining
}

// leak processes requests that have "leaked" out based on elapsed time.
// Must be called with mutex held.
func (lb *LeakyBucket) leak() {
	now := time.Now()
	elapsed := now.Sub(lb.lastLeakTime)

	// Calculate how many requests have leaked out
	leaks := (elapsed.Seconds() / lb.leakInterval.Seconds()) * lb.leakRate

	if leaks > 0 {
		lb.remaining -= leaks
		if lb.remaining < 0 {
			lb.remaining = 0
		}
		lb.lastLeakTime = now
	}
}
