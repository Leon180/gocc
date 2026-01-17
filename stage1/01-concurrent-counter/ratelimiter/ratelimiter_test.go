package ratelimiter

import (
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Token Bucket Tests
// =============================================================================

func TestTokenBucket_Basic(t *testing.T) {
	// 10 tokens max, refill 10 per second
	tb := NewTokenBucket(10, 10, time.Second)

	// Should allow first 10 requests
	for i := 0; i < 10; i++ {
		if !tb.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 11th request should be denied
	if tb.Allow() {
		t.Error("Request 11 should be denied")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	// 5 tokens max, refill 10 per second (fast refill for testing)
	tb := NewTokenBucket(5, 10, time.Second)

	// Use all tokens
	for i := 0; i < 5; i++ {
		tb.Allow()
	}

	// Wait for refill (100ms should add ~1 token)
	time.Sleep(150 * time.Millisecond)

	// Should be able to make 1 request now
	if !tb.Allow() {
		t.Error("Should allow request after refill")
	}
}

func TestTokenBucket_AllowN(t *testing.T) {
	tb := NewTokenBucket(10, 10, time.Second)

	// Should allow 5 at once
	if !tb.AllowN(5) {
		t.Error("Should allow 5 requests")
	}

	// Should allow another 5
	if !tb.AllowN(5) {
		t.Error("Should allow another 5 requests")
	}

	// Should deny 1 more
	if tb.AllowN(1) {
		t.Error("Should deny request when bucket empty")
	}
}

func TestTokenBucket_Concurrent(t *testing.T) {
	tb := NewTokenBucket(100, 100, time.Second)

	var wg sync.WaitGroup
	allowed := make(chan bool, 200)

	// Launch 200 goroutines, each trying to get 1 token
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- tb.Allow()
		}()
	}

	wg.Wait()
	close(allowed)

	// Count allowed requests
	count := 0
	for a := range allowed {
		if a {
			count++
		}
	}

	// Should be exactly 100 (initial bucket size)
	if count != 100 {
		t.Errorf("Expected 100 allowed, got %d", count)
	}
}

// =============================================================================
// Leaky Bucket Tests
// =============================================================================

func TestLeakyBucket_Basic(t *testing.T) {
	// Capacity 5, leak 1 every 100ms
	lb := NewLeakyBucket(5, 100, time.Second)

	// Should allow first 5 requests
	for i := 0; i < 5; i++ {
		if !lb.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied (bucket full)
	if lb.Allow() {
		t.Error("Request 6 should be denied")
	}
}

func TestLeakyBucket_Leak(t *testing.T) {
	// Capacity 5, leak 10 per second (so 1 per 100ms)
	lb := NewLeakyBucket(5, 10, time.Second)

	// Fill bucket with 5 requests
	for i := 0; i < 5; i++ {
		lb.Allow()
	}

	// Bucket is full, should deny
	if lb.Allow() {
		t.Error("Should deny when bucket is full")
	}

	// Wait for ~2 requests to leak (200ms at 10/sec = 2 leaked)
	time.Sleep(250 * time.Millisecond)

	// Should be able to make 2 more requests
	if !lb.Allow() {
		t.Error("Should allow after leak")
	}
	if !lb.Allow() {
		t.Error("Should allow second after leak")
	}

	// Third should be denied (bucket full again)
	if lb.Allow() {
		t.Error("Should deny third request")
	}
}

func TestLeakyBucket_Concurrent(t *testing.T) {
	lb := NewLeakyBucket(50, 10, time.Second)

	var wg sync.WaitGroup
	allowed := make(chan bool, 100)

	// Launch 100 goroutines
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- lb.Allow()
		}()
	}

	wg.Wait()
	close(allowed)

	// Count allowed
	count := 0
	for a := range allowed {
		if a {
			count++
		}
	}

	// Should be exactly 50 (capacity)
	if count != 50 {
		t.Errorf("Expected 50 allowed, got %d", count)
	}
}

// =============================================================================
// Sliding Window Log Tests
// =============================================================================

func TestSlidingWindowLog_Basic(t *testing.T) {
	// 5 requests per 100ms window
	sw := NewSlidingWindowLog(5, 100*time.Millisecond)

	// Should allow first 5 requests
	for i := 0; i < 5; i++ {
		if !sw.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied
	if sw.Allow() {
		t.Error("Request 6 should be denied")
	}
}

func TestSlidingWindowLog_WindowSlide(t *testing.T) {
	// 3 requests per 50ms window
	sw := NewSlidingWindowLog(3, 50*time.Millisecond)

	// Use all 3
	for i := 0; i < 3; i++ {
		sw.Allow()
	}

	// Should be denied
	if sw.Allow() {
		t.Error("Should be denied when at limit")
	}

	// Wait for window to slide
	time.Sleep(60 * time.Millisecond)

	// Should allow again
	if !sw.Allow() {
		t.Error("Should allow after window slides")
	}
}

func TestSlidingWindowLog_Concurrent(t *testing.T) {
	sw := NewSlidingWindowLog(50, time.Second)

	var wg sync.WaitGroup
	allowed := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- sw.Allow()
		}()
	}

	wg.Wait()
	close(allowed)

	count := 0
	for a := range allowed {
		if a {
			count++
		}
	}

	if count != 50 {
		t.Errorf("Expected 50 allowed, got %d", count)
	}
}

// =============================================================================
// Sliding Window Counter Tests
// =============================================================================

func TestSlidingWindowCounter_Basic(t *testing.T) {
	sw := NewSlidingWindowCounter(5, 100*time.Millisecond)

	// Should allow first 5 requests
	for i := 0; i < 5; i++ {
		if !sw.Allow() {
			t.Errorf("Request %d should be allowed", i+1)
		}
	}

	// 6th request should be denied
	if sw.Allow() {
		t.Error("Request 6 should be denied")
	}
}

func TestSlidingWindowCounter_Concurrent(t *testing.T) {
	sw := NewSlidingWindowCounter(50, time.Second)

	var wg sync.WaitGroup
	allowed := make(chan bool, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed <- sw.Allow()
		}()
	}

	wg.Wait()
	close(allowed)

	count := 0
	for a := range allowed {
		if a {
			count++
		}
	}

	// Should be around 50 (may vary slightly due to floating point)
	if count < 45 || count > 55 {
		t.Errorf("Expected ~50 allowed, got %d", count)
	}
}
