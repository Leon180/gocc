package ratelimiter

import (
	"sync"
	"time"
)

// TokenBucket implements the Token Bucket rate limiting algorithm.
//
// How it works:
// - Tokens are added to the bucket at a fixed rate (refillRate per refillInterval)
// - Each request consumes one token
// - If no tokens available, request is rejected
// - Bucket has a maximum capacity (burst limit)
//
// Use cases:
// - API rate limiting
// - Network traffic shaping
// - Burst-friendly rate limiting (allows short bursts)
type TokenBucket struct {
	mu             sync.Mutex
	tokens         float64       // Current number of tokens
	maxTokens      float64       // Maximum bucket capacity (burst limit)
	refillRate     float64       // Tokens added per refill interval
	refillInterval time.Duration // How often to refill
	lastRefill     time.Time     // Last refill timestamp
}

// NewTokenBucket creates a new Token Bucket rate limiter.
//
// Parameters:
//   - maxTokens: Maximum tokens the bucket can hold (burst capacity)
//   - refillRate: Number of tokens to add per refillInterval
//   - refillInterval: How often tokens are added
//
// Example:
//
//	// Allow 10 requests per second with burst of 20
//	limiter := NewTokenBucket(20, 10, time.Second)
func NewTokenBucket(maxTokens, refillRate float64, refillInterval time.Duration) *TokenBucket {
	return &TokenBucket{
		tokens:         maxTokens, // Start with full bucket
		maxTokens:      maxTokens,
		refillRate:     refillRate,
		refillInterval: refillInterval,
		lastRefill:     time.Now(),
	}
}

// Allow checks if a request is allowed and consumes a token if so.
// Returns true if the request is allowed, false if rate limited.
func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

// AllowN checks if n requests are allowed and consumes n tokens if so.
// Returns true if all n requests are allowed, false otherwise.
func (tb *TokenBucket) AllowN(n int) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= float64(n) {
		tb.tokens -= float64(n)
		return true
	}
	return false
}

// Tokens returns the current number of available tokens.
func (tb *TokenBucket) Tokens() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	return tb.tokens
}

// refill adds tokens based on elapsed time since last refill.
// Must be called with mutex held.
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)

	// Calculate tokens to add based on elapsed time
	tokensToAdd := (elapsed.Seconds() / tb.refillInterval.Seconds()) * tb.refillRate

	if tokensToAdd > 0 {
		tb.tokens += tokensToAdd
		if tb.tokens > tb.maxTokens {
			tb.tokens = tb.maxTokens
		}
		tb.lastRefill = now
	}
}
