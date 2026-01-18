package crawler

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"net/http"
	"time"
)

// RetryConfig configures retry behavior.
type RetryConfig struct {
	MaxRetries     int           // Maximum number of retry attempts
	InitialBackoff time.Duration // Initial backoff duration
	MaxBackoff     time.Duration // Maximum backoff duration
	Multiplier     float64       // Backoff multiplier (e.g., 2.0 for exponential)
	Jitter         float64       // Random jitter factor (0.0-1.0)
}

// DefaultRetryConfig returns sensible retry defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2.0,
		Jitter:         0.1,
	}
}

// RetryableError wraps an error that may be retried.
type RetryableError struct {
	Err       error
	Retryable bool
}

func (e *RetryableError) Error() string {
	return e.Err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable checks if an error is retryable.
// Retryable: timeouts, 5xx errors, connection errors
// Not retryable: 4xx errors, parse errors
func IsRetryable(err error, statusCode int) bool {
	if err != nil {
		// Timeouts and connection errors are retryable
		return true
	}

	// Server errors (5xx) are retryable
	if statusCode >= 500 && statusCode < 600 {
		return true
	}

	// Rate limiting (429) is retryable
	if statusCode == http.StatusTooManyRequests {
		return true
	}

	return false
}

// RetryWithBackoff executes a function with exponential backoff retry.
//
// Example:
//
//	result, err := RetryWithBackoff(ctx, config, func() (Result, error) {
//	    return fetchURL(url)
//	})
func RetryWithBackoff[T any](ctx context.Context, config RetryConfig, fn func() (T, error)) (T, error) {
	var result T
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		result, lastErr = fn()
		if lastErr == nil {
			return result, nil
		}

		// Check if error is retryable
		var retryErr *RetryableError
		if errors.As(lastErr, &retryErr) && !retryErr.Retryable {
			return result, lastErr
		}

		// Don't wait after last attempt
		if attempt == config.MaxRetries {
			break
		}

		// Calculate backoff with jitter
		backoff := calculateBackoff(config, attempt)

		// Wait with context awareness
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		case <-time.After(backoff):
		}
	}

	return result, lastErr
}

// calculateBackoff computes backoff duration with jitter.
func calculateBackoff(config RetryConfig, attempt int) time.Duration {
	// Exponential backoff: initial * multiplier^attempt
	backoff := float64(config.InitialBackoff) * math.Pow(config.Multiplier, float64(attempt))

	// Cap at max backoff
	if backoff > float64(config.MaxBackoff) {
		backoff = float64(config.MaxBackoff)
	}

	// Add jitter: +/- jitter%
	if config.Jitter > 0 {
		jitter := backoff * config.Jitter * (2*rand.Float64() - 1)
		backoff += jitter
	}

	return time.Duration(backoff)
}

// RateLimiter provides request rate limiting for polite crawling.
type RateLimiter struct {
	interval time.Duration
	lastReq  time.Time
}

// NewRateLimiter creates a rate limiter with minimum interval between requests.
func NewRateLimiter(interval time.Duration) *RateLimiter {
	return &RateLimiter{
		interval: interval,
	}
}

// Wait blocks until it's OK to make another request.
func (r *RateLimiter) Wait(ctx context.Context) error {
	elapsed := time.Since(r.lastReq)
	if elapsed < r.interval {
		wait := r.interval - elapsed

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	r.lastReq = time.Now()
	return nil
}
