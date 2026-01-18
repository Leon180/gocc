package crawler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryWithBackoff_Success(t *testing.T) {
	config := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
	}

	attempts := 0
	result, err := RetryWithBackoff(context.Background(), config, func() (string, error) {
		attempts++
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != "success" {
		t.Errorf("Expected 'success', got %s", result)
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt, got %d", attempts)
	}
}

func TestRetryWithBackoff_SuccessAfterRetries(t *testing.T) {
	config := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
	}

	attempts := 0
	result, err := RetryWithBackoff(context.Background(), config, func() (string, error) {
		attempts++
		if attempts < 3 {
			return "", errors.New("temporary error")
		}
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result != "success" {
		t.Errorf("Expected 'success', got %s", result)
	}
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestRetryWithBackoff_MaxRetriesExceeded(t *testing.T) {
	config := RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
	}

	attempts := 0
	_, err := RetryWithBackoff(context.Background(), config, func() (string, error) {
		attempts++
		return "", errors.New("always fails")
	})

	if err == nil {
		t.Error("Expected error after max retries")
	}
	// Initial attempt + 2 retries = 3 total
	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}
}

func TestRetryWithBackoff_ContextCancellation(t *testing.T) {
	config := RetryConfig{
		MaxRetries:     5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2.0,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := RetryWithBackoff(ctx, config, func() (string, error) {
		return "", errors.New("always fails")
	})
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context deadline exceeded, got %v", err)
	}

	// Should cancel before completing all retries
	if elapsed > 200*time.Millisecond {
		t.Errorf("Should have cancelled quickly, took %v", elapsed)
	}
}

func TestRetryWithBackoff_NonRetryableError(t *testing.T) {
	config := RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
	}

	attempts := 0
	_, err := RetryWithBackoff(context.Background(), config, func() (string, error) {
		attempts++
		return "", &RetryableError{
			Err:       errors.New("not retryable"),
			Retryable: false,
		}
	})

	if err == nil {
		t.Error("Expected error")
	}
	if attempts != 1 {
		t.Errorf("Expected 1 attempt (no retry for non-retryable), got %d", attempts)
	}
}

func TestRateLimiter(t *testing.T) {
	limiter := NewRateLimiter(50 * time.Millisecond)
	ctx := context.Background()

	var requestTimes []time.Time

	// Make 3 requests
	for range 3 {
		err := limiter.Wait(ctx)
		if err != nil {
			t.Fatalf("Wait failed: %v", err)
		}
		requestTimes = append(requestTimes, time.Now())
	}

	// Check intervals
	for i := 1; i < len(requestTimes); i++ {
		interval := requestTimes[i].Sub(requestTimes[i-1])
		if interval < 45*time.Millisecond { // Allow some tolerance
			t.Errorf("Interval %d too short: %v", i, interval)
		}
	}
}

func TestRateLimiter_ContextCancellation(t *testing.T) {
	limiter := NewRateLimiter(1 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// First request is immediate
	err := limiter.Wait(ctx)
	if err != nil {
		t.Fatalf("First wait failed: %v", err)
	}

	// Second request should be cancelled
	err = limiter.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context deadline exceeded, got %v", err)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		statusCode int
		want       bool
	}{
		{"error is retryable", errors.New("timeout"), 0, true},
		{"500 is retryable", nil, 500, true},
		{"502 is retryable", nil, 502, true},
		{"429 is retryable", nil, 429, true},
		{"404 is not retryable", nil, 404, false},
		{"200 is not retryable", nil, 200, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRetryable(tt.err, tt.statusCode)
			if got != tt.want {
				t.Errorf("IsRetryable(%v, %d) = %v, want %v", tt.err, tt.statusCode, got, tt.want)
			}
		})
	}
}

func TestCalculateBackoff(t *testing.T) {
	config := RetryConfig{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     10 * time.Second,
		Multiplier:     2.0,
		Jitter:         0, // No jitter for predictable test
	}

	// Attempt 0: 100ms * 2^0 = 100ms
	b0 := calculateBackoff(config, 0)
	if b0 != 100*time.Millisecond {
		t.Errorf("Attempt 0: expected 100ms, got %v", b0)
	}

	// Attempt 1: 100ms * 2^1 = 200ms
	b1 := calculateBackoff(config, 1)
	if b1 != 200*time.Millisecond {
		t.Errorf("Attempt 1: expected 200ms, got %v", b1)
	}

	// Attempt 2: 100ms * 2^2 = 400ms
	b2 := calculateBackoff(config, 2)
	if b2 != 400*time.Millisecond {
		t.Errorf("Attempt 2: expected 400ms, got %v", b2)
	}
}

func TestCalculateBackoff_MaxCap(t *testing.T) {
	config := RetryConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second,
		Multiplier:     10.0,
		Jitter:         0,
	}

	// Attempt 2: 1s * 10^2 = 100s, but capped at 5s
	b := calculateBackoff(config, 2)
	if b != 5*time.Second {
		t.Errorf("Expected capped at 5s, got %v", b)
	}
}

// Benchmark concurrent rate limiter usage
func BenchmarkRateLimiter_Concurrent(b *testing.B) {
	limiter := NewRateLimiter(time.Microsecond)
	ctx := context.Background()

	var ops atomic.Int64

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			limiter.Wait(ctx)
			ops.Add(1)
		}
	})
}
