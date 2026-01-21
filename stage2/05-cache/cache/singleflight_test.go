package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleflight_DeduplicatesCalls(t *testing.T) {
	sf := NewSingleflight[string, int]()

	var callCount atomic.Int32

	var wg sync.WaitGroup
	results := make([]int, 10)

	// Launch 10 concurrent requests for the same key
	for i := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			val, err := sf.Do("key", func() (int, error) {
				callCount.Add(1)
				time.Sleep(50 * time.Millisecond) // Simulate slow operation
				return 42, nil
			})
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			results[idx] = val
		}(i)
	}

	wg.Wait()

	// Should only have called the function once
	if callCount.Load() != 1 {
		t.Errorf("Expected 1 call, got %d", callCount.Load())
	}

	// All results should be 42
	for i, r := range results {
		if r != 42 {
			t.Errorf("Result %d: expected 42, got %d", i, r)
		}
	}
}

func TestSingleflight_DifferentKeys(t *testing.T) {
	sf := NewSingleflight[string, int]()

	var callCount atomic.Int32

	var wg sync.WaitGroup

	// Different keys should each call the function
	for i := range 5 {
		wg.Add(1)
		go func(key string, value int) {
			defer wg.Done()
			val, _ := sf.Do(key, func() (int, error) {
				callCount.Add(1)
				return value, nil
			})
			if val != value {
				t.Errorf("Expected %d, got %d", value, val)
			}
		}("key"+string(rune('0'+i)), i)
	}

	wg.Wait()

	// Each key should have its own call
	if callCount.Load() != 5 {
		t.Errorf("Expected 5 calls (one per key), got %d", callCount.Load())
	}
}

func TestSingleflight_Error(t *testing.T) {
	sf := NewSingleflight[string, int]()

	expectedErr := errors.New("fetch failed")

	_, err := sf.Do("key", func() (int, error) {
		return 0, expectedErr
	})

	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestCacheAside_HitAndMiss(t *testing.T) {
	cache := New[string, string](Config{})
	fetchCount := 0

	ca := NewCacheAside(cache, func(ctx context.Context, key string) (string, error) {
		fetchCount++
		return "value-" + key, nil
	})

	// First call - cache miss, should fetch
	val, err := ca.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != "value-key1" {
		t.Errorf("Expected 'value-key1', got '%s'", val)
	}
	if fetchCount != 1 {
		t.Errorf("Expected 1 fetch, got %d", fetchCount)
	}

	// Second call - cache hit, should NOT fetch
	val, err = ca.Get(context.Background(), "key1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if val != "value-key1" {
		t.Errorf("Expected 'value-key1', got '%s'", val)
	}
	if fetchCount != 1 {
		t.Errorf("Expected still 1 fetch (cache hit), got %d", fetchCount)
	}
}

func TestCacheAside_PreventsCacheStampede(t *testing.T) {
	cache := New[string, string](Config{})
	var fetchCount atomic.Int32

	ca := NewCacheAside(cache, func(ctx context.Context, key string) (string, error) {
		fetchCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Slow fetch
		return "value", nil
	})

	var wg sync.WaitGroup

	// 10 concurrent requests for same missing key
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ca.Get(context.Background(), "same-key")
		}()
	}

	wg.Wait()

	// Should only fetch once (singleflight!)
	if fetchCount.Load() != 1 {
		t.Errorf("Expected 1 fetch (cache stampede prevented), got %d", fetchCount.Load())
	}

	// Cache should be populated
	if _, ok := cache.Get("same-key"); !ok {
		t.Error("Cache should contain 'same-key'")
	}
}

func TestCacheAside_Invalidate(t *testing.T) {
	cache := New[string, string](Config{})
	fetchCount := 0

	ca := NewCacheAside(cache, func(ctx context.Context, key string) (string, error) {
		fetchCount++
		return "value", nil
	})

	// Populate cache
	ca.Get(context.Background(), "key")
	if fetchCount != 1 {
		t.Error("Should have fetched once")
	}

	// Invalidate
	ca.Invalidate("key")

	// Should fetch again
	ca.Get(context.Background(), "key")
	if fetchCount != 2 {
		t.Errorf("Expected 2 fetches after invalidation, got %d", fetchCount)
	}
}

func TestCacheAside_FetchError(t *testing.T) {
	cache := New[string, string](Config{})
	expectedErr := errors.New("database error")

	ca := NewCacheAside(cache, func(ctx context.Context, key string) (string, error) {
		return "", expectedErr
	})

	_, err := ca.Get(context.Background(), "key")
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Cache should NOT contain the key (error case)
	if _, ok := cache.Get("key"); ok {
		t.Error("Cache should not contain key after fetch error")
	}
}

// Benchmark singleflight under contention
func BenchmarkSingleflight_Contention(b *testing.B) {
	sf := NewSingleflight[int, int]()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sf.Do(i%10, func() (int, error) { // 10 keys
				return i, nil
			})
			i++
		}
	})
}
