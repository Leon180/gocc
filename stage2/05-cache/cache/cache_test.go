package cache

import (
	"sync"
	"testing"
	"time"
)

func TestCache_BasicOperations(t *testing.T) {
	c := New[string, int](Config{})

	// Test Set and Get
	c.Set("one", 1)
	c.Set("two", 2)

	val, ok := c.Get("one")
	if !ok || val != 1 {
		t.Errorf("Expected 1, got %d, ok=%v", val, ok)
	}

	val, ok = c.Get("two")
	if !ok || val != 2 {
		t.Errorf("Expected 2, got %d, ok=%v", val, ok)
	}

	// Test missing key
	_, ok = c.Get("three")
	if ok {
		t.Error("Expected false for missing key")
	}
}

func TestCache_Delete(t *testing.T) {
	c := New[string, string](Config{})

	c.Set("key", "value")
	if !c.Has("key") {
		t.Error("Key should exist")
	}

	deleted := c.Delete("key")
	if !deleted {
		t.Error("Delete should return true")
	}

	if c.Has("key") {
		t.Error("Key should not exist after delete")
	}

	// Delete non-existent key
	deleted = c.Delete("nonexistent")
	if deleted {
		t.Error("Delete should return false for non-existent key")
	}
}

func TestCache_TTL(t *testing.T) {
	c := New[string, string](Config{})

	// Set with short TTL
	c.SetWithTTL("expires", "soon", 50*time.Millisecond)

	// Should exist immediately
	val, ok := c.Get("expires")
	if !ok || val != "soon" {
		t.Error("Should exist before expiration")
	}

	// Wait for expiration
	time.Sleep(60 * time.Millisecond)

	// Should be expired (passive expiration on read)
	_, ok = c.Get("expires")
	if ok {
		t.Error("Should be expired after TTL")
	}
}

func TestCache_MaxSize(t *testing.T) {
	c := New[string, int](Config{MaxSize: 3})

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	if c.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", c.Len())
	}

	// Adding 4th should evict one
	c.Set("d", 4)

	if c.Len() != 3 {
		t.Errorf("Expected 3 entries after eviction, got %d", c.Len())
	}
}

func TestCache_Stats(t *testing.T) {
	c := New[string, int](Config{})

	c.Set("key", 42)

	// Hit
	c.Get("key")
	c.Get("key")

	// Miss
	c.Get("nonexistent")

	stats := c.Stats()

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}

	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}

	expectedHitRate := 2.0 / 3.0
	if stats.HitRate < expectedHitRate-0.01 || stats.HitRate > expectedHitRate+0.01 {
		t.Errorf("Expected hit rate ~%.2f, got %.2f", expectedHitRate, stats.HitRate)
	}
}

func TestCache_Concurrent(t *testing.T) {
	c := New[int, int](Config{})
	var wg sync.WaitGroup

	// Concurrent writers
	for i := range 100 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c.Set(id, id*10)
		}(i)
	}

	// Concurrent readers
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				c.Get(i)
			}
		}()
	}

	wg.Wait()

	// Verify some values
	for i := range 100 {
		val, ok := c.Get(i)
		if !ok {
			t.Errorf("Key %d should exist", i)
			continue
		}
		if val != i*10 {
			t.Errorf("Key %d: expected %d, got %d", i, i*10, val)
		}
	}
}

func TestCache_Clear(t *testing.T) {
	c := New[string, int](Config{})

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	c.Clear()

	if c.Len() != 0 {
		t.Errorf("Expected 0 after clear, got %d", c.Len())
	}
}

func TestCache_Keys(t *testing.T) {
	c := New[string, int](Config{})

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	keys := c.Keys()

	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}

	for _, expected := range []string{"a", "b", "c"} {
		if !keySet[expected] {
			t.Errorf("Missing key: %s", expected)
		}
	}
}

// Benchmark: RWMutex read performance
func BenchmarkCache_Get_Concurrent(b *testing.B) {
	c := New[int, int](Config{})

	// Pre-populate
	for i := range 1000 {
		c.Set(i, i*10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(i % 1000)
			i++
		}
	})
}

func BenchmarkCache_Set_Concurrent(b *testing.B) {
	c := New[int, int](Config{})

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i%1000, i)
			i++
		}
	})
}

func BenchmarkCache_Mixed_Concurrent(b *testing.B) {
	c := New[int, int](Config{})

	// Pre-populate
	for i := range 1000 {
		c.Set(i, i*10)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				c.Set(i%1000, i) // 10% writes
			} else {
				c.Get(i % 1000) // 90% reads
			}
			i++
		}
	})
}
