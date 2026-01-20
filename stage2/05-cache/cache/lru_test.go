package cache

import (
	"sync"
	"testing"
	"time"
)

func TestLRUCache_BasicOperations(t *testing.T) {
	c := NewLRU[string, int](3)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	val, ok := c.Get("a")
	if !ok || val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	val, ok = c.Get("b")
	if !ok || val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	c := NewLRU[string, int](3)

	// Fill cache
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Access "a" to make it recently used
	c.Get("a")

	// Order now: a(head) -> c -> b(tail)
	// Add new entry, should evict "b"
	c.Set("d", 4)

	// "b" should be evicted
	_, ok := c.Get("b")
	if ok {
		t.Error("'b' should have been evicted")
	}

	// Others should exist
	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should exist")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("'c' should exist")
	}
	if _, ok := c.Get("d"); !ok {
		t.Error("'d' should exist")
	}
}

func TestLRUCache_UpdateMovesToFront(t *testing.T) {
	c := NewLRU[string, int](3)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Update "a" - should move to front
	c.Set("a", 10)

	// Keys should be: a, c, b
	keys := c.Keys()
	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "a" {
		t.Errorf("Expected 'a' at front, got '%s'", keys[0])
	}
}

func TestLRUCache_TTL(t *testing.T) {
	c := NewLRU[string, string](10)

	c.SetWithTTL("expires", "soon", 30*time.Millisecond)

	// Should exist immediately
	if _, ok := c.Get("expires"); !ok {
		t.Error("Should exist before expiration")
	}

	time.Sleep(50 * time.Millisecond)

	// Should be expired
	if _, ok := c.Get("expires"); ok {
		t.Error("Should be expired")
	}
}

func TestLRUCache_Delete(t *testing.T) {
	c := NewLRU[string, int](3)

	c.Set("a", 1)
	c.Set("b", 2)

	deleted := c.Delete("a")
	if !deleted {
		t.Error("Delete should return true")
	}

	if _, ok := c.Get("a"); ok {
		t.Error("'a' should be deleted")
	}

	if c.Len() != 1 {
		t.Errorf("Expected length 1, got %d", c.Len())
	}

	// Delete non-existent
	deleted = c.Delete("nonexistent")
	if deleted {
		t.Error("Delete should return false for non-existent")
	}
}

func TestLRUCache_Stats(t *testing.T) {
	c := NewLRU[string, int](2)

	c.Set("a", 1)

	// Hits
	c.Get("a")
	c.Get("a")

	// Miss
	c.Get("nonexistent")

	// Eviction
	c.Set("b", 2)
	c.Set("c", 3) // Evicts oldest

	stats := c.Stats()

	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.Evictions != 1 {
		t.Errorf("Expected 1 eviction, got %d", stats.Evictions)
	}
	if stats.Capacity != 2 {
		t.Errorf("Expected capacity 2, got %d", stats.Capacity)
	}
}

func TestLRUCache_Concurrent(t *testing.T) {
	c := NewLRU[int, int](100)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				c.Set(id*100+j, j)
			}
		}(i)
	}

	// Concurrent reads
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				c.Get(0)
			}
		}()
	}

	wg.Wait()

	// Cache should be at capacity
	if c.Len() != c.Capacity() {
		t.Errorf("Expected capacity %d, got %d", c.Capacity(), c.Len())
	}
}

func TestLRUCache_Clear(t *testing.T) {
	c := NewLRU[string, int](10)

	c.Set("a", 1)
	c.Set("b", 2)

	c.Clear()

	if c.Len() != 0 {
		t.Errorf("Expected 0 after clear, got %d", c.Len())
	}

	if len(c.Keys()) != 0 {
		t.Error("Keys should be empty")
	}
}

func TestLRUCache_KeysOrder(t *testing.T) {
	c := NewLRU[string, int](5)

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Access in reverse order
	c.Get("a")
	c.Get("b")
	c.Get("c")

	// Order should be: c, b, a (most to least recent)
	keys := c.Keys()
	expected := []string{"c", "b", "a"}

	for i, k := range expected {
		if keys[i] != k {
			t.Errorf("Index %d: expected '%s', got '%s'", i, k, keys[i])
		}
	}
}

// Benchmark LRU Get performance
func BenchmarkLRUCache_Get(b *testing.B) {
	c := NewLRU[int, int](1000)

	// Pre-populate
	for i := range 1000 {
		c.Set(i, i)
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

func BenchmarkLRUCache_Set(b *testing.B) {
	c := NewLRU[int, int](1000)

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(i%1000, i)
			i++
		}
	})
}
