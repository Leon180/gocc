package cmap

import (
	"sync"
	"testing"
)

func TestShardedMap_BasicOperations(t *testing.T) {
	m := NewShardedMap[string, int]()

	m.Set("key1", 100)
	v, ok := m.Get("key1")
	if !ok || v != 100 {
		t.Errorf("Get(key1) = %d, %v; want 100, true", v, ok)
	}

	_, ok = m.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}

	m.Delete("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Error("Get(key1) after delete should return false")
	}
}

func TestShardedMap_GetOrSet(t *testing.T) {
	m := NewShardedMap[string, int]()

	v, loaded := m.GetOrSet("key", 100)
	if loaded || v != 100 {
		t.Errorf("GetOrSet(key, 100) = %d, %v; want 100, false", v, loaded)
	}

	v, loaded = m.GetOrSet("key", 200)
	if !loaded || v != 100 {
		t.Errorf("GetOrSet(key, 200) = %d, %v; want 100, true", v, loaded)
	}
}

func TestShardedMap_Len(t *testing.T) {
	m := NewShardedMap[int, int]()

	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	if m.Len() != 100 {
		t.Errorf("Len() = %d; want 100", m.Len())
	}
}

func TestShardedMap_Range(t *testing.T) {
	m := NewShardedMap[int, int]()
	for i := 0; i < 10; i++ {
		m.Set(i, i*10)
	}

	sum := 0
	m.Range(func(k, v int) bool {
		sum += v
		return true
	})

	expected := 0 + 10 + 20 + 30 + 40 + 50 + 60 + 70 + 80 + 90
	if sum != expected {
		t.Errorf("Range sum = %d; want %d", sum, expected)
	}
}

func TestShardedMap_ShardDistribution(t *testing.T) {
	m := NewShardedMapWithCount[int, int](8)

	// Insert many keys
	for i := 0; i < 1000; i++ {
		m.Set(i, i)
	}

	stats := m.ShardStats()
	if len(stats) != 8 {
		t.Errorf("ShardStats() returned %d shards; want 8", len(stats))
	}

	// Check all items are distributed
	total := 0
	for _, count := range stats {
		total += count
	}
	if total != 1000 {
		t.Errorf("Total items across shards = %d; want 1000", total)
	}

	// Log distribution for inspection (not a hard assertion)
	t.Logf("Shard distribution: %v", stats)
}

func TestShardedMap_Concurrent(t *testing.T) {
	m := NewShardedMap[int, int]()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m.Set(n, n*2)
		}(i)
	}
	wg.Wait()

	if m.Len() != 1000 {
		t.Errorf("After concurrent writes, Len() = %d; want 1000", m.Len())
	}

	// Concurrent reads
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v, ok := m.Get(n)
			if !ok || v != n*2 {
				t.Errorf("Get(%d) = %d, %v; want %d, true", n, v, ok, n*2)
			}
		}(i)
	}
	wg.Wait()
}

func TestShardedMap_ConcurrentMixed(t *testing.T) {
	m := NewShardedMap[int, int]()
	var wg sync.WaitGroup

	// Pre-populate
	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	// Mixed operations
	for i := 0; i < 500; i++ {
		wg.Add(3)
		go func(n int) {
			defer wg.Done()
			m.Get(n % 100)
		}(i)
		go func(n int) {
			defer wg.Done()
			m.Set(n%100, n)
		}(i)
		go func(n int) {
			defer wg.Done()
			m.Delete(n % 50)
		}(i)
	}
	wg.Wait()
}

func TestNextPowerOf2(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{16, 16},
		{17, 32},
		{100, 128},
	}

	for _, tc := range tests {
		got := nextPowerOf2(tc.input)
		if got != tc.expected {
			t.Errorf("nextPowerOf2(%d) = %d; want %d", tc.input, got, tc.expected)
		}
	}
}
