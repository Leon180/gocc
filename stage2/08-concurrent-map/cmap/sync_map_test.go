package cmap

import (
	"sync"
	"testing"
)

func TestSyncMap_BasicOperations(t *testing.T) {
	m := NewSyncMap[string, int]()

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

func TestSyncMap_GetOrSet(t *testing.T) {
	m := NewSyncMap[string, int]()

	v, loaded := m.GetOrSet("key", 100)
	if loaded || v != 100 {
		t.Errorf("GetOrSet(key, 100) = %d, %v; want 100, false", v, loaded)
	}

	v, loaded = m.GetOrSet("key", 200)
	if !loaded || v != 100 {
		t.Errorf("GetOrSet(key, 200) = %d, %v; want 100, true", v, loaded)
	}
}

func TestSyncMap_GetAndDelete(t *testing.T) {
	m := NewSyncMap[string, int]()

	m.Set("key", 100)
	v, ok := m.GetAndDelete("key")
	if !ok || v != 100 {
		t.Errorf("GetAndDelete(key) = %d, %v; want 100, true", v, ok)
	}

	_, ok = m.Get("key")
	if ok {
		t.Error("Get(key) after GetAndDelete should return false")
	}

	// GetAndDelete on non-existent
	_, ok = m.GetAndDelete("nonexistent")
	if ok {
		t.Error("GetAndDelete(nonexistent) should return false")
	}
}

func TestSyncMap_CompareAndSwap(t *testing.T) {
	m := NewSyncMap[string, int]()

	m.Set("key", 100)

	// Wrong old value
	if m.CompareAndSwap("key", 50, 200) {
		t.Error("CompareAndSwap with wrong old value should return false")
	}

	// Correct old value
	if !m.CompareAndSwap("key", 100, 200) {
		t.Error("CompareAndSwap with correct old value should return true")
	}

	v, _ := m.Get("key")
	if v != 200 {
		t.Errorf("After CompareAndSwap, Get(key) = %d; want 200", v)
	}
}

func TestSyncMap_CompareAndDelete(t *testing.T) {
	m := NewSyncMap[string, int]()

	m.Set("key", 100)

	// Wrong value
	if m.CompareAndDelete("key", 50) {
		t.Error("CompareAndDelete with wrong value should return false")
	}

	// Correct value
	if !m.CompareAndDelete("key", 100) {
		t.Error("CompareAndDelete with correct value should return true")
	}

	_, ok := m.Get("key")
	if ok {
		t.Error("Get(key) after CompareAndDelete should return false")
	}
}

func TestSyncMap_Range(t *testing.T) {
	m := NewSyncMap[int, int]()
	for i := 0; i < 5; i++ {
		m.Set(i, i*10)
	}

	count := 0
	m.Range(func(k, v int) bool {
		count++
		return true
	})

	if count != 5 {
		t.Errorf("Range visited %d items; want 5", count)
	}
}

func TestSyncMap_Concurrent(t *testing.T) {
	m := NewSyncMap[int, int]()
	var wg sync.WaitGroup

	// Concurrent writes (disjoint keys - sync.Map's sweet spot)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m.Set(n, n*2)
		}(i)
	}
	wg.Wait()

	// Concurrent reads
	for i := 0; i < 100; i++ {
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
