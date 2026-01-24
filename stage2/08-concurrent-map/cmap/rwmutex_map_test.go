package cmap

import (
	"sync"
	"testing"
)

func TestRWMutexMap_BasicOperations(t *testing.T) {
	m := NewRWMutexMap[string, int]()

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

func TestRWMutexMap_GetOrSet(t *testing.T) {
	m := NewRWMutexMap[string, int]()

	// First call should set
	v, loaded := m.GetOrSet("key", 100)
	if loaded || v != 100 {
		t.Errorf("GetOrSet(key, 100) = %d, %v; want 100, false", v, loaded)
	}

	// Second call should get existing
	v, loaded = m.GetOrSet("key", 200)
	if !loaded || v != 100 {
		t.Errorf("GetOrSet(key, 200) = %d, %v; want 100, true", v, loaded)
	}
}

func TestRWMutexMap_ConcurrentReads(t *testing.T) {
	m := NewRWMutexMap[int, int]()

	// Pre-populate
	for i := 0; i < 100; i++ {
		m.Set(i, i*2)
	}

	var wg sync.WaitGroup
	// Many concurrent readers
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := n % 100
			v, ok := m.Get(key)
			if !ok || v != key*2 {
				t.Errorf("Get(%d) = %d, %v; want %d, true", key, v, ok, key*2)
			}
		}(i)
	}
	wg.Wait()
}

func TestRWMutexMap_ConcurrentReadWrite(t *testing.T) {
	m := NewRWMutexMap[int, int]()
	var wg sync.WaitGroup

	// Writers
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m.Set(n, n*2)
		}(i)
	}

	// Readers
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Just exercise Gets, values may or may not be set yet
			m.Get(n % 50)
		}(i)
	}

	wg.Wait()
}

func TestRWMutexMap_Range(t *testing.T) {
	m := NewRWMutexMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)

	sum := 0
	m.Range(func(k string, v int) bool {
		sum += v
		return true
	})

	if sum != 6 {
		t.Errorf("Range sum = %d; want 6", sum)
	}
}
