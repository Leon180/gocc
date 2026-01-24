package cmap

import (
	"sync"
	"testing"
)

func TestMutexMap_BasicOperations(t *testing.T) {
	m := NewMutexMap[string, int]()

	// Set and Get
	m.Set("key1", 100)
	v, ok := m.Get("key1")
	if !ok || v != 100 {
		t.Errorf("Get(key1) = %d, %v; want 100, true", v, ok)
	}

	// Get non-existent
	_, ok = m.Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}

	// Delete
	m.Delete("key1")
	_, ok = m.Get("key1")
	if ok {
		t.Error("Get(key1) after delete should return false")
	}
}

func TestMutexMap_Len(t *testing.T) {
	m := NewMutexMap[string, string]()

	if m.Len() != 0 {
		t.Errorf("Len() = %d; want 0", m.Len())
	}

	m.Set("a", "1")
	m.Set("b", "2")
	m.Set("c", "3")

	if m.Len() != 3 {
		t.Errorf("Len() = %d; want 3", m.Len())
	}
}

func TestMutexMap_Range(t *testing.T) {
	m := NewMutexMap[int, int]()
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

	// Test early exit
	count = 0
	m.Range(func(k, v int) bool {
		count++
		return count < 3
	})

	if count != 3 {
		t.Errorf("Range with early exit visited %d items; want 3", count)
	}
}

func TestMutexMap_Concurrent(t *testing.T) {
	m := NewMutexMap[int, int]()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m.Set(n, n*2)
		}(i)
	}
	wg.Wait()

	if m.Len() != 100 {
		t.Errorf("After concurrent writes, Len() = %d; want 100", m.Len())
	}

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
