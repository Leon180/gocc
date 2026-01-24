package cmap

import (
	"fmt"
	"sync"
	"testing"
)

// Benchmark configurations
const (
	benchItems   = 10000
	benchWorkers = 100
)

// ========== Read-Heavy Benchmarks (95% read, 5% write) ==========

func BenchmarkMutexMap_ReadHeavy(b *testing.B) {
	m := NewMutexMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 { // 5% writes
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkRWMutexMap_ReadHeavy(b *testing.B) {
	m := NewRWMutexMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkSyncMap_ReadHeavy(b *testing.B) {
	m := NewSyncMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkShardedMap_ReadHeavy(b *testing.B) {
	m := NewShardedMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

// ========== Write-Heavy Benchmarks (50% read, 50% write) ==========

func BenchmarkMutexMap_WriteHeavy(b *testing.B) {
	m := NewMutexMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkRWMutexMap_WriteHeavy(b *testing.B) {
	m := NewRWMutexMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkSyncMap_WriteHeavy(b *testing.B) {
	m := NewSyncMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

func BenchmarkShardedMap_WriteHeavy(b *testing.B) {
	m := NewShardedMap[int, int]()
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

// ========== Disjoint Keys Benchmark (sync.Map's sweet spot) ==========

func BenchmarkSyncMap_DisjointKeys(b *testing.B) {
	m := NewSyncMap[int, int]()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine works with its own key range
		base := int(testing.AllocsPerRun(1, func() {})) * 1000000
		i := 0
		for pb.Next() {
			key := base + i
			m.Set(key, i)
			m.Get(key)
			i++
		}
	})
}

func BenchmarkShardedMap_DisjointKeys(b *testing.B) {
	m := NewShardedMap[int, int]()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := int(testing.AllocsPerRun(1, func() {})) * 1000000
		i := 0
		for pb.Next() {
			key := base + i
			m.Set(key, i)
			m.Get(key)
			i++
		}
	})
}

// ========== Shard Count Comparison ==========

func BenchmarkShardedMap_Shards8(b *testing.B) {
	benchmarkShardedMapWithCount(b, 8)
}

func BenchmarkShardedMap_Shards16(b *testing.B) {
	benchmarkShardedMapWithCount(b, 16)
}

func BenchmarkShardedMap_Shards32(b *testing.B) {
	benchmarkShardedMapWithCount(b, 32)
}

func BenchmarkShardedMap_Shards64(b *testing.B) {
	benchmarkShardedMapWithCount(b, 64)
}

func benchmarkShardedMapWithCount(b *testing.B, shardCount int) {
	m := NewShardedMapWithCount[int, int](shardCount)
	for i := 0; i < benchItems; i++ {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				m.Set(i%benchItems, i)
			} else {
				m.Get(i % benchItems)
			}
			i++
		}
	})
}

// ========== High Contention Benchmark ==========

func BenchmarkMutexMap_HighContention(b *testing.B) {
	m := NewMutexMap[int, int]()
	m.Set(0, 0) // Single key = maximum contention

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set(0, 1)
			m.Get(0)
		}
	})
}

func BenchmarkRWMutexMap_HighContention(b *testing.B) {
	m := NewRWMutexMap[int, int]()
	m.Set(0, 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set(0, 1)
			m.Get(0)
		}
	})
}

func BenchmarkSyncMap_HighContention(b *testing.B) {
	m := NewSyncMap[int, int]()
	m.Set(0, 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set(0, 1)
			m.Get(0)
		}
	})
}

func BenchmarkShardedMap_HighContention(b *testing.B) {
	m := NewShardedMap[int, int]()
	m.Set(0, 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Set(0, 1)
			m.Get(0)
		}
	})
}

// ========== Manual Comparison Test ==========

func TestComparePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance comparison in short mode")
	}

	const ops = 100000
	const workers = 10

	runTest := func(name string, set func(int, int), get func(int) (int, bool)) {
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for i := 0; i < ops/workers; i++ {
					key := workerID*1000 + i%100
					set(key, i)
					get(key)
				}
			}(w)
		}
		wg.Wait()
	}

	t.Run("MutexMap", func(t *testing.T) {
		m := NewMutexMap[int, int]()
		runTest("MutexMap", m.Set, m.Get)
	})

	t.Run("RWMutexMap", func(t *testing.T) {
		m := NewRWMutexMap[int, int]()
		runTest("RWMutexMap", m.Set, m.Get)
	})

	t.Run("SyncMap", func(t *testing.T) {
		m := NewSyncMap[int, int]()
		runTest("SyncMap", m.Set, m.Get)
	})

	t.Run("ShardedMap", func(t *testing.T) {
		m := NewShardedMap[int, int]()
		runTest("ShardedMap", m.Set, m.Get)
	})

	fmt.Println("Performance comparison completed. Use 'go test -bench=.' for detailed benchmarks.")
}
