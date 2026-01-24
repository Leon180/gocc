package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"concurrentmap/cmap"
)

func main() {
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║     Concurrent Map Demo - Project 8              ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()

	printHelp()
	runInteractive()
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  1. compare   - Compare all map types performance")
	fmt.Println("  2. shards    - Demo shard distribution")
	fmt.Println("  3. ttl       - Demo TTL map expiration")
	fmt.Println("  4. benchmark - Run quick benchmark")
	fmt.Println("  5. all       - Run all demos")
	fmt.Println("  h. help      - Show this help")
	fmt.Println("  q. quit      - Exit")
	fmt.Println()
}

func runInteractive() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("cmap> ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))

		switch input {
		case "1", "compare":
			runCompareDemo()
		case "2", "shards":
			runShardDemo()
		case "3", "ttl":
			runTTLDemo()
		case "4", "benchmark":
			runBenchmarkDemo()
		case "5", "all":
			runCompareDemo()
			runShardDemo()
			runTTLDemo()
			runBenchmarkDemo()
		case "h", "help":
			printHelp()
		case "q", "quit", "exit":
			fmt.Println("Goodbye!")
			return
		case "":
			// Ignore empty input
		default:
			fmt.Printf("Unknown command: %s\n", input)
		}
	}
}

func runCompareDemo() {
	fmt.Println("\n═══ Map Type Comparison ═══")

	const ops = 10000
	const workers = 10

	// MutexMap
	start := time.Now()
	mm := cmap.NewMutexMap[int, int]()
	runOps(workers, ops, func(i int) { mm.Set(i%1000, i) }, func(i int) { mm.Get(i % 1000) })
	fmt.Printf("  MutexMap:    %v\n", time.Since(start))

	// RWMutexMap
	start = time.Now()
	rwm := cmap.NewRWMutexMap[int, int]()
	runOps(workers, ops, func(i int) { rwm.Set(i%1000, i) }, func(i int) { rwm.Get(i % 1000) })
	fmt.Printf("  RWMutexMap:  %v\n", time.Since(start))

	// SyncMap
	start = time.Now()
	sm := cmap.NewSyncMap[int, int]()
	runOps(workers, ops, func(i int) { sm.Set(i%1000, i) }, func(i int) { sm.Get(i % 1000) })
	fmt.Printf("  SyncMap:     %v\n", time.Since(start))

	// ShardedMap
	start = time.Now()
	shm := cmap.NewShardedMap[int, int]()
	runOps(workers, ops, func(i int) { shm.Set(i%1000, i) }, func(i int) { shm.Get(i % 1000) })
	fmt.Printf("  ShardedMap:  %v\n", time.Since(start))

	fmt.Println()
}

func runOps(workers, ops int, setFn, getFn func(int)) {
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < ops/workers; i++ {
				key := workerID*1000 + i
				setFn(key)
				getFn(key)
			}
		}(w)
	}
	wg.Wait()
}

func runShardDemo() {
	fmt.Println("\n═══ Shard Distribution Demo ═══")

	m := cmap.NewShardedMapWithCount[string, int](8)

	// Add some keys
	keys := []string{
		"user:1", "user:2", "user:3", "user:4",
		"session:1", "session:2", "session:3",
		"cache:a", "cache:b", "cache:c",
	}

	for i, key := range keys {
		m.Set(key, i)
	}

	stats := m.ShardStats()
	fmt.Println("  Keys added:", keys)
	fmt.Println("  Distribution across 8 shards:", stats)
	fmt.Println()

	// Show larger distribution
	m2 := cmap.NewShardedMapWithCount[int, int](16)
	for i := 0; i < 1000; i++ {
		m2.Set(i, i)
	}
	fmt.Println("  1000 integer keys across 16 shards:")
	fmt.Printf("    %v\n", m2.ShardStats())
	fmt.Println()
}

func runTTLDemo() {
	fmt.Println("\n═══ TTL Map Demo ═══")

	m := cmap.NewTTLMapWithOptions[string, string](8, 500*time.Millisecond)
	defer m.Close()

	// Set with different TTLs
	m.Set("short", "expires in 100ms", 100*time.Millisecond)
	m.Set("medium", "expires in 300ms", 300*time.Millisecond)
	m.Set("long", "expires in 1s", 1*time.Second)
	m.SetNoExpire("forever", "never expires")

	fmt.Println("  Initial state:")
	printTTLStats(m)

	fmt.Println("  After 150ms:")
	time.Sleep(150 * time.Millisecond)
	printTTLStats(m)

	fmt.Println("  After 350ms:")
	time.Sleep(200 * time.Millisecond)
	printTTLStats(m)

	// Touch to extend TTL
	m.Touch("long", 2*time.Second)
	fmt.Println("  Touched 'long' (extended TTL)")

	// Cleanup
	time.Sleep(600 * time.Millisecond)
	fmt.Println("  After cleanup (600ms later):")
	printTTLStats(m)
	fmt.Println()
}

func printTTLStats(m *cmap.TTLMap[string, string]) {
	stats := m.Stats()
	fmt.Printf("    Active: %d, Expired: %d, Total: %d\n",
		stats.ActiveEntries, stats.ExpiredEntries, stats.TotalEntries)

	keys := []string{"short", "medium", "long", "forever"}
	for _, k := range keys {
		if v, ok := m.Get(k); ok {
			fmt.Printf("      %s: %s\n", k, v)
		}
	}
}

func runBenchmarkDemo() {
	fmt.Println("\n═══ Quick Benchmark ═══")
	fmt.Println("  (Use 'go test -bench=.' for detailed benchmarks)")
	fmt.Println()

	const iterations = 100000

	// Read-heavy benchmark
	fmt.Println("  Read-heavy (95% read, 5% write):")
	maps := []struct {
		name string
		set  func(int, int)
		get  func(int) (int, bool)
	}{
		{"MutexMap", nil, nil},
		{"RWMutexMap", nil, nil},
		{"SyncMap", nil, nil},
		{"ShardedMap", nil, nil},
	}

	mm := cmap.NewMutexMap[int, int]()
	rwm := cmap.NewRWMutexMap[int, int]()
	sm := cmap.NewSyncMap[int, int]()
	shm := cmap.NewShardedMap[int, int]()

	maps[0].set = mm.Set
	maps[0].get = mm.Get
	maps[1].set = rwm.Set
	maps[1].get = rwm.Get
	maps[2].set = sm.Set
	maps[2].get = sm.Get
	maps[3].set = shm.Set
	maps[3].get = shm.Get

	// Pre-populate
	for i := 0; i < 1000; i++ {
		mm.Set(i, i)
		rwm.Set(i, i)
		sm.Set(i, i)
		shm.Set(i, i)
	}

	for _, m := range maps {
		start := time.Now()
		for i := 0; i < iterations; i++ {
			if i%20 == 0 {
				m.set(i%1000, i)
			} else {
				m.get(i % 1000)
			}
		}
		elapsed := time.Since(start)
		opsPerSec := float64(iterations) / elapsed.Seconds()
		fmt.Printf("    %-12s %v (%s ops/sec)\n", m.name+":", elapsed, formatNumber(int64(opsPerSec)))
	}
	fmt.Println()
}

func formatNumber(n int64) string {
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}

	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
