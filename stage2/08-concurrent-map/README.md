# Project 8: Concurrent Map

> Focus: `sync.Map` vs sharded locks (sharding)

## Overview

Explore different approaches to concurrent map access:
- `map + sync.Mutex` - Simple but contended
- `map + sync.RWMutex` - Better for read-heavy workloads  
- `sync.Map` - Optimized for specific patterns
- Sharded Map - Reduces lock contention

## Key Concepts

### sync.Map Best For:
1. Keys are written once but read many times
2. Multiple goroutines read, write, and overwrite disjoint sets of keys

### Sharded Map Pattern:
```
Key "user:123" → Hash → Shard 7 (of 16)
                         ↓
                    [Lock + Map]
```

Reduces contention by distributing keys across multiple locks.

## Project Structure

```
stage2/08-concurrent-map/
├── cmap/
│   ├── mutex_map.go      # map + Mutex
│   ├── rwmutex_map.go    # map + RWMutex
│   ├── sync_map.go       # sync.Map wrapper
│   ├── sharded_map.go    # Sharded locks
│   ├── benchmark_test.go # Performance comparison
│   └── *_test.go
├── go.mod
└── README.md
```

## Progress

- [x] Step 8.1: Understanding sync.Map
- [x] Step 8.2: Sharded Lock Map
- [x] Step 8.3: Performance Testing
- [x] Step 8.4: High-Performance KV Store
