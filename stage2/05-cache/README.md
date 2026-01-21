# Project 5: Cache System

## Overview

A thread-safe in-memory cache with TTL expiration, LRU eviction, and cache statistics. This project focuses on:
- `sync.RWMutex` for high read concurrency
- TTL with passive and active expiration
- LRU/LFU eviction strategies
- Go generics for type safety

## Project Structure

```
05-cache/
├── cache/
│   ├── cache.go         # Basic cache with RWMutex
│   ├── ttl.go           # TTL expiration logic
│   ├── lru.go           # LRU eviction
│   ├── stats.go         # Cache statistics
│   └── cache_test.go    # Tests
└── README.md
```

## Key Concepts

### RWMutex Pattern
```go
// Multiple readers can proceed concurrently
mu.RLock()
value := cache[key]  // Read
mu.RUnlock()

// Only one writer at a time
mu.Lock()
cache[key] = value   // Write
mu.Unlock()
```

## Progress

- [x] Step 5.1: Basic In-Memory Cache
- [x] Step 5.2: TTL Expiration
- [x] Step 5.3: LRU Eviction
- [x] Step 5.4: Advanced Features (Singleflight, Cache-Aside)
