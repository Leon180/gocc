# 🚀 Go Concurrency Learning Roadmap

> Master Go concurrency patterns and best practices from beginner to expert

## 📋 Learning Objectives

- Deeply understand Go's concurrency model (CSP)
- Master goroutines, channels, and the sync package
- Learn common concurrency design patterns
- Identify and resolve race conditions
- Develop the ability to design high-concurrency systems

---

## 🥇 Phase 1: Beginner → Intermediate

### 1. Concurrent Counter / Rate Limiter

> Focus: `atomic`, `Mutex`, race condition detection

- [x] **1.1 Basic Counter**
  - [x] Implement `Counter` struct with `Increment()`, `Decrement()`, `GetValue()`, `Reset()`
  - [x] Use `go test -race` to detect race conditions
  - [x] Analyze and document why race conditions occur

- [x] **1.2 Fix with Mutex**
  - [x] Use `sync.Mutex` to protect shared resources
  - [x] Understand proper Lock/Unlock usage
  - [x] Use `defer` to ensure unlocking

- [x] **1.3 Optimize with atomic**
  - [x] Refactor using `sync/atomic` package
  - [x] Compare performance between Mutex vs atomic
  - [x] Write benchmark tests

- [ ] **1.4 Rate Limiter Implementation**
  - [ ] Implement Token Bucket algorithm
  - [ ] Implement Leaky Bucket algorithm
  - [ ] Implement Sliding Window algorithm
  - [ ] Add API quota management

---

### 2. Worker Pool

> Focus: `channel`, goroutine lifecycle management

- [ ] **2.1 Basic Worker Pool**
  - [ ] Implement fixed number of worker goroutines
  - [ ] Use channels for task distribution
  - [ ] Implement result collection

- [ ] **2.2 Dynamic Worker Pool**
  - [ ] Support dynamic worker count adjustment
  - [ ] Implement worker health checks
  - [ ] Add task timeout handling

- [ ] **2.3 Graceful Shutdown**
  - [ ] Use `context.Context` to control lifecycle
  - [ ] Implement graceful shutdown, waiting for all tasks to complete
  - [ ] Handle forced shutdown scenarios

- [ ] **2.4 Practical Application: Batch Task Executor**
  - [ ] Implement image processor (e.g., batch thumbnails)
  - [ ] Implement file processor (e.g., batch compression)
  - [ ] Add progress reporting

---

### 3. Simple Web Crawler

> Focus: `WaitGroup`, concurrency limiting, `channel`

- [ ] **3.1 Basic Web Crawling**
  - [ ] Use `net/http` to send requests
  - [ ] Parse HTML to extract links
  - [ ] Use `sync.WaitGroup` to wait for all tasks

- [ ] **3.2 Concurrency Control**
  - [ ] Use buffered channel to limit concurrency
  - [ ] Implement semaphore pattern
  - [ ] Avoid crawling duplicate URLs

- [ ] **3.3 Advanced Crawler Features**
  - [ ] Add request retry mechanism
  - [ ] Implement robots.txt parsing
  - [ ] Add request interval control (polite crawler)

- [ ] **3.4 Data Collector**
  - [ ] Crawl and parse JSON APIs
  - [ ] Save data to files or database
  - [ ] Implement incremental crawling

---

### 4. Pub/Sub System

> Focus: `channel`, fan-out/fan-in patterns

- [ ] **4.1 Basic Pub/Sub**
  - [ ] Implement Publisher and Subscriber interfaces
  - [ ] Use channels for message passing
  - [ ] Support multiple subscribers

- [ ] **4.2 Topic Subscription**
  - [ ] Implement topic-based subscription
  - [ ] Support wildcard subscription patterns
  - [ ] Implement subscriber groups

- [ ] **4.3 Fan-out / Fan-in Patterns**
  - [ ] Implement fan-out: distribute one input to multiple processors
  - [ ] Implement fan-in: merge multiple inputs to one output
  - [ ] Combine fan-out and fan-in

- [ ] **4.4 Event Dispatcher**
  - [ ] Implement Event Bus
  - [ ] Support synchronous and asynchronous event handling
  - [ ] Add event filtering mechanism

---

## 🥈 Phase 2: Intermediate → Advanced

### 5. Cache System

> Focus: `RWMutex`, TTL expiration, concurrent read/write

- [ ] **5.1 Basic In-Memory Cache**
  - [ ] Implement `Get()`, `Set()`, `Delete()` methods
  - [ ] Use `sync.RWMutex` for read-write separation
  - [ ] Support any type (using generics)

- [ ] **5.2 TTL Expiration Mechanism**
  - [ ] Set expiration time for each key
  - [ ] Implement passive expiration (check on read)
  - [ ] Implement active expiration (background cleanup)

- [ ] **5.3 Cache Eviction Strategies**
  - [ ] Implement LRU (Least Recently Used)
  - [ ] Implement LFU (Least Frequently Used)
  - [ ] Support memory limit control

- [ ] **5.4 Advanced Features**
  - [ ] Implement Cache Aside pattern
  - [ ] Add cache statistics (hit rate, miss rate)
  - [ ] Implement basic interface for distributed cache

---

### 6. Connection Pool

> Focus: channel as semaphore, resource management

- [ ] **6.1 Basic Connection Pool**
  - [ ] Use buffered channel to manage connections
  - [ ] Implement `Acquire()` and `Release()` methods
  - [ ] Set maximum connection limit

- [ ] **6.2 Connection Lifecycle Management**
  - [ ] Implement connection health checks
  - [ ] Auto-close idle connections
  - [ ] Connection timeout handling

- [ ] **6.3 Advanced Connection Pool Features**
  - [ ] Support min/max connection configuration
  - [ ] Implement connection warm-up
  - [ ] Add connection usage statistics

- [ ] **6.4 Practical Applications**
  - [ ] Implement Database Connection Pool
  - [ ] Implement HTTP Client Pool
  - [ ] Implement gRPC Connection Pool

---

### 7. Task Scheduler

> Focus: `Mutex`, `Cond`, scheduled tasks

- [ ] **7.1 Basic Scheduler**
  - [ ] Implement delayed execution
  - [ ] Implement interval execution
  - [ ] Use `time.Ticker` and `time.Timer`

- [ ] **7.2 Cron-like Scheduler**
  - [ ] Parse cron expressions
  - [ ] Support second-level precision
  - [ ] Implement task registration and cancellation

- [ ] **7.3 Task Management**
  - [ ] Implement task priority
  - [ ] Support task dependencies
  - [ ] Implement task retry mechanism

- [ ] **7.4 Advanced Features**
  - [ ] Use `sync.Cond` to implement task waiting
  - [ ] Implement distributed task lock
  - [ ] Support task persistence

---

### 8. Concurrent Map

> Focus: `sync.Map` vs sharded locks (sharding)

- [ ] **8.1 Understanding sync.Map**
  - [ ] Study `sync.Map` use cases
  - [ ] Compare performance with `map + Mutex`
  - [ ] Understand `sync.Map` internal implementation

- [ ] **8.2 Sharded Lock Map**
  - [ ] Implement Sharded Map
  - [ ] Design appropriate hash function
  - [ ] Dynamically adjust shard count

- [ ] **8.3 Performance Testing and Tuning**
  - [ ] Write comprehensive benchmarks
  - [ ] Test performance under different read/write ratios
  - [ ] Analyze lock contention

- [ ] **8.4 High-Performance KV Store**
  - [ ] Combine TTL with sharded locks
  - [ ] Implement batch operations
  - [ ] Add data persistence

---

## 🥉 Phase 3: Advanced → Production

### 9. HTTP Server

> Focus: request handling, graceful shutdown, `context`

- [ ] **9.1 Basic HTTP Server**
  - [ ] Build server using `net/http`
  - [ ] Implement routing
  - [ ] Implement middleware pattern

- [ ] **9.2 Request Handling**
  - [ ] Implement request context propagation
  - [ ] Handle request timeouts
  - [ ] Implement request cancellation

- [ ] **9.3 Graceful Shutdown**
  - [ ] Listen for system signals (SIGTERM, SIGINT)
  - [ ] Stop accepting new requests
  - [ ] Wait for existing requests to complete

- [ ] **9.4 Advanced Features**
  - [ ] Implement connection limit
  - [ ] Add request rate limiting
  - [ ] Implement simple Web Framework

---

### 10. Message Queue

> Focus: `channel`, persistence, consumer groups

- [ ] **10.1 In-Memory Queue**
  - [ ] Implement FIFO queue
  - [ ] Support multiple producers/consumers
  - [ ] Implement backpressure mechanism

- [ ] **10.2 Persistence**
  - [ ] Implement WAL (Write-Ahead Log)
  - [ ] Support recovery after restart
  - [ ] Implement checkpointing

- [ ] **10.3 Consumer Groups**
  - [ ] Implement consumer groups
  - [ ] Support partitions
  - [ ] Implement offset management

- [ ] **10.4 Advanced Features**
  - [ ] Implement dead letter queue
  - [ ] Support delayed message delivery
  - [ ] Implement message retry mechanism

---

### 11. Distributed Lock

> Focus: Mutex semantics, lease renewal, Fencing Token

- [ ] **11.1 Understanding Distributed Lock Concepts**
  - [ ] Study distributed lock use cases
  - [ ] Understand CAP theorem and its relation to locks
  - [ ] Analyze common lock implementation approaches

- [ ] **11.2 Redis-based Distributed Lock**
  - [ ] Implement basic lock using SETNX
  - [ ] Implement lock expiration and auto-release
  - [ ] Handle lock timeout issues

- [ ] **11.3 Lease Renewal and Fencing Token**
  - [ ] Implement automatic lease renewal
  - [ ] Use Fencing Token to prevent split-brain
  - [ ] Handle network partition scenarios

- [ ] **11.4 Advanced Implementation**
  - [ ] Implement Redlock algorithm
  - [ ] Implement distributed lock based on etcd
  - [ ] Implement reentrant lock

---

### 12. Chat Room / WebSocket

> Focus: multi-goroutine management, broadcasting, connection tracking

- [ ] **12.1 WebSocket Basics**
  - [ ] Establish connections using `gorilla/websocket`
  - [ ] Implement heartbeat mechanism
  - [ ] Handle connection disconnection

- [ ] **12.2 Connection Management**
  - [ ] Implement Hub to manage all connections
  - [ ] Use goroutines to handle each connection
  - [ ] Implement connection tracking and cleanup

- [ ] **12.3 Message Broadcasting**
  - [ ] Implement broadcast functionality
  - [ ] Support private messaging
  - [ ] Implement chat rooms/channels

- [ ] **12.4 Advanced Features**
  - [ ] Implement message history
  - [ ] Support user presence status
  - [ ] Add message encryption

---

### 13. Log Aggregator

> Focus: high-throughput writes, buffer management

- [ ] **13.1 Basic Log Collector**
  - [ ] Implement asynchronous writing
  - [ ] Use buffered channel as buffer
  - [ ] Support multi-source collection

- [ ] **13.2 Batch Writing**
  - [ ] Implement batch write optimization
  - [ ] Set flush conditions (time/count)
  - [ ] Handle buffer full scenarios

- [ ] **13.3 High-Performance Optimization**
  - [ ] Reduce memory allocations
  - [ ] Use `sync.Pool` for object reuse
  - [ ] Implement lock-free queue (optional)

- [ ] **13.4 Advanced Features**
  - [ ] Support log rotation
  - [ ] Implement log compression
  - [ ] Support multiple output formats

---

## 📚 Learning Resources

### Books
- [ ] "Concurrency in Go" - Katherine Cox-Buday
- [ ] "The Go Programming Language" - Alan Donovan & Brian Kernighan

### Online Resources
- [ ] [Go Concurrency Patterns](https://go.dev/blog/pipelines) - Official Go Blog
- [ ] [Go Memory Model](https://go.dev/ref/mem) - Official Documentation
- [ ] [Race Detector](https://go.dev/doc/articles/race_detector) - Official Guide

### Tools
- [ ] Master `go test -race`
- [ ] Master `go tool pprof`
- [ ] Master `go tool trace`
