# 📖 Go Concurrency Skills & Standards Guide

> Clear criteria and standards to follow for each project in your learning journey

## 🎯 How to Use This Guide

For each project you complete:
1. **Before Starting**: Read the skill requirements and acceptance criteria
2. **During Development**: Follow the code standards and testing requirements
3. **After Completing**: Use the self-assessment checklist to verify mastery
4. **Document**: Record your learnings in the project's own README

---

## 📐 Universal Standards (Apply to ALL Projects)

### Code Quality Standards

| Aspect | Requirement |
|--------|-------------|
| **Formatting** | Run `gofmt` or `goimports` before committing |
| **Linting** | Pass `golangci-lint run` with no errors |
| **Race Detection** | Pass `go test -race ./...` with no data races |
| **Documentation** | All exported functions/types have GoDoc comments |
| **Error Handling** | No ignored errors; wrap errors with context |
| **Naming** | Follow Go naming conventions (camelCase, clear intent) |

### Testing Requirements

| Type | Minimum Coverage | Notes |
|------|------------------|-------|
| **Unit Tests** | 70%+ | Test core logic and edge cases |
| **Benchmark Tests** | Required for performance-critical code | Compare implementations |
| **Race Tests** | 100% pass | Use `go test -race` |
| **Integration Tests** | Where applicable | Test component interactions |

### Project Structure

```
project-name/
├── README.md           # Project overview, learning outcomes, usage
├── main.go             # Entry point (if applicable)
├── {package}/          # Core implementation
│   ├── {file}.go       # Implementation
│   └── {file}_test.go  # Tests
├── examples/           # Usage examples
└── benchmarks/         # Benchmark results
```

### Documentation Template

Each project README should include:

```markdown
# Project Name

## Overview
Brief description of what this project does.

## Learning Objectives
- [ ] Skill 1
- [ ] Skill 2

## Key Concepts Learned
Explain the concurrency concepts you practiced.

## Usage
How to run the project.

## Testing
How to run tests and benchmarks.

## Challenges & Solutions
Problems you encountered and how you solved them.

## References
Links to resources that helped you.
```

---

## 🥇 Phase 1: Beginner → Intermediate

### Project 1: Concurrent Counter / Rate Limiter

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| `sync.Mutex` usage | ⭐⭐⭐ Comfortable | Can explain when to use Lock/Unlock |
| `sync/atomic` operations | ⭐⭐⭐ Comfortable | Know Add, Load, Store, CompareAndSwap |
| Race condition detection | ⭐⭐⭐ Expert | Can identify races from `-race` output |
| Lock vs atomic trade-offs | ⭐⭐⭐ Comfortable | Can choose appropriate solution |

#### Acceptance Criteria
- [ ] Counter is thread-safe (no race conditions)
- [ ] Benchmark comparing Mutex vs atomic implementations
- [ ] Rate limiter correctly limits requests per time window
- [ ] All edge cases handled (negative values, overflow, etc.)
- [ ] Documentation explains the algorithms used

#### Self-Assessment Questions
1. When would you choose Mutex over atomic? (Answer: complex operations, multiple fields)
2. What happens if you forget to Unlock? (Answer: deadlock)
3. What's the difference between `atomic.AddInt64` and `atomic.CompareAndSwapInt64`?
4. How does Token Bucket differ from Leaky Bucket?

---

### Project 2: Worker Pool

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Goroutine lifecycle | ⭐⭐⭐ Expert | Can start, stop, and manage goroutines |
| Channel operations | ⭐⭐⭐ Expert | Send, receive, close, range over channels |
| `context.Context` | ⭐⭐⭐ Comfortable | Use for cancellation and timeouts |
| Graceful shutdown | ⭐⭐⭐ Expert | No goroutine leaks, clean termination |

#### Acceptance Criteria
- [ ] Fixed pool of N workers processing jobs from a channel
- [ ] Results are collected and returned in order (or documented if not)
- [ ] Supports graceful shutdown via context cancellation
- [ ] No goroutine leaks (verify with runtime.NumGoroutine)
- [ ] Handles panics in worker goroutines gracefully
- [ ] Benchmark shows linear scaling with worker count

#### Self-Assessment Questions
1. How do you prevent goroutine leaks?
2. What happens when you send to a closed channel?
3. How do you ensure all workers finish before main exits?
4. When should you use buffered vs unbuffered channels?

---

### Project 3: Simple Web Crawler

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| `sync.WaitGroup` | ⭐⭐⭐ Expert | Add, Done, Wait correctly |
| Semaphore pattern | ⭐⭐⭐ Comfortable | Limit concurrent operations |
| URL deduplication | ⭐⭐ Familiar | Efficient visited URL tracking |
| HTTP client usage | ⭐⭐ Familiar | Timeouts, connection reuse |

#### Acceptance Criteria
- [ ] Crawls pages concurrently with configurable limit
- [ ] Tracks visited URLs to avoid duplicates
- [ ] Respects robots.txt (bonus)
- [ ] Implements retry with exponential backoff
- [ ] Handles HTTP errors gracefully
- [ ] Collects and outputs crawled data

#### Self-Assessment Questions
1. Why use buffered channel as semaphore?
2. How do you prevent the same URL from being crawled twice?
3. What's a reasonable timeout for HTTP requests?
4. How do you handle temporarily failed requests?

---

### Project 4: Pub/Sub System

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Fan-out pattern | ⭐⭐⭐ Expert | One input → multiple outputs |
| Fan-in pattern | ⭐⭐⭐ Expert | Multiple inputs → one output |
| Topic routing | ⭐⭐ Comfortable | Route messages by topic |
| Subscriber management | ⭐⭐⭐ Comfortable | Add/remove subscribers dynamically |

#### Acceptance Criteria
- [ ] Publishers can send messages to topics
- [ ] Multiple subscribers receive messages from same topic
- [ ] Fan-out correctly distributes messages
- [ ] Fan-in correctly merges multiple streams
- [ ] Slow subscribers don't block fast ones
- [ ] Subscribers can be added/removed at runtime

#### Self-Assessment Questions
1. How do you prevent slow subscribers from blocking publishers?
2. What happens when a subscriber disconnects mid-message?
3. How would you implement message filtering?
4. What's the difference between fan-out and broadcasting?

---

## 🥈 Phase 2: Intermediate → Advanced

### Project 5: Cache System

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| `sync.RWMutex` | ⭐⭐⭐ Expert | Read-write lock usage |
| TTL implementation | ⭐⭐⭐ Comfortable | Passive and active expiration |
| LRU/LFU algorithms | ⭐⭐⭐ Comfortable | Implement eviction strategies |
| Generics | ⭐⭐ Familiar | Type-safe cache for any type |

#### Acceptance Criteria
- [ ] Thread-safe Get/Set/Delete operations
- [ ] TTL expiration works correctly (both passive and active)
- [ ] LRU eviction when capacity reached
- [ ] High read concurrency (RWMutex performance)
- [ ] Cache hit/miss statistics
- [ ] Benchmark comparing with sync.Map

#### Self-Assessment Questions
1. When to use RWMutex vs regular Mutex?
2. What's the trade-off between passive and active expiration?
3. How do you prevent cache stampede?
4. How would you implement distributed cache invalidation?

---

### Project 6: Connection Pool

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Channel as semaphore | ⭐⭐⭐ Expert | Limit concurrent resource usage |
| Resource lifecycle | ⭐⭐⭐ Expert | Create, validate, destroy |
| Timeout handling | ⭐⭐⭐ Comfortable | Acquire with timeout |
| Health checking | ⭐⭐ Comfortable | Validate connections |

#### Acceptance Criteria
- [ ] Pool manages fixed number of connections
- [ ] Acquire blocks when pool is empty
- [ ] Acquire with timeout returns error
- [ ] Release returns connection to pool
- [ ] Dead connections are detected and replaced
- [ ] Pool can be gracefully closed

#### Self-Assessment Questions
1. Why use channel instead of slice for pool?
2. How do you detect a dead connection?
3. What happens if Release is called twice on same connection?
4. How would you implement connection warm-up?

---

### Project 7: Task Scheduler

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| `time.Timer/Ticker` | ⭐⭐⭐ Expert | Scheduling with time package |
| `sync.Cond` | ⭐⭐⭐ Comfortable | Wait and signal patterns |
| Cron parsing | ⭐⭐ Familiar | Parse cron expressions |
| Priority queues | ⭐⭐ Familiar | Heap-based scheduling |

#### Acceptance Criteria
- [ ] Schedule one-time delayed tasks
- [ ] Schedule recurring tasks at intervals
- [ ] Parse and execute cron expressions
- [ ] Cancel scheduled tasks
- [ ] Handle task failures gracefully
- [ ] Support task priorities

#### Self-Assessment Questions
1. What's the difference between Timer and Ticker?
2. Why use sync.Cond instead of channel for waiting?
3. How do you ensure precision for second-level scheduling?
4. How would you persist scheduled tasks?

---

### Project 8: Concurrent Map

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| `sync.Map` | ⭐⭐⭐ Comfortable | Know when to use |
| Sharded locks | ⭐⭐⭐ Expert | Reduce lock contention |
| Hash functions | ⭐⭐ Familiar | Consistent distribution |
| Benchmarking | ⭐⭐⭐ Expert | Measure lock contention |

#### Acceptance Criteria
- [ ] Implement map with sharded locks
- [ ] Benchmark vs sync.Map and map+Mutex
- [ ] Demonstrate when each approach wins
- [ ] Dynamic shard count adjustment (bonus)
- [ ] Comprehensive benchmarks with various read/write ratios

#### Self-Assessment Questions
1. When does sync.Map outperform sharded map?
2. How many shards should you use?
3. What makes a good hash function for sharding?
4. How do you measure lock contention?

---

## 🥉 Phase 3: Advanced → Production

### Project 9: HTTP Server

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Request context | ⭐⭐⭐ Expert | Propagate and cancel |
| Middleware pattern | ⭐⭐⭐ Expert | Chain handlers |
| Graceful shutdown | ⭐⭐⭐ Expert | Clean termination |
| Connection limiting | ⭐⭐⭐ Comfortable | Prevent overload |

#### Acceptance Criteria
- [ ] Route requests to handlers
- [ ] Middleware chain (logging, auth, rate limiting)
- [ ] Request timeout handling
- [ ] Graceful shutdown on SIGTERM/SIGINT
- [ ] Connection limit enforced
- [ ] Context properly propagated to handlers

---

### Project 10: Message Queue

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| WAL implementation | ⭐⭐⭐ Comfortable | Write-ahead logging |
| Consumer groups | ⭐⭐⭐ Comfortable | Parallel consumption |
| Backpressure | ⭐⭐⭐ Expert | Handle slow consumers |
| Offset management | ⭐⭐ Familiar | Track consumption progress |

#### Acceptance Criteria
- [ ] Durable message storage (survives restart)
- [ ] Multiple consumer groups
- [ ] At-least-once delivery guarantee
- [ ] Dead letter queue for failed messages
- [ ] Backpressure mechanism

---

### Project 11: Distributed Lock

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Lock semantics | ⭐⭐⭐ Expert | Mutual exclusion guarantees |
| Lease renewal | ⭐⭐⭐ Expert | Prevent stale locks |
| Fencing tokens | ⭐⭐⭐ Comfortable | Handle split-brain |
| Consensus basics | ⭐⭐ Familiar | Understand Redlock |

#### Acceptance Criteria
- [ ] Lock prevents concurrent access
- [ ] Lock auto-expires to prevent deadlocks
- [ ] Lease renewal prevents premature expiration
- [ ] Fencing token for safe resource access
- [ ] Handles network partitions gracefully

---

### Project 12: Chat Room / WebSocket

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| WebSocket handling | ⭐⭐⭐ Expert | Connection lifecycle |
| Hub pattern | ⭐⭐⭐ Expert | Centralized connection management |
| Broadcasting | ⭐⭐⭐ Expert | Fan-out to all connections |
| Presence tracking | ⭐⭐ Comfortable | Track online users |

#### Acceptance Criteria
- [ ] Handle WebSocket connections
- [ ] Broadcast messages to all connected clients
- [ ] Support private messaging
- [ ] Track user presence (online/offline)
- [ ] Handle disconnections gracefully
- [ ] Implement heartbeat/ping-pong

---

### Project 13: Log Aggregator

#### Skills to Master
| Skill | Proficiency Level | How to Verify |
|-------|-------------------|---------------|
| Batch processing | ⭐⭐⭐ Expert | Buffer and flush |
| `sync.Pool` | ⭐⭐⭐ Comfortable | Object reuse |
| High-throughput writes | ⭐⭐⭐ Expert | Minimize allocations |
| Backpressure | ⭐⭐⭐ Expert | Handle buffer overflow |

#### Acceptance Criteria
- [ ] Collect logs from multiple sources
- [ ] Batch writes for efficiency
- [ ] Handle high throughput (>100K logs/sec target)
- [ ] Graceful degradation under load
- [ ] Log rotation support
- [ ] Multiple output formats (JSON, text)

---

## 🏆 Proficiency Scale

| Level | Rating | Description |
|-------|--------|-------------|
| ⭐ | Beginner | Can use with documentation |
| ⭐⭐ | Familiar | Can use independently |
| ⭐⭐⭐ | Comfortable | Can teach others |
| ⭐⭐⭐⭐ | Expert | Can design systems using this |
| ⭐⭐⭐⭐⭐ | Master | Can optimize and extend |

---

## 📋 Project Completion Checklist

Use this checklist when you think a project is complete:

```markdown
## Final Review Checklist

### Code Quality
- [ ] Code passes `gofmt`
- [ ] Code passes `golangci-lint`
- [ ] No race conditions (`go test -race`)
- [ ] Exported items have GoDoc comments

### Testing
- [ ] Unit tests pass
- [ ] Test coverage > 70%
- [ ] Benchmark tests exist
- [ ] Edge cases covered

### Documentation
- [ ] README.md complete
- [ ] Usage examples provided
- [ ] Key decisions documented

### Self-Assessment
- [ ] Can explain the code to someone else
- [ ] Understand all concurrency primitives used
- [ ] Know the trade-offs of design choices
- [ ] Recorded learnings in notes
```

---

## 📝 Learning Journal Template

After completing each project, add an entry:

```markdown
## [Date] - [Project Name]

### What I Learned
- Point 1
- Point 2

### Challenges Faced
1. Challenge and how I solved it

### Key Takeaways
- Insight about concurrency

### Questions for Further Study
- Things I want to explore more

### Time Spent
- Estimated: X hours
- Actual: Y hours
```

---

> 💡 **Remember**: The goal is not just to complete projects, but to truly understand the concurrency concepts. Take time to experiment, break things, and learn from mistakes!
