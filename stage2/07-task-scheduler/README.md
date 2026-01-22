# Project 7: Task Scheduler

> Focus: `time.Timer`, `time.Ticker`, `sync.Cond`, scheduled tasks

## Overview

A task scheduler that supports:
- Delayed execution (run once after delay)
- Interval execution (run repeatedly)
- Cron-like scheduling

## Key Concepts

### time.Timer vs time.Ticker

```go
// Timer: fire ONCE after duration
timer := time.NewTimer(5 * time.Second)
<-timer.C  // Blocks until timer fires

// Ticker: fire REPEATEDLY at interval
ticker := time.NewTicker(1 * time.Second)
for range ticker.C {
    // Fires every second
}
```

### Scheduler Architecture

```
                    ┌─────────────┐
                    │  Scheduler  │
                    │             │
                    │ tasks map   │
                    │ timer heap  │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   ┌─────────┐       ┌─────────┐        ┌─────────┐
   │ Task A  │       │ Task B  │        │ Task C  │
   │ Once    │       │ Interval│        │ Cron    │
   │ @5s     │       │ @1min   │        │ 0 * * * │
   └─────────┘       └─────────┘        └─────────┘
```

## Project Structure

```
stage2/07-task-scheduler/
├── scheduler/
│   ├── scheduler.go    # Core scheduler
│   ├── task.go         # Task types
│   └── cron.go         # Cron parsing (Step 7.2)
├── go.mod
└── README.md
```

## Progress

- [x] Step 7.1: Basic Scheduler (Timer, Ticker)
- [x] Step 7.2: Cron-like Scheduler
- [ ] Step 7.3: Task Management
