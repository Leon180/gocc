# Project 6: Connection Pool

## Overview

A generic connection pool that manages reusable connections using channels. This project focuses on:
- Channel as semaphore for limiting connections
- Resource lifecycle (create, validate, destroy)
- Timeout handling for acquire operations
- Health checking and connection replacement

## Project Structure

```
06-connection-pool/
├── pool/
│   ├── pool.go         # Core pool implementation
│   ├── options.go      # Configuration options
│   └── pool_test.go    # Tests
└── README.md
```

## Key Concepts

### Channel as Connection Store
```go
type Pool struct {
    conns chan Conn  // Buffered channel holds connections
}

// Acquire: receive from channel (blocks if empty)
conn := <-pool.conns

// Release: send back to channel
pool.conns <- conn
```

## Progress

- [x] Step 6.1: Basic Connection Pool
- [x] Step 6.2: Lifecycle Management
- [x] Step 6.3: Advanced Features (WarmUp, MinSize)
