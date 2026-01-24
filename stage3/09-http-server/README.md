# Project 9: HTTP Server

> Focus: Request handling, Middleware, Context, Graceful Shutdown

## Overview

Build a production-grade HTTP server from scratch using Go's standard `net/http` package.
This project avoids external frameworks (like Gin or Chi) to ensure deep understanding of HTTP server fundamentals and concurrency patterns.

## Learning Objectives

- [ ] **9.1 Basic HTTP Server**: `net/http`, `http.Handler`, `http.ServeMux`
- [ ] **9.2 Middleware Patterns**: Chaining, wrapping handlers, panic recovery
- [ ] **9.3 Context & Timeouts**: `context.Context` propagation, cancellation
- [ ] **9.4 Advanced**: Graceful shutdown, rate limiting

## Structure

```
stage3/09-http-server/
├── server/
│   ├── server.go       # Server lifecycle
│   ├── handler.go      # Route handlers
│   └── middleware.go   # Middleware chain
├── client/
│   └── main.go         # Demo client
├── main.go             # Entry point
└── README.md           # This file
```

## Running

```bash
# Start server
go run main.go

# In another terminal, run client
go run client/main.go
```
