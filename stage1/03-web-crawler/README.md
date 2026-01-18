# Project 3: Simple Web Crawler

## Overview

A concurrent web crawler that fetches pages, extracts links, and respects rate limits. This project focuses on:
- Using `sync.WaitGroup` for coordination
- Semaphore pattern for concurrency limiting
- URL deduplication with concurrent map
- HTTP client best practices

## Project Structure

```
03-web-crawler/
├── crawler/
│   ├── crawler.go        # Core crawler implementation
│   ├── fetcher.go        # HTTP fetching logic
│   ├── parser.go         # HTML link extraction
│   └── crawler_test.go   # Tests
├── examples/
│   └── main.go           # Usage examples
└── README.md
```

## Key Concepts

### Semaphore Pattern
```
Limit: 3 concurrent requests

[█ █ █ _ _]  ← 3 active, 2 waiting
    │
    └─► Buffered channel controls concurrency
```

## Progress

- [x] Step 3.1: Basic Web Crawling
- [x] Step 3.2: Concurrency Control
