# Project 4: Pub/Sub System

## Overview

A publish-subscribe messaging system where publishers send messages to topics and subscribers receive messages from topics they're interested in.

## Key Concepts

### Fan-Out Pattern
```
Publisher ────► Topic ─┬──► Subscriber 1
                       ├──► Subscriber 2
                       └──► Subscriber 3
```

### Fan-In Pattern
```
Source 1 ───┐
Source 2 ───┼──► Merged Output
Source 3 ───┘
```

## Project Structure

```
04-pubsub/
├── pubsub/
│   ├── broker.go       # Message broker
│   ├── topic.go        # Topic management
│   ├── subscriber.go   # Subscriber implementation
│   └── pubsub_test.go  # Tests
└── README.md
```

## Progress

- [x] Step 4.1: Basic Pub/Sub
- [x] Step 4.2: Fan-out/Fan-in
