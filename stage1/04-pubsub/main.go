/*
Pub/Sub Demo - Interactive CLI for testing the publish/subscribe system.

# Workflow Overview

	                          ┌─────────────┐
	                          │   Broker    │
	                          │             │
	                          │ topics map  │
	                          │ subs map    │
	                          └──────┬──────┘
	                                 │
	        ┌────────────────────────┼─────────────────────────┐
	        │                        │                         │
	        ▼                        ▼                         ▼
	  ┌───────────┐           ┌───────────┐            ┌───────────┐
	  │ Topic: A  │           │ Topic: B  │            │ Topic: C  │
	  └─────┬─────┘           └─────┬─────┘            └─────┬─────┘
	        │                       │                        │
	   ┌────┴────┐             ┌────┴────┐              ┌────┴────┐
	   ▼         ▼             ▼         ▼              ▼         ▼
	┌─────┐   ┌─────┐       ┌─────┐   ┌─────┐       ┌─────┐   ┌─────┐
	│Sub 1│   │Sub 2│       │Sub 2│   │Sub 3│       │Sub 1│   │Sub 3│
	└─────┘   └─────┘       └─────┘   └─────┘       └─────┘   └─────┘

# Publish Flow (Fan-Out)

	Publisher                    Broker                     Subscribers
	    │                          │                             │
	    │  Publish("topicA", msg)  │                             │
	    │─────────────────────────▶│                             │
	    │                          │                             │
	    │                          │  Lookup subscribers for     │
	    │                          │  "topicA"                   │
	    │                          │                             │
	    │                          │  Non-blocking send to       │
	    │                          │  each subscriber channel    │
	    │                          │────────────────────────────▶│ Sub1
	    │                          │────────────────────────────▶│ Sub2
	    │                          │                             │
	    │  return delivered count  │                             │
	    │◀─────────────────────────│                             │

# Key Features

  - Non-blocking sends: slow subscribers don't block publishers
  - Thread-safe: RWMutex protects broker state
  - Buffered channels: absorb bursts, prevent backpressure
  - Graceful cleanup: Close() stops all subscribers

Usage:

	go run main.go
*/
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"pubsub/pubsub"
)

func main() {
	fmt.Println("=================================")
	fmt.Println("   Pub/Sub Demo - Interactive CLI")
	fmt.Println("=================================")
	fmt.Println()

	broker := pubsub.NewBroker()
	subscribers := make(map[string]*pubsub.Subscriber)
	var wg sync.WaitGroup

	printHelp()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("\n> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "help", "h":
			printHelp()

		case "new", "n":
			if len(parts) < 2 {
				fmt.Println("Usage: new <subscriber-id>")
			} else {
				id := parts[1]
				if _, exists := subscribers[id]; exists {
					fmt.Printf("Subscriber '%s' already exists\n", id)
				} else {
					sub := pubsub.NewSubscriber(id, 10)
					subscribers[id] = sub

					// Start goroutine to listen for messages
					wg.Add(1)
					go func(s *pubsub.Subscriber, name string) {
						defer wg.Done()
						for msg := range s.Messages() {
							fmt.Printf("\n📩 [%s] received: topic=%s, payload=%v\n> ", name, msg.Topic, msg.Payload)
						}
					}(sub, id)

					fmt.Printf("✅ Created subscriber '%s'\n", id)
				}
			}

		case "sub", "s":
			if len(parts) < 3 {
				fmt.Println("Usage: sub <subscriber-id> <topic>")
			} else {
				id, topic := parts[1], parts[2]
				if sub, exists := subscribers[id]; exists {
					broker.Subscribe(sub, topic)
					fmt.Printf("✅ '%s' subscribed to '%s'\n", id, topic)
				} else {
					fmt.Printf("❌ Subscriber '%s' not found\n", id)
				}
			}

		case "unsub", "u":
			if len(parts) < 3 {
				fmt.Println("Usage: unsub <subscriber-id> <topic>")
			} else {
				id, topic := parts[1], parts[2]
				if sub, exists := subscribers[id]; exists {
					broker.Unsubscribe(sub, topic)
					fmt.Printf("✅ '%s' unsubscribed from '%s'\n", id, topic)
				} else {
					fmt.Printf("❌ Subscriber '%s' not found\n", id)
				}
			}

		case "pub", "p":
			if len(parts) < 3 {
				fmt.Println("Usage: pub <topic> <message...>")
			} else {
				topic := parts[1]
				message := strings.Join(parts[2:], " ")
				count := broker.Publish(context.Background(), topic, message)
				fmt.Printf("📤 Published to '%s': delivered to %d subscriber(s)\n", topic, count)
			}

		case "list", "l":
			fmt.Println("\n📋 Subscribers:")
			for id, sub := range subscribers {
				topics := sub.Topics()
				if len(topics) == 0 {
					fmt.Printf("  - %s (no subscriptions)\n", id)
				} else {
					fmt.Printf("  - %s → [%s]\n", id, strings.Join(topics, ", "))
				}
			}
			fmt.Printf("\n📊 Topics: %d\n", broker.TopicCount())

		case "remove", "r":
			if len(parts) < 2 {
				fmt.Println("Usage: remove <subscriber-id>")
			} else {
				id := parts[1]
				if sub, exists := subscribers[id]; exists {
					broker.RemoveSubscriber(sub)
					delete(subscribers, id)
					fmt.Printf("✅ Removed subscriber '%s'\n", id)
				} else {
					fmt.Printf("❌ Subscriber '%s' not found\n", id)
				}
			}

		case "demo", "d":
			runDemo(broker, subscribers, &wg)

		case "fanout", "f":
			runFanoutDemo()

		case "quit", "q", "exit":
			fmt.Println("\n👋 Shutting down...")
			// Close all subscribers (not just those in broker)
			for _, sub := range subscribers {
				sub.Close()
			}
			broker.Close()
			wg.Wait()
			fmt.Println("Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help' for commands)\n", cmd)
		}

		fmt.Print("> ")
	}
}

func printHelp() {
	fmt.Println(`Commands:
  new  <id>              Create a new subscriber
  sub  <id> <topic>      Subscribe to a topic
  unsub <id> <topic>     Unsubscribe from a topic
  pub  <topic> <msg>     Publish a message to a topic
  list                   List all subscribers
  remove <id>            Remove a subscriber
  demo                   Run a pub/sub demo
  fanout                 Run a fan-out/fan-in demo
  quit                   Exit the program

Shortcuts: n=new, s=sub, u=unsub, p=pub, l=list, r=remove, d=demo, f=fanout, q=quit`)
}

func runDemo(broker *pubsub.Broker, subs map[string]*pubsub.Subscriber, wg *sync.WaitGroup) {
	fmt.Println("\n🎬 Running demo...")

	// Create demo subscribers if not exist
	for _, id := range []string{"alice", "bob"} {
		if _, exists := subs[id]; !exists {
			sub := pubsub.NewSubscriber(id, 10)
			subs[id] = sub
			wg.Add(1)
			go func(s *pubsub.Subscriber, name string) {
				defer wg.Done()
				for msg := range s.Messages() {
					fmt.Printf("\n📩 [%s] received: topic=%s, payload=%v\n> ", name, msg.Topic, msg.Payload)
				}
			}(sub, id)
		}
	}

	// Subscribe
	broker.Subscribe(subs["alice"], "news")
	broker.Subscribe(subs["alice"], "sports")
	broker.Subscribe(subs["bob"], "news")

	fmt.Println("✅ alice subscribed to: news, sports")
	fmt.Println("✅ bob subscribed to: news")

	// Publish
	fmt.Println("\n📤 Publishing messages...")
	broker.Publish(context.Background(), "news", "Breaking: Go 1.22 released!")
	broker.Publish(context.Background(), "sports", "Team wins championship!")

	fmt.Println("\n🎬 Demo complete! Check messages above.")
}

func runFanoutDemo() {
	fmt.Println("\n🎬 Fan-Out / Fan-In / Pipeline Demo")
	fmt.Println("====================================\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ============ FAN-OUT DEMO ============
	fmt.Println("📤 FAN-OUT: One source → Multiple workers")
	fmt.Println("   Source ─┬──► Worker 1")
	fmt.Println("           ├──► Worker 2")
	fmt.Println("           └──► Worker 3\n")

	input := make(chan int)
	outputs := pubsub.FanOut(ctx, input, 3)

	// Send data
	go func() {
		for i := 1; i <= 3; i++ {
			input <- i
		}
		close(input)
	}()

	// Collect from workers
	var wg sync.WaitGroup
	for i, out := range outputs {
		wg.Add(1)
		go func(workerID int, ch <-chan int) {
			defer wg.Done()
			for val := range ch {
				fmt.Printf("   Worker %d received: %d\n", workerID+1, val)
			}
		}(i, out)
	}
	wg.Wait()

	// ============ FAN-IN DEMO ============
	fmt.Println("\n📥 FAN-IN: Multiple sources → One collector")
	fmt.Println("   Source 1 ───┐")
	fmt.Println("   Source 2 ───┼──► Collector")
	fmt.Println("   Source 3 ───┘\n")

	ch1 := make(chan int, 1)
	ch2 := make(chan int, 1)
	ch3 := make(chan int, 1)

	ch1 <- 10
	ch2 <- 20
	ch3 <- 30
	close(ch1)
	close(ch2)
	close(ch3)

	merged := pubsub.FanIn(ctx, ch1, ch2, ch3)
	for val := range merged {
		fmt.Printf("   Collector received: %d\n", val)
	}

	// ============ PIPELINE DEMO ============
	fmt.Println("\n🔗 PIPELINE: Chain of transformations")
	fmt.Println("   Input ──► ×2 ──► +10 ──► Output\n")

	pipeInput := make(chan int)
	result := pubsub.Pipeline(ctx, pipeInput,
		func(x int) int {
			fmt.Printf("   Stage 1: %d × 2 = %d\n", x, x*2)
			return x * 2
		},
		func(x int) int {
			fmt.Printf("   Stage 2: %d + 10 = %d\n", x, x+10)
			return x + 10
		},
	)

	go func() {
		pipeInput <- 5
		close(pipeInput)
	}()

	for val := range result {
		fmt.Printf("   Final output: %d\n", val)
	}

	// ============ BROADCAST DEMO ============
	fmt.Println("\n📢 BROADCAST: One sender → Multiple subscribers")
	fmt.Println("   Sender ─┬──► Sub A")
	fmt.Println("           └──► Sub B\n")

	bc := pubsub.NewBroadcast[string](10)
	subA := bc.Subscribe(10)
	subB := bc.Subscribe(10)

	bc.Send("Hello!")
	bc.Send("World!")

	// Read from subscribers
	fmt.Printf("   Sub A received: %s, %s\n", <-subA, <-subA)
	fmt.Printf("   Sub B received: %s, %s\n", <-subB, <-subB)

	bc.Close()

	fmt.Println("\n🎬 Fan-out demo complete!")
}
