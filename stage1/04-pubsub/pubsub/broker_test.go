package pubsub

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBroker_BasicPubSub(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Create subscriber
	sub := NewSubscriber("sub1", 10)
	broker.Subscribe(sub, "news")

	// Publish message
	ctx := context.Background()
	delivered := broker.Publish(ctx, "news", "Hello World")

	if delivered != 1 {
		t.Errorf("Expected 1 delivery, got %d", delivered)
	}

	// Receive message
	select {
	case msg := <-sub.Messages():
		if msg.Topic != "news" {
			t.Errorf("Expected topic 'news', got '%s'", msg.Topic)
		}
		if msg.Payload != "Hello World" {
			t.Errorf("Expected payload 'Hello World', got '%v'", msg.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for message")
	}
}

func TestBroker_MultipleSubscribers(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Create multiple subscribers
	sub1 := NewSubscriber("sub1", 10)
	sub2 := NewSubscriber("sub2", 10)
	sub3 := NewSubscriber("sub3", 10)

	broker.Subscribe(sub1, "news")
	broker.Subscribe(sub2, "news")
	broker.Subscribe(sub3, "sports") // Different topic

	// Publish to news
	ctx := context.Background()
	delivered := broker.Publish(ctx, "news", "Breaking News")

	if delivered != 2 {
		t.Errorf("Expected 2 deliveries (fan-out), got %d", delivered)
	}

	// Both news subscribers should receive
	for _, sub := range []*Subscriber{sub1, sub2} {
		select {
		case msg := <-sub.Messages():
			if msg.Payload != "Breaking News" {
				t.Errorf("Wrong message: %v", msg.Payload)
			}
		case <-time.After(100 * time.Millisecond):
			t.Errorf("Subscriber %s didn't receive message", sub.ID())
		}
	}

	// Sports subscriber should NOT receive
	select {
	case <-sub3.Messages():
		t.Error("Sports subscriber should not receive news")
	case <-time.After(50 * time.Millisecond):
		// Expected - no message
	}
}

func TestBroker_Unsubscribe(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	sub := NewSubscriber("sub1", 10)
	broker.Subscribe(sub, "news")

	if broker.SubscriberCount("news") != 1 {
		t.Error("Expected 1 subscriber")
	}

	broker.Unsubscribe(sub, "news")

	if broker.SubscriberCount("news") != 0 {
		t.Error("Expected 0 subscribers after unsubscribe")
	}

	// Publish should deliver to 0
	ctx := context.Background()
	delivered := broker.Publish(ctx, "news", "test")

	if delivered != 0 {
		t.Errorf("Expected 0 deliveries, got %d", delivered)
	}
}

func TestBroker_RemoveSubscriber(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	sub := NewSubscriber("sub1", 10)
	broker.Subscribe(sub, "news")
	broker.Subscribe(sub, "sports")

	broker.RemoveSubscriber(sub)

	// Subscriber should be closed
	select {
	case _, ok := <-sub.Messages():
		if ok {
			t.Error("Channel should be closed")
		}
	default:
		// Channel might be empty but not closed yet
	}

	// No deliveries
	ctx := context.Background()
	if broker.Publish(ctx, "news", "test") != 0 {
		t.Error("Should have no deliveries")
	}
}

func TestBroker_SlowSubscriber(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Create subscriber with small buffer
	slowSub := NewSubscriber("slow", 2)
	fastSub := NewSubscriber("fast", 100)

	broker.Subscribe(slowSub, "news")
	broker.Subscribe(fastSub, "news")

	ctx := context.Background()

	// Publish many messages (more than slow buffer)
	for i := range 10 {
		broker.Publish(ctx, "news", i)
	}

	// Fast subscriber should get all
	fastCount := 0
	for {
		select {
		case <-fastSub.Messages():
			fastCount++
		default:
			goto checkSlow
		}
	}

checkSlow:
	// Slow subscriber should only get buffer size (2)
	slowCount := 0
	for {
		select {
		case <-slowSub.Messages():
			slowCount++
		default:
			goto done
		}
	}

done:
	if fastCount != 10 {
		t.Errorf("Fast subscriber expected 10, got %d", fastCount)
	}
	if slowCount != 2 {
		t.Errorf("Slow subscriber expected 2 (buffer size), got %d", slowCount)
	}
}

func TestBroker_ConcurrentPublish(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	sub := NewSubscriber("sub1", 1000)
	broker.Subscribe(sub, "news")

	var wg sync.WaitGroup
	ctx := context.Background()

	// Concurrent publishers
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				broker.Publish(ctx, "news", id*100+j)
			}
		}(i)
	}

	wg.Wait()

	// Count received messages
	count := 0
	for {
		select {
		case <-sub.Messages():
			count++
		default:
			goto done2
		}
	}

done2:
	if count != 1000 {
		t.Errorf("Expected 1000 messages, got %d", count)
	}
}

func TestSubscriber_MultipleTopic(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	sub := NewSubscriber("sub1", 10)
	broker.Subscribe(sub, "news")
	broker.Subscribe(sub, "sports")

	ctx := context.Background()
	broker.Publish(ctx, "news", "news1")
	broker.Publish(ctx, "sports", "sports1")

	received := make(map[string]bool)

	for range 2 {
		select {
		case msg := <-sub.Messages():
			received[msg.Topic] = true
		case <-time.After(100 * time.Millisecond):
			t.Error("Timeout")
		}
	}

	if !received["news"] || !received["sports"] {
		t.Errorf("Should receive from both topics: %v", received)
	}
}
