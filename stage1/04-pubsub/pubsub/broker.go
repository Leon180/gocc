// Package pubsub implements a publish/subscribe messaging system.
// See main.go for workflow documentation and CLI demo.
package pubsub

import (
	"context"
	"sync"
)

// Message represents a message in the pub/sub system.
//
// Thread Safety:
//   - Topic (string) is immutable and safe to share.
//   - Payload (any) is copied by reference if it's a pointer, slice, or map.
//     For thread safety, ensure Payload is immutable or deep-copy before publishing.
type Message struct {
	Topic   string
	Payload any
}

// Subscriber represents a message subscriber.
type Subscriber struct {
	id       string
	messages chan Message
	topics   map[string]bool
	mu       sync.RWMutex
	closed   bool
}

// NewSubscriber creates a new subscriber with the given buffer size.
func NewSubscriber(id string, bufferSize int) *Subscriber {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &Subscriber{
		id:       id,
		messages: make(chan Message, bufferSize),
		topics:   make(map[string]bool),
	}
}

// ID returns the subscriber's unique identifier.
func (s *Subscriber) ID() string {
	return s.id
}

// Messages returns the channel to receive messages from.
func (s *Subscriber) Messages() <-chan Message {
	return s.messages
}

// Subscribe adds a topic to this subscriber's interests.
func (s *Subscriber) Subscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topic] = true
}

// Unsubscribe removes a topic from this subscriber's interests.
func (s *Subscriber) Unsubscribe(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.topics, topic)
}

// IsSubscribed checks if subscriber is interested in a topic.
func (s *Subscriber) IsSubscribed(topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topics[topic]
}

// Topics returns a list of topics this subscriber is interested in.
func (s *Subscriber) Topics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	topics := make([]string, 0, len(s.topics))
	for t := range s.topics {
		topics = append(topics, t)
	}
	return topics
}

// Close closes the subscriber's message channel.
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.messages)
	}
}

// send attempts to send a message to the subscriber.
// Returns false if the subscriber is closed or buffer is full (non-blocking).
func (s *Subscriber) send(msg Message) bool {
	// Check subscriber's status
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return false
	}
	s.mu.RUnlock()

	// Non-blocking send to prevent slow subscribers from blocking
	select {
	case s.messages <- msg:
		return true
	default:
		// Buffer full, message dropped
		return false
	}
}

// Broker manages topics and subscribers.
type Broker struct {
	subscribers map[string]*Subscriber // subscriber ID -> Subscriber
	topics      map[string][]string    // topic -> subscriber IDs
	mu          sync.RWMutex
	closed      bool
}

// NewBroker creates a new message broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string]*Subscriber),
		topics:      make(map[string][]string),
	}
}

// Subscribe registers a subscriber for a topic.
func (b *Broker) Subscribe(sub *Subscriber, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Register subscriber if not already registered
	if _, exists := b.subscribers[sub.ID()]; !exists {
		b.subscribers[sub.ID()] = sub
	}

	// Add to topic
	sub.Subscribe(topic)
	b.topics[topic] = append(b.topics[topic], sub.ID())
}

// Unsubscribe removes a subscriber from a topic.
func (b *Broker) Unsubscribe(sub *Subscriber, topic string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub.Unsubscribe(topic)

	// Remove from topic list
	subs := b.topics[topic]
	for i, id := range subs {
		if id == sub.ID() {
			b.topics[topic] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
}

// Publish sends a message to all subscribers of a topic.
// This is the fan-out operation: one message → multiple subscribers.
func (b *Broker) Publish(ctx context.Context, topic string, payload any) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return 0
	}

	msg := Message{Topic: topic, Payload: payload}
	delivered := 0

	// Fan-out to all subscribers of this topic
	for _, subID := range b.topics[topic] {
		if sub, exists := b.subscribers[subID]; exists {
			if sub.send(msg) {
				delivered++
			}
		}
	}

	return delivered
}

// RemoveSubscriber completely removes a subscriber from the broker.
func (b *Broker) RemoveSubscriber(sub *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove from all topics
	for topic := range b.topics {
		subs := b.topics[topic]
		for i, id := range subs {
			if id == sub.ID() {
				b.topics[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}

	// Remove from subscribers map
	delete(b.subscribers, sub.ID())

	// Close the subscriber
	sub.Close()
}

// Close shuts down the broker and all subscribers.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.closed = true

	for _, sub := range b.subscribers {
		sub.Close()
	}

	// Clear maps
	b.subscribers = make(map[string]*Subscriber)
	b.topics = make(map[string][]string)
}

// SubscriberCount returns the number of subscribers for a topic.
func (b *Broker) SubscriberCount(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.topics[topic])
}

// TopicCount returns the number of topics.
func (b *Broker) TopicCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.topics)
}
