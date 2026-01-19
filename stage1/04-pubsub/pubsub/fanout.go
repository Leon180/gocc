package pubsub

import (
	"context"
	"sync"
)

// FanOut distributes messages from one input channel to multiple output channels.
// Each output channel receives a copy of every message.
//
// Thread Safety:
//   - Simple values (string, int, struct without pointers) are safe.
//   - If T contains pointers, slices, or maps, the underlying data is shared.
//     In such cases, either ensure T is immutable or deep-copy before sending.
//
// Pattern:
//
//	input ─┬──► output1
//	       ├──► output2
//	       └──► output3
func FanOut[T any](ctx context.Context, input <-chan T, numOutputs int) []<-chan T {
	outputs := make([]chan T, numOutputs)
	for i := range numOutputs {
		outputs[i] = make(chan T)
	}

	go func() {
		defer func() {
			for _, out := range outputs {
				close(out)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-input:
				if !ok {
					return
				}
				// Distribute to all outputs
				for _, out := range outputs {
					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	// Convert to read-only channels
	result := make([]<-chan T, numOutputs)
	for i, out := range outputs {
		result[i] = out
	}
	return result
}

// FanIn merges multiple input channels into a single output channel.
// Messages from all inputs are combined in arrival order.
//
// Pattern:
//
//	input1 ───┐
//	input2 ───┼──► output
//	input3 ───┘
func FanIn[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	output := make(chan T)

	var wg sync.WaitGroup

	// Start a goroutine for each input
	multiplex := func(input <-chan T) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-input:
				if !ok {
					return
				}
				select {
				case output <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	wg.Add(len(inputs))
	for _, input := range inputs {
		go multiplex(input)
	}

	// Close output when all inputs are done
	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

// Pipeline chains multiple processing stages.
// Each stage transforms messages and passes them to the next.
//
// Pattern:
//
//	input ──► stage1 ──► stage2 ──► stage3 ──► output
func Pipeline[T any](ctx context.Context, input <-chan T, stages ...func(T) T) <-chan T {
	for _, stage := range stages {
		input = pipelineStage(ctx, input, stage)
	}
	return input
}

func pipelineStage[T any](ctx context.Context, input <-chan T, transform func(T) T) <-chan T {
	output := make(chan T)

	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-input:
				if !ok {
					return
				}
				result := transform(msg)
				select {
				case output <- result:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return output
}

// Broadcast sends each message to all subscribers (fan-out with buffering).
// Unlike basic FanOut, this uses buffered channels to prevent slow receivers
// from blocking the sender.
type Broadcast[T any] struct {
	input   chan T
	outputs []chan T
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewBroadcast creates a new broadcast with buffered outputs.
func NewBroadcast[T any](bufferSize int) *Broadcast[T] {
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broadcast[T]{
		input:   make(chan T),
		ctx:     ctx,
		cancel:  cancel,
		outputs: make([]chan T, 0), // Start empty, subscribers add themselves
	}

	b.wg.Add(1)
	go b.run()

	return b
}

func (b *Broadcast[T]) run() {
	defer b.wg.Done()

	for {
		select {
		case <-b.ctx.Done():
			return
		case msg, ok := <-b.input:
			if !ok {
				return
			}
			b.mu.RLock()
			for _, out := range b.outputs {
				// Non-blocking send
				select {
				case out <- msg:
				default:
					// Slow receiver, drop message
				}
			}
			b.mu.RUnlock()
		}
	}
}

// Send sends a message to all subscribers.
func (b *Broadcast[T]) Send(msg T) bool {
	select {
	case b.input <- msg:
		return true
	case <-b.ctx.Done():
		return false
	}
}

// Subscribe creates a new subscriber channel.
func (b *Broadcast[T]) Subscribe(bufferSize int) <-chan T {
	if bufferSize <= 0 {
		bufferSize = 10
	}
	ch := make(chan T, bufferSize)

	b.mu.Lock()
	b.outputs = append(b.outputs, ch)
	b.mu.Unlock()

	return ch
}

// Close shuts down the broadcast.
func (b *Broadcast[T]) Close() {
	b.cancel()
	close(b.input)
	b.wg.Wait()

	b.mu.Lock()
	for _, out := range b.outputs {
		close(out)
	}
	b.outputs = nil
	b.mu.Unlock()
}
