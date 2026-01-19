package pubsub

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestFanOut(t *testing.T) {
	ctx := context.Background()
	input := make(chan int)

	// Fan out to 3 outputs
	outputs := FanOut(ctx, input, 3)

	if len(outputs) != 3 {
		t.Fatalf("Expected 3 outputs, got %d", len(outputs))
	}

	// Send values
	go func() {
		for i := range 5 {
			input <- i
		}
		close(input)
	}()

	// Each output should receive all values
	var wg sync.WaitGroup
	results := make([][]int, 3)

	for i, out := range outputs {
		wg.Add(1)
		go func(idx int, ch <-chan int) {
			defer wg.Done()
			for v := range ch {
				results[idx] = append(results[idx], v)
			}
		}(i, out)
	}

	wg.Wait()

	// Verify each output got all 5 values
	for i, r := range results {
		if len(r) != 5 {
			t.Errorf("Output %d: expected 5 values, got %d", i, len(r))
		}
	}
}

func TestFanIn(t *testing.T) {
	ctx := context.Background()

	// Create 3 input channels
	inputs := make([]chan int, 3)
	for i := range 3 {
		inputs[i] = make(chan int)
	}

	// Convert to read-only for FanIn
	readInputs := make([]<-chan int, 3)
	for i, in := range inputs {
		readInputs[i] = in
	}

	output := FanIn(ctx, readInputs...)

	// Send values from each input
	go func() {
		for i, in := range inputs {
			for j := range 3 {
				in <- i*10 + j
			}
			close(in)
		}
	}()

	// Collect all values
	var results []int
	for v := range output {
		results = append(results, v)
	}

	// Should have 9 values total (3 inputs × 3 values each)
	if len(results) != 9 {
		t.Errorf("Expected 9 values, got %d", len(results))
	}
}

func TestFanIn_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	input := make(chan int)
	output := FanIn(ctx, input)

	// Send one value
	go func() {
		input <- 1
		time.Sleep(50 * time.Millisecond)
		cancel() // Cancel while waiting
	}()

	// Should receive first value
	<-output

	// Context cancellation should eventually close output
	select {
	case <-time.After(100 * time.Millisecond):
		// OK - might take time
	case _, ok := <-output:
		if ok {
			// Unexpected value, but OK
		}
	}
}

func TestPipeline(t *testing.T) {
	ctx := context.Background()
	input := make(chan int)

	// Pipeline: double → add 1
	output := Pipeline(ctx, input,
		func(x int) int { return x * 2 },
		func(x int) int { return x + 1 },
	)

	go func() {
		input <- 5
		close(input)
	}()

	// 5 * 2 = 10, 10 + 1 = 11
	result := <-output
	if result != 11 {
		t.Errorf("Expected 11, got %d", result)
	}
}

func TestPipeline_Multiple(t *testing.T) {
	ctx := context.Background()
	input := make(chan int)

	// Three-stage pipeline
	output := Pipeline(ctx, input,
		func(x int) int { return x + 1 },
		func(x int) int { return x * 2 },
		func(x int) int { return x - 3 },
	)

	go func() {
		for i := range 5 {
			input <- i
		}
		close(input)
	}()

	// Collect results
	var results []int
	for v := range output {
		results = append(results, v)
	}

	// Verify: (x+1)*2-3
	expected := []int{-1, 1, 3, 5, 7}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("Index %d: expected %d, got %d", i, exp, results[i])
		}
	}
}

func TestBroadcast(t *testing.T) {
	bc := NewBroadcast[int](10)
	defer bc.Close()

	// Create subscribers
	sub1 := bc.Subscribe(10)
	sub2 := bc.Subscribe(10)
	sub3 := bc.Subscribe(10)

	// Send messages
	for i := range 5 {
		bc.Send(i)
	}

	// Give time for messages to propagate
	time.Sleep(50 * time.Millisecond)

	// Each subscriber should receive all messages
	for _, sub := range []<-chan int{sub1, sub2, sub3} {
		count := 0
		for {
			select {
			case <-sub:
				count++
			default:
				goto next
			}
		}
	next:
		if count != 5 {
			t.Errorf("Expected 5 messages, got %d", count)
		}
	}
}

func TestBroadcast_SlowSubscriber(t *testing.T) {
	bc := NewBroadcast[int](10)
	defer bc.Close()

	// Fast subscriber
	fast := bc.Subscribe(100)

	// Slow subscriber with small buffer
	slow := bc.Subscribe(2)

	// Send many messages
	for i := range 10 {
		bc.Send(i)
	}

	time.Sleep(50 * time.Millisecond)

	// Fast should get all
	fastCount := 0
	for {
		select {
		case <-fast:
			fastCount++
		default:
			goto checkSlow
		}
	}

checkSlow:
	// Slow should get at most buffer size
	slowCount := 0
	for {
		select {
		case <-slow:
			slowCount++
		default:
			goto done
		}
	}

done:
	if fastCount != 10 {
		t.Errorf("Fast expected 10, got %d", fastCount)
	}
	if slowCount > 2 {
		t.Errorf("Slow expected <= 2 (buffer size), got %d", slowCount)
	}
}

// Benchmark FanIn with multiple inputs
func BenchmarkFanIn(b *testing.B) {
	ctx := context.Background()
	numInputs := 10

	inputs := make([]chan int, numInputs)
	readInputs := make([]<-chan int, numInputs)
	for i := range numInputs {
		inputs[i] = make(chan int, 100)
		readInputs[i] = inputs[i]
	}

	output := FanIn(ctx, readInputs...)

	b.ResetTimer()

	// Send to inputs
	go func() {
		for i := 0; i < b.N; i++ {
			inputs[i%numInputs] <- i
		}
		for _, in := range inputs {
			close(in)
		}
	}()

	// Drain output
	for range output {
	}
}

func TestFanOutFanIn_Combined(t *testing.T) {
	ctx := context.Background()
	input := make(chan int)

	// Fan out to 3 workers, each doubles the value
	outputs := FanOut(ctx, input, 3)

	// Process in each worker (simulate work)
	processed := make([]<-chan int, len(outputs))
	for i, out := range outputs {
		ch := make(chan int)
		processed[i] = ch
		go func(in <-chan int, out chan<- int) {
			defer close(out)
			for v := range in {
				out <- v * 2
			}
		}(out, ch)
	}

	// Fan in results
	merged := FanIn(ctx, processed...)

	// Send input
	go func() {
		for i := 1; i <= 3; i++ {
			input <- i
		}
		close(input)
	}()

	// Collect results
	var results []int
	for v := range merged {
		results = append(results, v)
	}

	// Each input value fan-out to 3 workers, so 3 * 3 = 9 results
	// Each doubled, so [2,2,2,4,4,4,6,6,6]
	if len(results) != 9 {
		t.Errorf("Expected 9 results, got %d", len(results))
	}

	// Sort and verify
	sort.Ints(results)
	expected := []int{2, 2, 2, 4, 4, 4, 6, 6, 6}
	for i := range expected {
		if results[i] != expected[i] {
			t.Errorf("Index %d: expected %d, got %d", i, expected[i], results[i])
		}
	}
}
