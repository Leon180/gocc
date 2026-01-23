/*
Task Scheduler Demo - Interactive CLI

This demo showcases the Task Scheduler features:
- Delayed execution (time.Timer)
- Interval execution (time.Ticker)
- Cron scheduling
- Task priority queue
- Task dependencies
- Retry with exponential backoff
- Task waiting (sync.Cond)

Run with: go run main.go
*/
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"taskscheduler/scheduler"
)

func main() {
	fmt.Println("=================================")
	fmt.Println("   Task Scheduler - Demo CLI")
	fmt.Println("=================================")
	printHelp()

	s := scheduler.New()
	tw := scheduler.NewTaskWaiter()
	defer func() { _ = s.Close() }()

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("\n> ")

	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		input = strings.TrimSpace(input)
		if input == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(input)
		cmd := parts[0]

		switch cmd {
		case "help", "h":
			printHelp()

		case "once", "o":
			runOnceDemo(s, tw)

		case "interval", "i":
			runIntervalDemo(s)

		case "cron", "c":
			runCronDemo(s)

		case "priority", "p":
			runPriorityDemo()

		case "retry", "r":
			runRetryDemo(s)

		case "deps", "d":
			runDependencyDemo(s)

		case "wait", "w":
			runWaitDemo(tw)

		case "all", "a":
			runAllDemo(s, tw)

		case "quit", "q":
			fmt.Println("\n👋 Goodbye!")
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help')\n", cmd)
		}

		fmt.Print("\n> ")
	}
}

func printHelp() {
	fmt.Println(`
Commands:
  once      (o)  - Demo: delayed execution
  interval  (i)  - Demo: interval execution
  cron      (c)  - Demo: cron scheduling
  priority  (p)  - Demo: priority queue
  retry     (r)  - Demo: retry with backoff
  deps      (d)  - Demo: task dependencies
  wait      (w)  - Demo: sync.Cond waiting
  all       (a)  - Run all demos
  quit      (q)  - Exit`)
}

func runOnceDemo(s *scheduler.Scheduler, tw *scheduler.TaskWaiter) {
	fmt.Println("\n⏱️  ONCE DEMO: Delayed Execution")
	fmt.Println("================================")

	_, _ = s.ScheduleOnce("once-demo", "Delayed Task", 2*time.Second, func(ctx context.Context) error {
		fmt.Println("   ✅ Task executed after 2 second delay!")
		tw.MarkComplete("once-demo", nil)
		return nil
	})

	fmt.Println("   Task scheduled, waiting 2 seconds...")
	time.Sleep(2500 * time.Millisecond)
}

func runIntervalDemo(s *scheduler.Scheduler) {
	fmt.Println("\n🔄 INTERVAL DEMO: Repeated Execution")
	fmt.Println("=====================================")

	var count int32
	task, _ := s.ScheduleInterval("interval-demo", "Heartbeat", 500*time.Millisecond, func(ctx context.Context) error {
		n := atomic.AddInt32(&count, 1)
		fmt.Printf("   💓 Heartbeat #%d\n", n)
		return nil
	})

	fmt.Println("   Running for 2.5 seconds...")
	time.Sleep(2500 * time.Millisecond)
	task.Cancel()
	fmt.Printf("   ⏹️  Stopped after %d heartbeats\n", atomic.LoadInt32(&count))
}

func runCronDemo(s *scheduler.Scheduler) {
	fmt.Println("\n📅 CRON DEMO: Cron Scheduling")
	fmt.Println("=============================")

	var count int32
	task, err := s.ScheduleCron("cron-demo", "Every Second", "* * * * * *", func(ctx context.Context) error {
		n := atomic.AddInt32(&count, 1)
		fmt.Printf("   ⏰ Cron tick #%d at %s\n", n, time.Now().Format("15:04:05"))
		return nil
	})

	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
		return
	}

	fmt.Println("   Cron '* * * * * *' (every second) for 3 seconds...")
	time.Sleep(3500 * time.Millisecond)
	task.Cancel()
	fmt.Printf("   ⏹️  Stopped after %d ticks\n", atomic.LoadInt32(&count))
}

func runPriorityDemo() {
	fmt.Println("\n🎯 PRIORITY DEMO: Priority Queue")
	fmt.Println("=================================")

	pq := scheduler.NewPriorityQueue()

	// Add tasks in random order
	tasks := []*scheduler.Task{
		{ID: "low-1", Name: "Low Priority 1", Priority: scheduler.PriorityLow},
		{ID: "high-1", Name: "High Priority 1", Priority: scheduler.PriorityHigh},
		{ID: "normal-1", Name: "Normal Priority 1", Priority: scheduler.PriorityNormal},
		{ID: "high-2", Name: "High Priority 2", Priority: scheduler.PriorityHigh},
		{ID: "low-2", Name: "Low Priority 2", Priority: scheduler.PriorityLow},
	}

	fmt.Println("   Adding tasks in random order...")
	for _, t := range tasks {
		pq.PushTask(t)
		fmt.Printf("   + %s (Priority: %d)\n", t.Name, t.Priority)
	}

	fmt.Println("\n   Popping by priority:")
	for pq.Size() > 0 {
		t := pq.PopTask()
		fmt.Printf("   → %s (Priority: %d)\n", t.Name, t.Priority)
	}
}

func runRetryDemo(s *scheduler.Scheduler) {
	fmt.Println("\n🔁 RETRY DEMO: Exponential Backoff")
	fmt.Println("===================================")

	var attempts int32
	failUntil := int32(2)

	task, _ := s.ScheduleOnce("retry-demo", "Flaky Task", 100*time.Millisecond, func(ctx context.Context) error {
		n := atomic.AddInt32(&attempts, 1)
		if n <= failUntil {
			fmt.Printf("   ❌ Attempt %d: FAILED\n", n)
			return errors.New("simulated failure")
		}
		fmt.Printf("   ✅ Attempt %d: SUCCESS\n", n)
		return nil
	})

	task.Retry = &scheduler.RetryConfig{
		MaxRetries: 3,
		Delay:      200 * time.Millisecond,
		Multiplier: 2.0,
		MaxDelay:   1 * time.Second,
	}

	fmt.Println("   Task will fail 2 times, then succeed...")
	fmt.Println("   Retry delays: 200ms, 400ms (exponential backoff)")
	time.Sleep(1500 * time.Millisecond)
}

func runDependencyDemo(s *scheduler.Scheduler) {
	fmt.Println("\n🔗 DEPENDENCY DEMO: Task Dependencies")
	fmt.Println("======================================")

	completed := make(map[string]bool)

	taskA, _ := s.ScheduleOnce("dep-A", "Task A", 100*time.Millisecond, func(ctx context.Context) error {
		fmt.Println("   ✅ Task A completed")
		completed["A"] = true
		return nil
	})

	taskB, _ := s.ScheduleOnce("dep-B", "Task B (depends on A)", 200*time.Millisecond, func(ctx context.Context) error {
		fmt.Println("   ✅ Task B completed (after A)")
		return nil
	})
	taskB.DependsOn = []scheduler.TaskID{"dep-A"}

	fmt.Println("   Task B depends on Task A...")
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("   Task A state: %s\n", taskA.State())
}

func runWaitDemo(tw *scheduler.TaskWaiter) {
	fmt.Println("\n⏳ WAIT DEMO: sync.Cond Waiting")
	fmt.Println("================================")

	// Reset waiter
	tw.Reset("wait-task")

	go func() {
		time.Sleep(1 * time.Second)
		fmt.Println("   🔔 Task completed, waking waiters...")
		tw.MarkComplete("wait-task", nil)
	}()

	fmt.Println("   Waiting for task completion...")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := tw.Wait(ctx, "wait-task")
	if err != nil {
		fmt.Printf("   ❌ Wait error: %v\n", err)
	} else {
		fmt.Println("   ✅ Wait completed!")
	}
}

func runAllDemo(s *scheduler.Scheduler, tw *scheduler.TaskWaiter) {
	fmt.Println("\n🚀 RUNNING ALL DEMOS")
	fmt.Println("====================")

	runPriorityDemo()
	time.Sleep(500 * time.Millisecond)

	runOnceDemo(s, tw)
	time.Sleep(500 * time.Millisecond)

	runRetryDemo(s)
	time.Sleep(500 * time.Millisecond)

	runWaitDemo(tw)

	fmt.Println("\n🎉 All demos complete!")
}
