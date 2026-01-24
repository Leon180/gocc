package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"httpserver/server"
)

func main() {
	srv := server.New(":8080")

	// Start server (non-blocking)
	errChan := srv.Start()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		fmt.Printf("Server error: %v\n", err)
		os.Exit(1)
	case <-quit:
		fmt.Println("\nReceived interrupt signal")
	}

	// Graceful shutdown with 5s timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Stop(ctx); err != nil {
		fmt.Printf("Server shutdown error: %v\n", err)
	} else {
		fmt.Println("Server stopped gracefully")
	}
}
