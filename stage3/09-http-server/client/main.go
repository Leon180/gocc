package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

func main() {
	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	baseURL := "http://localhost:8080"
	client := &http.Client{Timeout: 5 * time.Second}

	fmt.Println("--- Testing Health Check ---")
	testRequest(client, "GET", baseURL+"/health", nil)

	fmt.Println("\n--- Testing Echo ---")
	testRequest(client, "POST", baseURL+"/echo", []byte("Hello, HTTP Server!"))

	fmt.Println("\n--- Testing Slow (Timeout) ---")
	// Should return 503 because handler takes 2s > 1s timeout
	testRequest(client, "GET", baseURL+"/slow", nil)
}

func testRequest(client *http.Client, method, url string, body []byte) {
	fmt.Printf("%s %s\n", method, url)

	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %v\n", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("Status: %s\n", resp.Status)
	fmt.Printf("Body: %s\n", string(respBody))
}
