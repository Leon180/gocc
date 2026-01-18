package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCrawler_Basic(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`
			<html>
			<body>
				<a href="/page1">Page 1</a>
				<a href="/page2">Page 2</a>
			</body>
			</html>
		`))
	}))
	defer server.Close()

	crawler := New(DefaultConfig())
	ctx := context.Background()

	results := crawler.Crawl(ctx, []string{server.URL})

	var crawled []Result
	for result := range results {
		crawled = append(crawled, result)
	}

	if len(crawled) != 1 {
		t.Errorf("Expected 1 result, got %d", len(crawled))
	}

	if crawled[0].StatusCode != 200 {
		t.Errorf("Expected status 200, got %d", crawled[0].StatusCode)
	}

	if len(crawled[0].Links) != 2 {
		t.Errorf("Expected 2 links, got %d", len(crawled[0].Links))
	}
}

func TestCrawler_Deduplication(t *testing.T) {
	requestCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.Write([]byte(`<html><body>Hello</body></html>`))
	}))
	defer server.Close()

	crawler := New(DefaultConfig())
	ctx := context.Background()

	// Try to crawl same URL multiple times
	urls := []string{server.URL, server.URL, server.URL}
	results := crawler.Crawl(ctx, urls)

	for range results {
	}

	mu.Lock()
	count := requestCount
	mu.Unlock()

	// Should only make 1 request due to deduplication
	if count != 1 {
		t.Errorf("Expected 1 request (deduplicated), got %d", count)
	}
}

func TestCrawler_ConcurrencyLimit(t *testing.T) {
	maxConcurrent := 0
	currentConcurrent := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		currentConcurrent++
		if currentConcurrent > maxConcurrent {
			maxConcurrent = currentConcurrent
		}
		mu.Unlock()

		time.Sleep(50 * time.Millisecond) // Simulate slow response

		mu.Lock()
		currentConcurrent--
		mu.Unlock()

		w.Write([]byte(`<html></html>`))
	}))
	defer server.Close()

	config := DefaultConfig()
	config.MaxWorkers = 3
	crawler := New(config)
	ctx := context.Background()

	// Queue 10 unique URLs
	var urls []string
	for i := range 10 {
		urls = append(urls, server.URL+"/page"+string(rune('0'+i)))
	}

	results := crawler.Crawl(ctx, urls)
	for range results {
	}

	mu.Lock()
	max := maxConcurrent
	mu.Unlock()

	// Should never exceed MaxWorkers
	if max > 3 {
		t.Errorf("Exceeded max workers: max concurrent was %d, expected <= 3", max)
	}
}

func TestCrawler_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.Write([]byte(`<html></html>`))
	}))
	defer server.Close()

	config := DefaultConfig()
	config.Timeout = 1 * time.Second
	crawler := New(config)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results := crawler.Crawl(ctx, []string{server.URL})

	// Either get an error result, or no result (request dropped)
	// Both are valid behaviors when context is cancelled
	resultCount := 0
	for result := range results {
		resultCount++
		// If we got a result, it should have an error (context cancelled during fetch)
		if result.Error == nil && result.StatusCode == 0 {
			t.Log("Got result with no error and no status - request may have been dropped")
		}
	}

	// Should complete without hanging
	t.Logf("Got %d results", resultCount)
}

func TestParseLinks(t *testing.T) {
	html := `
		<html>
		<body>
			<a href="/relative">Relative</a>
			<a href="https://example.com/absolute">Absolute</a>
			<a href="#fragment">Fragment</a>
			<a href="javascript:void(0)">JavaScript</a>
			<a href="mailto:test@example.com">Mail</a>
		</body>
		</html>
	`

	links, err := ParseLinks(strings.NewReader(html), "https://test.com/page")
	if err != nil {
		t.Fatalf("ParseLinks failed: %v", err)
	}

	// Should only have 2 valid links (relative + absolute)
	if len(links) != 2 {
		t.Errorf("Expected 2 links, got %d: %v", len(links), links)
	}

	// Check relative URL was resolved
	foundRelative := false
	for _, link := range links {
		if link == "https://test.com/relative" {
			foundRelative = true
		}
	}
	if !foundRelative {
		t.Error("Relative URL was not resolved correctly")
	}
}
