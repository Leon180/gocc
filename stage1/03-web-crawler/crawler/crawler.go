package crawler

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Result represents the outcome of crawling a single URL.
type Result struct {
	URL        string
	StatusCode int
	Links      []string // Extracted links from the page
	Error      error
	Duration   time.Duration
}

// Crawler is a concurrent web crawler with configurable limits.
type Crawler struct {
	client     *http.Client
	maxWorkers int
	visited    sync.Map // Thread-safe visited URL tracking
	userAgent  string
	timeout    time.Duration
}

// Config holds crawler configuration.
type Config struct {
	MaxWorkers int           // Maximum concurrent requests
	Timeout    time.Duration // HTTP request timeout
	UserAgent  string        // User-Agent header
}

// DefaultConfig returns sensible default configuration.
func DefaultConfig() Config {
	return Config{
		MaxWorkers: 5,
		Timeout:    10 * time.Second,
		UserAgent:  "GoCrawler/1.0 (Learning Project)",
	}
}

// New creates a new Crawler with the given configuration.
func New(config Config) *Crawler {
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 5
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Second
	}
	if config.UserAgent == "" {
		config.UserAgent = "GoCrawler/1.0"
	}

	return &Crawler{
		client: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		maxWorkers: config.MaxWorkers,
		userAgent:  config.UserAgent,
		timeout:    config.Timeout,
	}
}

// Crawl starts crawling from the given seed URLs.
// It returns a channel of results that will be closed when crawling is complete.
func (c *Crawler) Crawl(ctx context.Context, seedURLs []string) <-chan Result {
	results := make(chan Result, 100)
	semaphore := make(chan struct{}, c.maxWorkers)
	var wg sync.WaitGroup

	// Queue seed URLs
	// NOTE: crawlURL calls wg.Add(1) SYNCHRONOUSLY before starting its goroutine.
	// This ensures all seeds are counted before wg.Wait() is called below.
	for _, url := range seedURLs {
		c.crawlURL(ctx, url, results, semaphore, &wg)
	}

	// Close results when all done
	go func() {
		wg.Wait()
		close(results)
	}()

	return results
}

// crawlURL crawls a single URL if not already visited.
func (c *Crawler) crawlURL(ctx context.Context, url string, results chan<- Result, sem chan struct{}, wg *sync.WaitGroup) {
	// Check if already visited (atomic check-and-set)
	if _, loaded := c.visited.LoadOrStore(url, true); loaded {
		return // Already visited
	}

	// IMPORTANT: wg.Add(1) must be BEFORE the goroutine, not inside it.
	// This prevents a race where wg.Wait() returns before Add is called.
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Check context before proceeding
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Acquire semaphore (blocks if at max concurrency)
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }() // Release
		case <-ctx.Done():
			return
		}

		// Fetch and parse
		result := c.fetch(ctx, url)

		// Send result (non-blocking on cancel)
		select {
		case results <- result:
		case <-ctx.Done():
		}
	}()
}

// fetch performs the actual HTTP request and parses the response.
func (c *Crawler) fetch(ctx context.Context, url string) Result {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return Result{URL: url, Error: err, Duration: time.Since(start)}
	}

	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return Result{URL: url, Error: err, Duration: time.Since(start)}
	}
	defer resp.Body.Close()

	// Parse links from HTML
	links, err := ParseLinks(resp.Body, url)
	if err != nil {
		return Result{
			URL:        url,
			StatusCode: resp.StatusCode,
			Error:      err,
			Duration:   time.Since(start),
		}
	}

	return Result{
		URL:        url,
		StatusCode: resp.StatusCode,
		Links:      links,
		Duration:   time.Since(start),
	}
}

// VisitedCount returns the number of visited URLs.
func (c *Crawler) VisitedCount() int {
	count := 0
	c.visited.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
