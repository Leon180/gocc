package crawler

import (
	"io"
	"net/url"
	"strings"

	"golang.org/x/net/html"
)

// ParseLinks extracts all links from an HTML document.
// It resolves relative URLs based on the base URL.
func ParseLinks(body io.Reader, baseURL string) ([]string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	doc, err := html.Parse(body)
	if err != nil {
		return nil, err
	}

	var links []string
	seen := make(map[string]bool)

	var extract func(*html.Node)
	extract = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					link := resolveURL(base, attr.Val)
					if link != "" && !seen[link] {
						seen[link] = true
						links = append(links, link)
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			extract(c)
		}
	}

	extract(doc)
	return links, nil
}

// resolveURL resolves a potentially relative URL against a base URL.
// Returns empty string for invalid or non-HTTP URLs.
func resolveURL(base *url.URL, href string) string {
	// Skip empty, javascript:, mailto:, etc.
	href = strings.TrimSpace(href)
	if href == "" || strings.HasPrefix(href, "#") ||
		strings.HasPrefix(href, "javascript:") ||
		strings.HasPrefix(href, "mailto:") {
		return ""
	}

	parsed, err := url.Parse(href)
	if err != nil {
		return ""
	}

	resolved := base.ResolveReference(parsed)

	// Only return HTTP/HTTPS URLs
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}

	// Normalize: remove fragment, ensure trailing slash consistency
	resolved.Fragment = ""

	return resolved.String()
}
