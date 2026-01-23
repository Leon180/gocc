# Go Concurrency Learning Project Makefile

.PHONY: all test race cover lint fmt modernize clean help

# Default target
all: fmt lint test

# Run all tests
test:
	@echo "Running tests..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Testing $$dir"; \
			(cd $$dir && go test ./...); \
		fi \
	done

# Run tests with race detector
race:
	@echo "Running tests with race detector..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Testing $$dir"; \
			(cd $$dir && go test -race ./...); \
		fi \
	done

# Run tests with coverage
cover:
	@echo "Running tests with coverage..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Coverage $$dir"; \
			(cd $$dir && go test -cover ./...); \
		fi \
	done

# Run linter
lint:
	@echo "Running golangci-lint..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Linting $$dir"; \
			(cd $$dir && golangci-lint run 2>/dev/null || true); \
		fi \
	done

# Format code
fmt:
	@echo "Formatting code..."
	@gofmt -w .

# Run modernize analyzer
modernize:
	@echo "Running modernize analyzer..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Modernizing $$dir"; \
			(cd $$dir && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest ./... 2>/dev/null || \
			 go run golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize@latest ./... 2>/dev/null || \
			 echo "modernize not available for $$dir"); \
		fi \
	done

# Run modernize with fix
modernize-fix:
	@echo "Running modernize with auto-fix..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Modernizing $$dir"; \
			(cd $$dir && go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest -fix ./... 2>/dev/null || \
			 go run golang.org/x/tools/go/analysis/passes/modernize/cmd/modernize@latest -fix ./... 2>/dev/null || \
			 echo "modernize not available for $$dir"); \
		fi \
	done

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@for dir in stage1/*/; do \
		if [ -f "$$dir/go.mod" ]; then \
			echo "Benchmarking $$dir"; \
			(cd $$dir && go test -bench=. -benchmem ./...); \
		fi \
	done

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@find . -name "*.test" -delete
	@find . -name "*.out" -delete
	@find . -name "*.prof" -delete

# Run pub/sub interactive demo
pubsub-demo:
	@echo "Starting Pub/Sub Demo..."
	@cd stage1/04-pubsub && go run main.go

# Run task scheduler interactive demo
scheduler-demo:
	@echo "Starting Task Scheduler Demo..."
	@cd stage2/07-task-scheduler && go run main.go

# Show help
help:
	@echo "Available targets:"
	@echo "  all          - Format, lint, and test (default)"
	@echo "  test         - Run all tests"
	@echo "  race         - Run tests with race detector"
	@echo "  cover        - Run tests with coverage"
	@echo "  lint         - Run golangci-lint"
	@echo "  fmt          - Format code with gofmt"
	@echo "  modernize    - Run go modernize analyzer"
	@echo "  modernize-fix- Run modernize with auto-fix"
	@echo "  bench        - Run benchmarks"
	@echo "  pubsub-demo  - Run pub/sub interactive demo"
	@echo "  scheduler-demo - Run task scheduler demo"
	@echo "  clean        - Clean build artifacts"
	@echo "  help         - Show this help"

