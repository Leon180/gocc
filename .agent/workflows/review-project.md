---
description: Review a completed Go concurrency project against the skills standards
---

# Review a Completed Project

When the user thinks they've completed a project:

## Step 1: Read the Standards
// turbo
Read the .agent/skills/standard/SKILL.md file for the project's acceptance criteria and self-assessment questions.

## Step 2: Run Quality Checks
// turbo
Run the following commands in the project directory:
```bash
gofmt -l .
golangci-lint run
go test -race -v ./...
go test -cover ./...
```

## Step 3: Code Review
Review the implementation for:
- Correct use of concurrency primitives
- Proper error handling
- No goroutine leaks
- Clean code structure

## Step 4: Self-Assessment
Ask the user the self-assessment questions from .agent/skills/standard/SKILL.md. Verify they can explain:
- Why they made certain design choices
- Trade-offs between different approaches
- How the concurrency concepts work

## Step 5: Update Progress
If all criteria are met:
1. Mark the tasks as complete [x] in README.md
2. Suggest documenting learnings in the project's README
3. Recommend the next project to tackle

## English Practice Reminder
Correct any writing errors the user makes during the review discussion.