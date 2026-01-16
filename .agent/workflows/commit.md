---
description: Generate a commit message by analyzing staged changes using conventional commits format
---

# Generate Commit Message

When the user wants to create a commit message for staged changes:

## Step 1: Check Staged Changes
// turbo
Run the following commands to see what's being committed:
```bash
git diff --staged --stat
git diff --staged
```

## Step 2: Analyze Changes
Based on the diff output, determine:
- **Type**: feat, fix, docs, refactor, test, chore, etc.
- **Scope**: Which part of the codebase (e.g., counter, ratelimiter, worker-pool)
- **Summary**: Brief description in imperative mood

## Step 3: Generate Commit Message
Create a commit message following conventional commits format:
```
<type>(<scope>): <summary>

[optional body explaining what and why]
```

## Step 4: Present Options
Provide 2-3 commit message options for the user to choose from, ranging from:
- Concise (one-liner)
- Detailed (with body)

## Step 5: Execute Commit
After user selects a message, run:
```bash
git commit -m "<selected message>"
```

## Commit Message Rules
- Use imperative mood: "Add" not "Added"
- Capitalize first letter of summary
- No period at end of summary
- Keep summary under 50 characters
- Body explains WHY not just WHAT

## English Practice Reminder
Correct any writing errors the user makes during the discussion.
