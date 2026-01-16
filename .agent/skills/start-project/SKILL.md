---
description: Start a new Go concurrency project following the skills standards
---

# Start a New Go Concurrency Project

When the user wants to start working on a project from the learning roadmap:

## Step 1: Read the Standards
// turbo
Read the .claudes/skills/SKILLS.md file to understand the skills to master, acceptance criteria, and self-assessment questions for the requested project.

## Step 2: Read the Roadmap
// turbo
Read the README.md file to see the current progress and specific tasks for the project.

## Step 3: Create Project Structure
Create the project folder with the standard structure:
```
{project-name}/
├── README.md           # Project overview (use template from SKILLS.md)
├── {package}/          # Core implementation
│   ├── {file}.go
│   └── {file}_test.go
└── examples/           # Usage examples
```

## Step 4: Guide Implementation
Walk the user through each sub-task:
1. Explain the concept being practiced
2. Provide hints but let the user write the code
3. Review their code for correctness and style
4. Suggest improvements

## Step 5: Verify Completion
Before marking a task complete, ensure:
- [ ] Code passes `go test -race`
- [ ] Unit tests exist with good coverage
- [ ] The user can answer self-assessment questions
- [ ] Learning is documented in project README

## English Practice Reminder
The user is practicing English. Correct any writing errors politely with a table format showing original vs corrected text at the end of each response.
