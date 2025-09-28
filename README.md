# Go Worker Pool

A concurrent job processing system in Go that supports retries, timeouts, and a dead-letter queue. Inspired by production job runners (e.g., Sidekiq, Celery) but implemented from scratch to learn concurrency, databases, and error handling.

## Features
- Worker pool with configurable concurrency
- Context-based timeouts and cancellation
- Automatic retry with limits
- Dead-letter queue for failed jobs
- PostgreSQL persistence (`Job`, `Job_Attempts`, `DLQ` tables)

## Architecture

## Tech Stack
- Go (goroutines, channels, contexts, sync)
- PostgreSQL

## Run Locally
```bash
go run main.go

Example Output
Enqueued job at 2025-09-27
Inserted job with ID 42
Worker 1 completed job 42
