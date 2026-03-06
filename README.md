# distributed job queue

production-style queue in go using redis with:
- producer -> broker -> worker flow
- retries with exponential backoff
- dead-letter queue
- priority queues
- idempotency key handling
- horizontal worker scaling

## run
start redis on `127.0.0.1:6379`, then:

`go run ./cmd/worker -concurrency 8`

`go run ./cmd/producer -id job-1 -idempotency-key order-1 -priority high`

`go run ./cmd/bench -jobs 5000 -workers 16`

## tests
unit + integration tests live under `pkg/*`.
