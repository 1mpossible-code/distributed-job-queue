# Distributed Job Queue

A production-style distributed job queue in Go, backed by Redis.

It demonstrates:

- producer → broker → worker flow
- Redis-backed queue storage
- priority queues
- idempotency keys
- retries with exponential backoff
- dead-letter queue behavior
- horizontal worker scaling
- Prometheus worker metrics
- benchmark and failure simulation tools
- Dockerized browser terminal demo

## Live Demo

Try the browser-based sandbox demo:

https://djq.yemel.me

Commands to try:

- `about`
- `produce-high`
- `produce-low`
- `produce-batch`
- `status`
- `bench`

The demo runs in a restricted terminal environment. Each visitor gets a separate session id, and the available commands are intentionally limited.

## Architecture

```text
producer
   |
   v
Redis-backed broker
   |
   v
workers
   |
   +-- ack successful jobs
   +-- retry failed jobs with exponential backoff
   +-- move exhausted jobs to the dead-letter queue
```

## Run locally

Start Redis on `127.0.0.1:6379`, then run a worker:

```bash
go run ./cmd/worker -concurrency 8
```

Enqueue a job:

```bash
go run ./cmd/producer -id job-1 -idempotency-key order-1 -priority high
```

Run a benchmark:

```bash
go run ./cmd/bench -jobs 5000 -workers 16
```

You can also set Redis through an environment variable:

```bash
REDIS_ADDR=127.0.0.1:6379 go run ./cmd/worker
```

The `-redis` flag still overrides the default/env value:

```bash
go run ./cmd/worker -redis 10.0.0.5:6379
```

## Docker demo

This repo includes a Docker Compose setup with:

- Redis
- a background worker
- a `ttyd` browser terminal running the sandbox shell

Start it with:

```bash
docker compose up --build
```

Then open:

```text
http://localhost:7681
```

## Failure simulation

Run a worker with simulated random failures:

```bash
go run ./cmd/worker -concurrency 8 -fail-rate 0.05
```

Run a benchmark that cancels one worker mid-run:

```bash
go run ./cmd/bench -jobs 5000 -workers 16 -simulate-failure=true -failure-delay-ms 250
```

## Metrics

Workers expose Prometheus metrics on `:2112` by default:

```text
http://localhost:2112/metrics
```

Override the metrics address:

```bash
go run ./cmd/worker -metrics-addr :9090
```

## Tests

Unit and integration tests live under `pkg/*`.

```bash
go test ./...
```

## Delivery semantics

See [`docs/delivery-semantics.md`](docs/delivery-semantics.md) for notes on retries, leases, recovery, and delivery guarantees.
