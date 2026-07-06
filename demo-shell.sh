#!/usr/bin/env bash

set -u

REPO_URL="https://github.com/1mpossible-code/distributed-job-queue"
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"

print_banner() {
  clear
  cat <<EOF
============================================================
 Distributed Job Queue — Live Demo
============================================================

A small Go-based distributed job queue with Redis-backed workers,
priorities, idempotency keys, retries, and benchmarking.

GitHub:
  $REPO_URL

Type 'help' to see available commands.

EOF
}

print_help() {
  cat <<EOF
Commands:
  help            show this menu
  about           explain what this project demonstrates
  github          print the GitHub repo link
  status          show Redis connection + basic queue state
  keys            list demo Redis keys
  produce-high    enqueue a high-priority demo job
  produce-low     enqueue a low-priority demo job
  produce-batch   enqueue 5 demo jobs
  bench           run a small benchmark
  reset           clear demo Redis state
  clear           clear the screen
  exit            close the demo session

Notes:
  - This is a sandboxed browser terminal.
  - Only the commands above are allowed.
  - Source code is available here:
    $REPO_URL
EOF
}

about() {
  cat <<EOF
This project demonstrates a distributed job queue written in Go.

Core ideas shown here:
  - Redis-backed queue storage
  - Worker process consuming jobs
  - Job priority support
  - Idempotency keys to avoid duplicate enqueue behavior
  - Simple benchmark command for throughput testing
  - Dockerized deployment for reproducible demos

Try:
  produce-high
  produce-low
  status
  bench

GitHub:
  $REPO_URL
EOF
}

redis_check() {
  if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping >/dev/null 2>&1; then
    echo "Redis: connected at $REDIS_HOST:$REDIS_PORT"
  else
    echo "Redis: not reachable at $REDIS_HOST:$REDIS_PORT"
    return 1
  fi
}

status() {
  redis_check || return 1

  echo
  echo "Redis database size:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" dbsize

  echo
  echo "Demo keys:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan | head -20

  echo
  echo
  echo "Worker should be running in a separate Docker service."
  echo "Try 'produce-high', then run 'status' again."
}

list_keys() {
  redis_check || return 1

  echo "Redis keys:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan | head -50
}

produce_job() {
  local priority="$1"
  local id="demo-${priority}-$(date +%s%N)"

  echo "Enqueuing ${priority}-priority job..."
  producer -id "$id" -idempotency-key "$id" -priority "$priority"
  echo
  echo "Job id: $id"
}

produce_batch() {
  echo "Enqueuing 5 demo jobs..."

  for i in 1 2 3 4 5; do
    if (( i % 2 == 0 )); then
      produce_job "high"
    else
      produce_job "low"
    fi
  done
}

run_bench() {
  echo "Running small benchmark..."
  echo "This is intentionally small so the public demo stays lightweight."
  echo

  bench -jobs 25 -workers 2
}

reset_demo() {
  echo "This will clear demo Redis state."
  read -rp "Continue? [y/N] " answer

  case "$answer" in
    y|Y|yes|YES)
      redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" flushdb >/dev/null
      echo "Demo Redis state cleared."
      ;;
    *)
      echo "Reset cancelled."
      ;;
  esac
}

print_banner

while true; do
  read -rp "djq> " cmd

  case "$cmd" in
    help)
      print_help
      ;;
    about)
      about
      ;;
    github)
      echo "$REPO_URL"
      ;;
    status)
      status
      ;;
    keys)
      list_keys
      ;;
    produce-high)
      produce_job "high"
      ;;
    produce-low)
      produce_job "low"
      ;;
    produce-batch)
      produce_batch
      ;;
    bench)
      run_bench
      ;;
    reset)
      reset_demo
      ;;
    clear)
      print_banner
      ;;
    exit|quit)
      echo "Thanks for trying the demo."
      echo "GitHub: $REPO_URL"
      exit 0
      ;;
    "")
      ;;
    *)
      echo "Unknown command: $cmd"
      echo "Type 'help' to see available commands."
      ;;
  esac

  echo
done
