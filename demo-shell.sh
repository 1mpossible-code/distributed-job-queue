#!/usr/bin/env bash

set -u

REPO_URL="https://github.com/1mpossible-code/distributed-job-queue"
REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_ADDR="$REDIS_HOST:$REDIS_PORT"
SESSION_ID="demo-$(head -c 4 /dev/urandom | od -An -tx1 | tr -d ' \n')"

print_banner() {
  clear
  cat <<EOF
============================================================
 Distributed Job Queue — Live Demo
============================================================

A small Go-based distributed job queue with Redis-backed workers,
priorities, idempotency keys, retries, and benchmarking.

Session:
  $SESSION_ID

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
  status          show Redis connection + this session's demo state
  keys            list this session's Redis keys
  produce-high    enqueue a high-priority demo job
  produce-low     enqueue a low-priority demo job
  produce-batch   enqueue 5 demo jobs
  bench           run a small benchmark
  clear           clear the screen
  exit            close the demo session

Notes:
  - This is a sandboxed browser terminal.
  - Each visitor gets a separate session id.
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

This terminal session is namespaced as:
  $SESSION_ID

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
    echo "Redis: connected at $REDIS_ADDR"
  else
    echo "Redis: not reachable at $REDIS_ADDR"
    return 1
  fi
}

session_key_count() {
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan --pattern "*$SESSION_ID*" | wc -l
}

status() {
  redis_check || return 1

  echo
  echo "Session:"
  echo "  $SESSION_ID"

  echo
  echo "Total Redis database size:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" dbsize

  echo
  echo "Keys associated with this session:"
  session_key_count

  echo
  echo "Sample session keys:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan --pattern "*$SESSION_ID*" | head -20

  echo
  echo "Worker is running in a separate Docker service."
  echo "Try 'produce-high' or 'produce-low', then run 'status' again."
}

list_keys() {
  redis_check || return 1

  echo "Session keys for $SESSION_ID:"
  redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" --scan --pattern "*$SESSION_ID*" | head -50
}

produce_job() {
  local priority="$1"
  local id="${SESSION_ID}-${priority}-$(date +%s%N)"

  echo "Enqueuing ${priority}-priority job..."
  producer -redis "$REDIS_ADDR" -id "$id" -idempotency-key "$id" -priority "$priority"
  echo
  echo "Job id: $id"
}

produce_batch() {
  echo "Enqueuing 5 demo jobs for session $SESSION_ID..."
  echo

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

  bench -redis "$REDIS_ADDR" -jobs 25 -workers 2
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
