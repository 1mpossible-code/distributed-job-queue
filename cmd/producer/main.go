package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"distributed_job_queue/pkg/producer"
	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"

	"github.com/redis/go-redis/v9"
)

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	id := flag.String("id", "", "job id")
	kind := flag.String("type", "default", "job type")
	idem := flag.String("idempotency-key", "", "idempotency key")
	priority := flag.String("priority", "medium", "priority: high|medium|low")
	flag.Parse()

	if *id == "" {
		*id = fmt.Sprintf("job-%d", time.Now().UnixNano())
	}
	if *idem == "" {
		*idem = *id
	}

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	pub := producer.New(redisbroker.New(rdb, redisbroker.Config{Prefix: "dq"}), nil)

	job := queue.Job{
		ID:             *id,
		Type:           *kind,
		IdempotencyKey: *idem,
		Priority:       queue.Priority(*priority),
		MaxAttempts:    3,
	}
	if err := pub.Enqueue(context.Background(), job); err != nil {
		log.Fatalf("enqueue failed: %v", err)
	}
	log.Printf("enqueued id=%s priority=%s", job.ID, job.Priority)
}
