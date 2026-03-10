package main

import (
	"context"
	"flag"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"distributed_job_queue/pkg/producer"
	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"
	"distributed_job_queue/pkg/worker"

	"github.com/redis/go-redis/v9"
)

type benchHandler struct {
	latencies chan time.Duration
	done      *int64
}

func (h *benchHandler) Handle(_ context.Context, job queue.Job) error {
	atomic.AddInt64(h.done, 1)
	if !job.EnqueuedAt.IsZero() {
		h.latencies <- time.Since(job.EnqueuedAt)
	}
	return nil
}

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	jobs := flag.Int("jobs", 2000, "job count")
	workers := flag.Int("workers", 8, "worker count")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()
	_ = rdb.FlushDB(ctx).Err()
	broker := redisbroker.New(rdb, redisbroker.Config{Prefix: "dq"})
	pub := producer.New(broker, nil)

	var done int64
	h := &benchHandler{latencies: make(chan time.Duration, *jobs), done: &done}
	for i := 0; i < *workers; i++ {
		rt := worker.New(broker, h, worker.Config{WorkerID: fmt.Sprintf("w-%d", i+1), Concurrency: 1})
		go func() { _ = rt.Run(context.Background()) }()
	}

	start := time.Now()
	for i := 0; i < *jobs; i++ {
		_ = pub.Enqueue(ctx, queue.Job{
			ID: fmt.Sprintf("job-%d", i), Type: "bench", IdempotencyKey: fmt.Sprintf("idem-%d", i), Priority: queue.PriorityMedium, MaxAttempts: 3,
		})
	}
	for atomic.LoadInt64(&done) < int64(*jobs) {
		_, _ = broker.RequeueExpired(ctx)
		time.Sleep(25 * time.Millisecond)
	}
	close(h.latencies)

	latencies := make([]time.Duration, 0, *jobs)
	for l := range h.latencies {
		latencies = append(latencies, l)
	}
	total := time.Since(start)
	if len(latencies) == 0 {
		fmt.Printf("throughput=0.00 jobs/sec p50=0s p95=0s p99=0s total=%s\n", total)
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p95 := latencies[len(latencies)*95/100]
	p99 := latencies[len(latencies)*99/100]
	fmt.Printf("throughput=%.2f jobs/sec p50=%s p95=%s p99=%s total=%s\n",
		float64(*jobs)/total.Seconds(), p50, p95, p99, total)
}
