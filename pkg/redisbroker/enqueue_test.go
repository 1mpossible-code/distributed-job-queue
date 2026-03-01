package redisbroker

import (
	"context"
	"testing"
	"time"

	"distributed_job_queue/pkg/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestBroker(t *testing.T) (*Broker, context.Context) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start failed: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return New(rdb, Config{
		Prefix:         "test",
		LeaseDuration:  50 * time.Millisecond,
		IdempotencyTTL: time.Minute,
	}), context.Background()
}

func TestEnqueueIdempotent(t *testing.T) {
	b, ctx := newTestBroker(t)
	job := queue.Job{
		ID:             "job-1",
		Type:           "email",
		IdempotencyKey: "order-1",
		Priority:       queue.PriorityHigh,
		MaxAttempts:    3,
	}
	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}
	if err := b.Enqueue(ctx, job); err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}
	n, err := b.rdb.LLen(ctx, b.readyKey(queue.PriorityHigh)).Result()
	if err != nil {
		t.Fatalf("llen failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 queued job, got %d", n)
	}
}
