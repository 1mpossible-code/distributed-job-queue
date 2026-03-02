package redisbroker

import (
	"testing"
	"time"

	"distributed_job_queue/pkg/queue"
)

func TestNackSchedulesAndRequeues(t *testing.T) {
	b, ctx := newTestBroker(t)
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "job-1",
		Type:           "t",
		IdempotencyKey: "idem-1",
		Priority:       queue.PriorityMedium,
		MaxAttempts:    3,
	})
	job, lease, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	err = b.Nack(ctx, lease, queue.RetryDecision{
		Attempt:     job.Attempt + 1,
		MaxAttempts: 3,
		RetryAtUnix: time.Now().Add(5 * time.Millisecond).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	next, _, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve retried failed: %v", err)
	}
	if next.Attempt != 1 {
		t.Fatalf("expected attempt=1 after retry, got %d", next.Attempt)
	}
}
