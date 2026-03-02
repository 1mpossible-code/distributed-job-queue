package redisbroker

import (
	"testing"
	"time"

	"distributed_job_queue/pkg/queue"
)

func TestDLQAfterMaxAttempts(t *testing.T) {
	b, ctx := newTestBroker(t)
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "job-1",
		Type:           "t",
		IdempotencyKey: "idem-1",
		Priority:       queue.PriorityMedium,
		MaxAttempts:    2,
	})
	job, lease, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	err = b.Nack(ctx, lease, queue.RetryDecision{
		Attempt:     job.Attempt + 2,
		MaxAttempts: 2,
		RetryAtUnix: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}
	n, err := b.DLQLen(ctx)
	if err != nil {
		t.Fatalf("dlq len failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected dlq size 1, got %d", n)
	}
}

func TestRequeueExpiredLease(t *testing.T) {
	b, ctx := newTestBroker(t)
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "job-1",
		Type:           "t",
		IdempotencyKey: "idem-2",
		Priority:       queue.PriorityHigh,
		MaxAttempts:    3,
	})
	_, _, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	moved, err := b.RequeueExpired(ctx)
	if err != nil {
		t.Fatalf("requeue expired failed: %v", err)
	}
	if moved != 1 {
		t.Fatalf("expected moved 1, got %d", moved)
	}
	if _, _, err := b.Reserve(ctx, "w2"); err != nil {
		t.Fatalf("expected requeued job, got error: %v", err)
	}
}
