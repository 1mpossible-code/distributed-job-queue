package redisbroker

import (
	"errors"
	"testing"

	"distributed_job_queue/pkg/queue"
)

func TestReservePriorityOrder(t *testing.T) {
	b, ctx := newTestBroker(t)
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "low",
		Type:           "t",
		IdempotencyKey: "idem-low",
		Priority:       queue.PriorityLow,
		MaxAttempts:    2,
	})
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "high",
		Type:           "t",
		IdempotencyKey: "idem-high",
		Priority:       queue.PriorityHigh,
		MaxAttempts:    2,
	})

	job, _, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if job.ID != "high" {
		t.Fatalf("expected high job first, got %s", job.ID)
	}
}

func TestAckRemovesInflightState(t *testing.T) {
	b, ctx := newTestBroker(t)
	_ = b.Enqueue(ctx, queue.Job{
		ID:             "job-1",
		Type:           "t",
		IdempotencyKey: "idem-1",
		Priority:       queue.PriorityMedium,
		MaxAttempts:    2,
	})
	_, lease, err := b.Reserve(ctx, "w1")
	if err != nil {
		t.Fatalf("reserve failed: %v", err)
	}
	if err := b.Ack(ctx, lease); err != nil {
		t.Fatalf("ack failed: %v", err)
	}
	inflight, _ := b.rdb.ZCard(ctx, b.inflightKey()).Result()
	if inflight != 0 {
		t.Fatalf("expected inflight=0, got %d", inflight)
	}
}

func TestReserveNoJob(t *testing.T) {
	b, ctx := newTestBroker(t)
	_, _, err := b.Reserve(ctx, "w1")
	if !errors.Is(err, ErrNoJob) {
		t.Fatalf("expected ErrNoJob, got %v", err)
	}
}
