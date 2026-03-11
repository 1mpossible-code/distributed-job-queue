package producer

import (
	"context"
	"testing"

	"distributed_job_queue/pkg/queue"
)

type fakeBroker struct{ job queue.Job }

func (f *fakeBroker) Enqueue(_ context.Context, job queue.Job) error { f.job = job; return nil }
func (f *fakeBroker) Reserve(context.Context, string) (queue.Job, queue.Lease, error) {
	return queue.Job{}, queue.Lease{}, nil
}
func (f *fakeBroker) Ack(context.Context, queue.Lease) error { return nil }
func (f *fakeBroker) Nack(context.Context, queue.Lease, queue.RetryDecision) error { return nil }
func (f *fakeBroker) RequeueExpired(context.Context) (int64, error) { return 0, nil }

func TestEnqueueSetsTimestamp(t *testing.T) {
	fb := &fakeBroker{}
	p := New(fb, nil)
	err := p.Enqueue(context.Background(), queue.Job{
		ID:             "job-1",
		Type:           "t",
		IdempotencyKey: "idem-1",
		Priority:       queue.PriorityHigh,
		MaxAttempts:    3,
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if fb.job.EnqueuedAt.IsZero() {
		t.Fatal("expected enqueued timestamp")
	}
}
