package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"
)

type fakeBroker struct {
	mu     sync.Mutex
	jobs   []queue.Job
	acked  int
	nacked int
}

func (f *fakeBroker) Enqueue(context.Context, queue.Job) error { return nil }

func (f *fakeBroker) Reserve(_ context.Context, workerID string) (queue.Job, queue.Lease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.jobs) == 0 {
		return queue.Job{}, queue.Lease{}, redisbroker.ErrNoJob
	}
	job := f.jobs[0]
	f.jobs = f.jobs[1:]
	return job, queue.Lease{Token: "lease-1", JobID: job.ID, Worker: workerID}, nil
}

func (f *fakeBroker) Ack(_ context.Context, _ queue.Lease) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked++
	return nil
}

func (f *fakeBroker) Nack(_ context.Context, _ queue.Lease, _ queue.RetryDecision) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nacked++
	return nil
}

func (f *fakeBroker) RequeueExpired(_ context.Context) (int64, error) { return 0, nil }

type fakeHandler struct{ err error }

func (h fakeHandler) Handle(context.Context, queue.Job) error { return h.err }

func TestWorkerAcksOnSuccess(t *testing.T) {
	fb := &fakeBroker{jobs: []queue.Job{{ID: "j1", Type: "t", IdempotencyKey: "i1", Priority: queue.PriorityHigh, MaxAttempts: 3}}}
	rt := New(fb, fakeHandler{}, Config{WorkerID: "w1", PollInterval: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_ = rt.Run(ctx)
	if fb.acked == 0 {
		t.Fatal("expected ack")
	}
}

func TestWorkerNacksOnFailure(t *testing.T) {
	fb := &fakeBroker{jobs: []queue.Job{{ID: "j1", Type: "t", IdempotencyKey: "i1", Priority: queue.PriorityHigh, MaxAttempts: 3}}}
	rt := New(fb, fakeHandler{err: errors.New("boom")}, Config{WorkerID: "w1", PollInterval: time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(10 * time.Millisecond); cancel() }()
	_ = rt.Run(ctx)
	if fb.nacked == 0 {
		t.Fatal("expected nack")
	}
}

func TestWorkerStopsPromptlyOnCancel(t *testing.T) {
	fb := &fakeBroker{}
	rt := New(fb, fakeHandler{}, Config{WorkerID: "w1", PollInterval: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	go func() { time.Sleep(5 * time.Millisecond); cancel() }()
	_ = rt.Run(ctx)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("worker shutdown took too long")
	}
}
