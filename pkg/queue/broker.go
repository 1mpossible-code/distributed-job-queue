package queue

import "context"

type Lease struct {
	Token   string
	JobID   string
	Worker  string
	Expires int64
}

type RetryDecision struct {
	Attempt      int
	MaxAttempts  int
	RetryAtUnix  int64
	LastErrorMsg string
}

type Broker interface {
	Enqueue(ctx context.Context, job Job) error
	Reserve(ctx context.Context, workerID string) (Job, Lease, error)
	Ack(ctx context.Context, lease Lease) error
	Nack(ctx context.Context, lease Lease, decision RetryDecision) error
	RequeueExpired(ctx context.Context) (int64, error)
}

type Handler interface {
	Handle(ctx context.Context, job Job) error
}
