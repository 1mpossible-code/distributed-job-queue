package queue

import (
	"errors"
	"time"
)

type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityMedium Priority = "medium"
	PriorityLow    Priority = "low"
)

type Job struct {
	ID             string
	Type           string
	Payload        []byte
	IdempotencyKey string
	Priority       Priority
	Attempt        int
	MaxAttempts    int
	EnqueuedAt     time.Time
}

func (j Job) Validate() error {
	if j.ID == "" {
		return errors.New("job id is required")
	}
	if j.Type == "" {
		return errors.New("job type is required")
	}
	if j.IdempotencyKey == "" {
		return errors.New("idempotency key is required")
	}
	if j.MaxAttempts < 1 {
		return errors.New("max attempts must be at least 1")
	}
	if j.Priority != PriorityHigh && j.Priority != PriorityMedium && j.Priority != PriorityLow {
		return errors.New("invalid priority")
	}
	return nil
}
