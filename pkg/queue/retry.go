package queue

import (
	"math/rand"
	"time"
)

type RetryPolicy interface {
	NextDelay(attempt int) time.Duration
}

type ExponentialBackoff struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	JitterFrac float64
}

func (b ExponentialBackoff) NextDelay(attempt int) time.Duration {
	// make sure first failure still gets a base delay.
	if attempt < 1 {
		attempt = 1
	}
	// each attempt doubles until max delay.
	delay := b.BaseDelay * time.Duration(1<<(attempt-1))
	if delay > b.MaxDelay {
		delay = b.MaxDelay
	}
	if b.JitterFrac <= 0 {
		return delay
	}
	jitter := int64(float64(delay) * b.JitterFrac)
	delta := rand.Int63n(2*jitter+1) - jitter
	return delay + time.Duration(delta)
}
