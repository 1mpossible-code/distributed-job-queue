package queue

import (
	"testing"
	"time"
)

func TestExponentialBackoffNextDelay(t *testing.T) {
	p := ExponentialBackoff{
		BaseDelay:  time.Second,
		MaxDelay:   8 * time.Second,
		JitterFrac: 0,
	}

	if got := p.NextDelay(1); got != time.Second {
		t.Fatalf("attempt 1 expected 1s, got %s", got)
	}
	if got := p.NextDelay(2); got != 2*time.Second {
		t.Fatalf("attempt 2 expected 2s, got %s", got)
	}
	if got := p.NextDelay(5); got != 8*time.Second {
		t.Fatalf("attempt 5 expected cap 8s, got %s", got)
	}
}
