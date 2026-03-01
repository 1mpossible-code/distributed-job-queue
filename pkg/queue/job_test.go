package queue

import "testing"

func TestJobValidate(t *testing.T) {
	t.Run("valid job", func(t *testing.T) {
		j := Job{
			ID:             "job-1",
			Type:           "email",
			IdempotencyKey: "idem-1",
			Priority:       PriorityHigh,
			MaxAttempts:    3,
		}
		if err := j.Validate(); err != nil {
			t.Fatalf("expected valid job, got error: %v", err)
		}
	})

	t.Run("invalid priority", func(t *testing.T) {
		j := Job{
			ID:             "job-1",
			Type:           "email",
			IdempotencyKey: "idem-1",
			Priority:       "urgent",
			MaxAttempts:    3,
		}
		if err := j.Validate(); err == nil {
			t.Fatal("expected validation error")
		}
	})
}
