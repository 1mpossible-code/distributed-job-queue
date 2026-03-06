package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"distributed_job_queue/pkg/metrics"
	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"
)

type Config struct {
	WorkerID     string
	Concurrency  int
	PollInterval time.Duration
	RetryPolicy  queue.RetryPolicy
	Metrics      *metrics.Collector
}

type Runtime struct {
	broker  queue.Broker
	handler queue.Handler
	cfg     Config
}

func New(broker queue.Broker, handler queue.Handler, cfg Config) *Runtime {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 50 * time.Millisecond
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = queue.ExponentialBackoff{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second}
	}
	return &Runtime{broker: broker, handler: handler, cfg: cfg}
}

func (r *Runtime) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.cfg.Metrics != nil {
				r.cfg.Metrics.ActiveWorkers.Inc()
				defer r.cfg.Metrics.ActiveWorkers.Dec()
			}
			r.loop(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
	return nil
}

func (r *Runtime) loop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, lease, err := r.broker.Reserve(ctx, r.cfg.WorkerID)
		if err != nil {
			if errors.Is(err, redisbroker.ErrNoJob) {
				if !waitForNextPoll(ctx, ticker.C) {
					return
				}
				continue
			}
			if !waitForNextPoll(ctx, ticker.C) {
				return
			}
			continue
		}
		if err := r.handler.Handle(ctx, job); err == nil {
			_ = r.broker.Ack(ctx, lease)
			if r.cfg.Metrics != nil {
				r.cfg.Metrics.Processed.WithLabelValues("success").Inc()
				if !job.EnqueuedAt.IsZero() {
					r.cfg.Metrics.LatencyMilliseconds.Observe(float64(time.Since(job.EnqueuedAt).Milliseconds()))
				}
			}
			continue
		}
		nextAttempt := job.Attempt + 1
		_ = r.broker.Nack(ctx, lease, queue.RetryDecision{
			Attempt:     nextAttempt,
			MaxAttempts: job.MaxAttempts,
			RetryAtUnix: time.Now().Add(r.cfg.RetryPolicy.NextDelay(nextAttempt)).UnixMilli(),
		})
		if r.cfg.Metrics != nil {
			if nextAttempt >= job.MaxAttempts {
				r.cfg.Metrics.DLQ.Inc()
				r.cfg.Metrics.Processed.WithLabelValues("failed").Inc()
			} else {
				r.cfg.Metrics.Retries.Inc()
				r.cfg.Metrics.Processed.WithLabelValues("retry").Inc()
			}
		}
	}
}

func waitForNextPoll(ctx context.Context, tick <-chan time.Time) bool {
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	}
}
