package producer

import (
	"context"
	"time"

	"distributed_job_queue/pkg/metrics"
	"distributed_job_queue/pkg/queue"
)

type Publisher struct {
	broker  queue.Broker
	metrics *metrics.Collector
}

func New(broker queue.Broker, m *metrics.Collector) *Publisher {
	return &Publisher{broker: broker, metrics: m}
}

func (p *Publisher) Enqueue(ctx context.Context, job queue.Job) error {
	if job.EnqueuedAt.IsZero() {
		job.EnqueuedAt = time.Now().UTC()
	}
	if err := p.broker.Enqueue(ctx, job); err != nil {
		return err
	}
	if p.metrics != nil {
		p.metrics.Enqueued.Inc()
	}
	return nil
}
