package redisbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"distributed_job_queue/pkg/queue"

	"github.com/redis/go-redis/v9"
)

var ErrNoJob = redis.Nil

type Broker struct {
	rdb            redis.UniversalClient
	prefix         string
	leaseDuration  time.Duration
	idempotencyTTL time.Duration
}

type Config struct {
	Prefix         string
	LeaseDuration  time.Duration
	IdempotencyTTL time.Duration
}

func New(rdb redis.UniversalClient, cfg Config) *Broker {
	if cfg.Prefix == "" {
		cfg.Prefix = "dq"
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	return &Broker{
		rdb:            rdb,
		prefix:         cfg.Prefix,
		leaseDuration:  cfg.LeaseDuration,
		idempotencyTTL: cfg.IdempotencyTTL,
	}
}

func (b *Broker) readyKey(priority queue.Priority) string {
	return fmt.Sprintf("%s:ready:%s", b.prefix, priority)
}

func (b *Broker) retryKey() string {
	return fmt.Sprintf("%s:retry", b.prefix)
}

func (b *Broker) inflightKey() string {
	return fmt.Sprintf("%s:inflight", b.prefix)
}

func (b *Broker) processingKey() string {
	return fmt.Sprintf("%s:processing", b.prefix)
}

func (b *Broker) dlqKey() string {
	return fmt.Sprintf("%s:dlq", b.prefix)
}

func (b *Broker) Enqueue(ctx context.Context, job queue.Job) error {
	if err := job.Validate(); err != nil {
		return err
	}
	if job.EnqueuedAt.IsZero() {
		job.EnqueuedAt = time.Now().UTC()
	}

	idemKey := fmt.Sprintf("%s:idem:%s", b.prefix, job.IdempotencyKey)
	ok, err := b.rdb.SetNX(ctx, idemKey, job.ID, b.idempotencyTTL).Result()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	raw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = b.rdb.RPush(ctx, b.readyKey(job.Priority), raw).Result()
	return err
}
