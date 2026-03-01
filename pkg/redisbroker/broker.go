package redisbroker

import (
	"fmt"
	"time"

	"distributed_job_queue/pkg/queue"

	"github.com/redis/go-redis/v9"
)

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
