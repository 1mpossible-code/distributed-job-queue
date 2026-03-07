package redisbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
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

type scheduledRetry struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
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

	// setnx is the simple dedupe gate for duplicate enqueue calls.
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

func (b *Broker) Reserve(ctx context.Context, workerID string) (queue.Job, queue.Lease, error) {
	// move retry jobs that are due back into ready queues.
	if err := b.promoteDueRetries(ctx); err != nil {
		return queue.Job{}, queue.Lease{}, err
	}

	var raw string
	var err error
	for _, priority := range []queue.Priority{queue.PriorityHigh, queue.PriorityMedium, queue.PriorityLow} {
		raw, err = b.rdb.LPop(ctx, b.readyKey(priority)).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return queue.Job{}, queue.Lease{}, err
		}
		break
	}
	if raw == "" {
		return queue.Job{}, queue.Lease{}, ErrNoJob
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return queue.Job{}, queue.Lease{}, err
	}
	lease := queue.Lease{
		Token:   b.newLeaseToken(job.ID),
		JobID:   job.ID,
		Worker:  workerID,
		Expires: time.Now().Add(b.leaseDuration).UnixMilli(),
	}
	// keep both payload and expiry index so expired leases can be recovered.
	if err := b.rdb.HSet(ctx, b.processingKey(), lease.Token, raw).Err(); err != nil {
		return queue.Job{}, queue.Lease{}, err
	}
	if err := b.rdb.ZAdd(ctx, b.inflightKey(), redis.Z{
		Score:  float64(lease.Expires),
		Member: lease.Token,
	}).Err(); err != nil {
		return queue.Job{}, queue.Lease{}, err
	}
	return job, lease, nil
}

func (b *Broker) Ack(ctx context.Context, lease queue.Lease) error {
	if err := b.rdb.ZRem(ctx, b.inflightKey(), lease.Token).Err(); err != nil {
		return err
	}
	return b.rdb.HDel(ctx, b.processingKey(), lease.Token).Err()
}

func (b *Broker) Nack(ctx context.Context, lease queue.Lease, decision queue.RetryDecision) error {
	raw, err := b.rdb.HGet(ctx, b.processingKey(), lease.Token).Result()
	if err != nil {
		return err
	}
	if err := b.rdb.ZRem(ctx, b.inflightKey(), lease.Token).Err(); err != nil {
		return err
	}
	if err := b.rdb.HDel(ctx, b.processingKey(), lease.Token).Err(); err != nil {
		return err
	}

	var job queue.Job
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return err
	}
	job.Attempt = decision.Attempt
	nextRaw, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if decision.Attempt >= decision.MaxAttempts {
		// after max attempts we stop retrying and put it in dlq.
		_, err = b.rdb.RPush(ctx, b.dlqKey(), nextRaw).Result()
		return err
	}
	retryItem, err := json.Marshal(scheduledRetry{ID: lease.Token, Payload: nextRaw})
	if err != nil {
		return err
	}
	return b.rdb.ZAdd(ctx, b.retryKey(), redis.Z{
		Score:  float64(decision.RetryAtUnix),
		Member: retryItem,
	}).Err()
}

func (b *Broker) RequeueExpired(ctx context.Context) (int64, error) {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	tokens, err := b.rdb.ZRangeByScore(ctx, b.inflightKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		return 0, err
	}
	var moved int64
	for _, token := range tokens {
		raw, err := b.rdb.HGet(ctx, b.processingKey(), token).Result()
		if err == redis.Nil {
			_ = b.rdb.ZRem(ctx, b.inflightKey(), token).Err()
			continue
		}
		if err != nil {
			return moved, err
		}
		var job queue.Job
		if err := json.Unmarshal([]byte(raw), &job); err != nil {
			return moved, err
		}
		if err := b.rdb.ZRem(ctx, b.inflightKey(), token).Err(); err != nil {
			return moved, err
		}
		if err := b.rdb.HDel(ctx, b.processingKey(), token).Err(); err != nil {
			return moved, err
		}
		if err := b.rdb.LPush(ctx, b.readyKey(job.Priority), raw).Err(); err != nil {
			return moved, err
		}
		// lpush puts recovered jobs at the front to minimize recovery lag.
		moved++
	}
	return moved, nil
}

func (b *Broker) DLQLen(ctx context.Context) (int64, error) {
	return b.rdb.LLen(ctx, b.dlqKey()).Result()
}

func (b *Broker) promoteDueRetries(ctx context.Context) error {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	items, err := b.rdb.ZRangeByScore(ctx, b.retryKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		return err
	}
	for _, item := range items {
		var sr scheduledRetry
		if err := json.Unmarshal([]byte(item), &sr); err != nil {
			return err
		}
		var job queue.Job
		if err := json.Unmarshal(sr.Payload, &job); err != nil {
			return err
		}
		if err := b.rdb.ZRem(ctx, b.retryKey(), item).Err(); err != nil {
			return err
		}
		if err := b.rdb.RPush(ctx, b.readyKey(job.Priority), sr.Payload).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) newLeaseToken(jobID string) string {
	return fmt.Sprintf("%s-%d-%d", jobID, time.Now().UnixNano(), rand.Int63())
}
