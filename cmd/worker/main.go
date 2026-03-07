package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed_job_queue/pkg/metrics"
	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"
	"distributed_job_queue/pkg/worker"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type handler struct {
	failRate float64
}

func (h handler) Handle(_ context.Context, _ queue.Job) error {
	if rand.Float64() < h.failRate {
		return errors.New("simulated worker failure")
	}
	return nil
}

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	workerID := flag.String("worker-id", "worker-1", "worker id")
	concurrency := flag.Int("concurrency", 4, "worker concurrency")
	metricsAddr := flag.String("metrics-addr", ":2112", "metrics address")
	failRate := flag.Float64("fail-rate", 0.0, "chance to fail a job")
	flag.Parse()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer func() { _ = rdb.Close() }()
	broker := redisbroker.New(rdb, redisbroker.Config{Prefix: "dq"})

	rt := worker.New(broker, handler{failRate: *failRate}, worker.Config{
		WorkerID:    *workerID,
		Concurrency: *concurrency,
		RetryPolicy: queue.ExponentialBackoff{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second, JitterFrac: 0.2},
		Metrics:     m,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = broker.RequeueExpired(ctx)
			}
		}
	}()
	_ = rt.Run(ctx)
}
