package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"distributed_job_queue/pkg/metrics"
	"distributed_job_queue/pkg/queue"
	"distributed_job_queue/pkg/redisbroker"
	"distributed_job_queue/pkg/worker"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

type handler struct{}

func (handler) Handle(_ context.Context, _ queue.Job) error { return nil }

func main() {
	redisAddr := flag.String("redis", "127.0.0.1:6379", "redis address")
	workerID := flag.String("worker-id", "worker-1", "worker id")
	concurrency := flag.Int("concurrency", 4, "worker concurrency")
	metricsAddr := flag.String("metrics-addr", ":2112", "metrics address")
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

	rt := worker.New(broker, handler{}, worker.Config{
		WorkerID:    *workerID,
		Concurrency: *concurrency,
		RetryPolicy: queue.ExponentialBackoff{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second, JitterFrac: 0.2},
		Metrics:     m,
	})

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = broker.RequeueExpired(context.Background())
		}
	}()
	_ = rt.Run(context.Background())
}
