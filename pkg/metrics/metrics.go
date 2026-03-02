package metrics

import "github.com/prometheus/client_golang/prometheus"

type Collector struct {
	Enqueued             prometheus.Counter
	Processed            *prometheus.CounterVec
	Retries              prometheus.Counter
	DLQ                  prometheus.Counter
	ActiveWorkers        prometheus.Gauge
	LatencyMilliseconds  prometheus.Histogram
}

func New(reg prometheus.Registerer) *Collector {
	c := &Collector{
		Enqueued: prometheus.NewCounter(prometheus.CounterOpts{Name: "dq_enqueued_total", Help: "total enqueued jobs"}),
		Processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "dq_processed_total", Help: "total processed jobs by outcome",
		}, []string{"outcome"}),
		Retries:       prometheus.NewCounter(prometheus.CounterOpts{Name: "dq_retries_total", Help: "total retries"}),
		DLQ:           prometheus.NewCounter(prometheus.CounterOpts{Name: "dq_dlq_total", Help: "total jobs moved to dead-letter queue"}),
		ActiveWorkers: prometheus.NewGauge(prometheus.GaugeOpts{Name: "dq_active_workers", Help: "number of worker goroutines"}),
		LatencyMilliseconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "dq_enqueue_to_completion_ms", Help: "latency from enqueue to completion in milliseconds",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000},
		}),
	}
	reg.MustRegister(c.Enqueued, c.Processed, c.Retries, c.DLQ, c.ActiveWorkers, c.LatencyMilliseconds)
	return c
}
