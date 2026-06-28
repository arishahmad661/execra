package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var registerOnce sync.Once

type Metrics struct {
	// Counters (with labels)
	EnqueueTotal      *prometheus.CounterVec
	DequeueTotal      *prometheus.CounterVec
	AckTotal          *prometheus.CounterVec
	NackTotal         *prometheus.CounterVec
	RetryTotal        *prometheus.CounterVec
	DLQTotal          *prometheus.CounterVec
	LeaseExpiredTotal *prometheus.CounterVec
	RaftCommitTotal   *prometheus.CounterVec

	// Error counters (with labels)
	EnqueueErrorsTotal *prometheus.CounterVec
	DequeueErrorsTotal *prometheus.CounterVec
	AckErrorsTotal     *prometheus.CounterVec
	NackErrorsTotal    *prometheus.CounterVec
	StoreErrorsTotal   *prometheus.CounterVec
	RaftErrorsTotal    *prometheus.CounterVec

	// Gauges
	QueueDepth        *prometheus.GaugeVec
	ScheduleDepth     *prometheus.GaugeVec
	InflightJobs      *prometheus.GaugeVec
	DLQSize           *prometheus.GaugeVec
	RaftLeader        *prometheus.GaugeVec
	RaftTerm          prometheus.Gauge
	WorkerUtilization prometheus.Gauge

	// Histograms
	EnqueueLatency    *prometheus.HistogramVec
	DequeueLatency    *prometheus.HistogramVec
	AckLatency        *prometheus.HistogramVec
	NackLatency       *prometheus.HistogramVec
	JobDuration       *prometheus.HistogramVec
	RaftCommitLatency prometheus.Histogram
	LeaseTTLRemaining prometheus.Histogram
}

func NewMetrics() *Metrics {
	latencyBuckets := prometheus.DefBuckets

	m := &Metrics{
		// Counters
		EnqueueTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_enqueue_total",
				Help: "Total number of jobs enqueued.",
			},
			[]string{"queue"},
		),

		DequeueTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_dequeue_total",
				Help: "Total number of jobs dequeued.",
			},
			[]string{"queue"},
		),

		AckTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_ack_total",
				Help: "Total number of acknowledged jobs.",
			},
			[]string{"queue"},
		),

		NackTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_nack_total",
				Help: "Total number of negatively acknowledged jobs.",
			},
			[]string{"queue", "reason"},
		),

		RetryTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_retry_total",
				Help: "Total number of retried jobs.",
			},
			[]string{"queue"},
		),

		DLQTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_dlq_total",
				Help: "Total number of jobs moved to the dead-letter queue.",
			},
			[]string{"queue"},
		),

		LeaseExpiredTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_lease_expired_total",
				Help: "Total number of expired leases.",
			},
			[]string{"queue"},
		),

		RaftCommitTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_raft_commit_total",
				Help: "Total number of Raft commits.",
			},
			[]string{"status"},
		),

		// Error Counters
		EnqueueErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_enqueue_errors_total",
				Help: "Total enqueue errors.",
			},
			[]string{"queue", "error_type"},
		),

		DequeueErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_dequeue_errors_total",
				Help: "Total dequeue errors.",
			},
			[]string{"queue", "error_type"},
		),

		AckErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_ack_errors_total",
				Help: "Total acknowledgement errors.",
			},
			[]string{"queue", "error_type"},
		),

		NackErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_nack_errors_total",
				Help: "Total negative acknowledgement errors.",
			},
			[]string{"queue", "error_type"},
		),

		StoreErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_store_errors_total",
				Help: "Total storage errors.",
			},
			[]string{"operation", "error_type"},
		),

		RaftErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "execra_raft_errors_total",
				Help: "Total Raft errors.",
			},
			[]string{"error_type"},
		),

		// Gauges
		QueueDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "execra_queue_depth",
				Help: "Current queue depth.",
			},
			[]string{"queue", "state"},
		),

		ScheduleDepth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "execra_schedule_depth",
				Help: "Current schedule queue depth.",
			},
			[]string{"queue", "state"},
		),

		InflightJobs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "execra_inflight_jobs",
				Help: "Current number of inflight jobs.",
			},
			[]string{"queue", "state"},
		),

		DLQSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "execra_dlq_size",
				Help: "Current dead-letter queue size.",
			},
			[]string{"queue"},
		),

		RaftLeader: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "execra_raft_leader",
				Help: "Whether the node is the Raft leader (1) or follower (0).",
			},
			[]string{"node_id"},
		),

		RaftTerm: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "execra_raft_term",
				Help: "Current Raft term.",
			},
		),

		WorkerUtilization: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "execra_worker_utilization",
				Help: "Current worker utilization ratio.",
			},
		),

		// Histograms
		EnqueueLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execra_enqueue_latency_seconds",
				Help:    "Enqueue latency.",
				Buckets: latencyBuckets,
			},
			[]string{"queue"},
		),

		DequeueLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execra_dequeue_latency_seconds",
				Help:    "Dequeue latency.",
				Buckets: latencyBuckets,
			},
			[]string{"queue"},
		),

		AckLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execra_ack_latency_seconds",
				Help:    "Ack latency.",
				Buckets: latencyBuckets,
			},
			[]string{"queue"},
		),

		NackLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execra_nack_latency_seconds",
				Help:    "Nack latency.",
				Buckets: latencyBuckets,
			},
			[]string{"queue"},
		),

		JobDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "execra_job_duration_seconds",
				Help:    "Job execution duration.",
				Buckets: latencyBuckets,
			},
			[]string{"queue"},
		),

		RaftCommitLatency: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "execra_raft_commit_latency_seconds",
				Help:    "Raft commit latency.",
				Buckets: latencyBuckets,
			},
		),

		LeaseTTLRemaining: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "execra_lease_ttl_remaining_seconds",
				Help:    "Remaining lease TTL when renewed or released.",
				Buckets: latencyBuckets,
			},
		),
	}

	registerOnce.Do(func() {
		prometheus.MustRegister(
			m.EnqueueTotal,
			m.DequeueTotal,
			m.AckTotal,
			m.NackTotal,
			m.RetryTotal,
			m.DLQTotal,
			m.LeaseExpiredTotal,
			m.RaftCommitTotal,

			m.EnqueueErrorsTotal,
			m.DequeueErrorsTotal,
			m.AckErrorsTotal,
			m.NackErrorsTotal,
			m.StoreErrorsTotal,
			m.RaftErrorsTotal,

			m.QueueDepth,
			m.ScheduleDepth,
			m.InflightJobs,
			m.DLQSize,
			m.RaftLeader,
			m.RaftTerm,
			m.WorkerUtilization,

			m.EnqueueLatency,
			m.DequeueLatency,
			m.AckLatency,
			m.NackLatency,
			m.JobDuration,
			m.RaftCommitLatency,
			m.LeaseTTLRemaining,
		)
	})

	return m
}
