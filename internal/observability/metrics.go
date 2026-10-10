package observability

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	Registry                                   *prometheus.Registry
	Results                                    *prometheus.CounterVec
	Replays, Conflicts, ReconciliationMismatch prometheus.Counter
	ConcurrencyConflicts                       prometheus.Counter
	Retries                                    *prometheus.CounterVec
	// Duration is the processing time of one request or message, by transport ("http" or "sqs").
	Duration                      *prometheus.HistogramVec
	LockWait                      prometheus.Histogram
	OutboxPending, OutboxAge, DLQ prometheus.Gauge
}

func New() *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry(),
		Results:                prometheus.NewCounterVec(prometheus.CounterOpts{Name: "wager_transactions_total", Help: "Durable wager results observed."}, []string{"status", "kind"}),
		Replays:                prometheus.NewCounter(prometheus.CounterOpts{Name: "idempotent_replays_total", Help: "Idempotent financial replays."}),
		Conflicts:              prometheus.NewCounter(prometheus.CounterOpts{Name: "idempotency_conflicts_total", Help: "Identity conflicts."}),
		ReconciliationMismatch: prometheus.NewCounter(prometheus.CounterOpts{Name: "wallet_reconciliation_mismatches_total", Help: "Detected ledger discrepancies."}),
		ConcurrencyConflicts:   prometheus.NewCounter(prometheus.CounterOpts{Name: "concurrency_conflicts_total", Help: "Transactions repeated after a deadlock or serialization failure."}),
		Retries:                prometheus.NewCounterVec(prometheus.CounterOpts{Name: "processing_retries_total", Help: "Retried work."}, []string{"reason"}),
		Duration:               prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "wager_processing_duration_seconds", Help: "Wager processing duration by transport."}, []string{"transport"}),
		LockWait: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "wallet_lock_wait_seconds", Help: "Time spent waiting for a wallet row lock; sustained growth means contention on hot wallets.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_pending_events", Help: "Unpublished events."}),
		OutboxAge:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "outbox_oldest_pending_age_seconds", Help: "Age of oldest unpublished event."}),
		DLQ:           prometheus.NewGauge(prometheus.GaugeOpts{Name: "sqs_dlq_messages", Help: "Approximate messages waiting in input DLQ."}),
	}
	// Both series exist from the start, so a dashboard sees the SQS path before its first message.
	m.Duration.WithLabelValues("http")
	m.Duration.WithLabelValues("sqs")
	m.Registry.MustRegister(m.Results, m.Replays, m.Conflicts, m.ReconciliationMismatch, m.Retries, m.ConcurrencyConflicts, m.Duration, m.LockWait, m.OutboxPending, m.OutboxAge, m.DLQ, prometheus.NewGoCollector())
	return m
}
