package observability

import (
	"testing"
)

// valueOf returns the value of an unlabeled counter or gauge, and the number of series of the family.
func valueOf(t *testing.T, m *Metrics, name string) (float64, int) {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if len(family.Metric) == 0 {
			return 0, 0
		}
		metric := family.Metric[0]
		switch {
		case metric.Gauge != nil:
			return metric.Gauge.GetValue(), len(family.Metric)
		case metric.Counter != nil:
			return metric.Counter.GetValue(), len(family.Metric)
		case metric.Histogram != nil:
			return float64(metric.Histogram.GetSampleCount()), len(family.Metric)
		}
	}
	t.Fatalf("metric %s is not exported", name)
	return 0, 0
}

// The operational questions the service must answer from /metrics: what results were produced, how often work
// was repeated or deduplicated, whether the dead-letter queue and the outbox are growing, whether wallets are
// contended, and whether the ledger and the balance diverge.
func TestEveryRequiredMetricIsExported(t *testing.T) {
	m := New()
	m.Results.WithLabelValues("PROCESSED", "BET").Inc()
	m.Retries.WithLabelValues("consumer").Inc()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	exported := map[string]bool{}
	for _, family := range families {
		exported[family.GetName()] = true
	}
	for _, name := range []string{
		"wager_transactions_total",          // status results
		"idempotent_replays_total",          // duplicates
		"idempotency_conflicts_total",       // identity conflicts
		"processing_retries_total",          // retries
		"sqs_dlq_messages",                  // dead-letter queue depth
		"concurrency_conflicts_total",       // deadlocks and serialization failures
		"wallet_lock_wait_seconds",          // contention on hot wallets
		"outbox_pending_events",             // outbox lag (count)
		"outbox_oldest_pending_age_seconds", // outbox lag (age)
		"wager_processing_duration_seconds", // processing latency
		"wallet_reconciliation_mismatches_total",
	} {
		if !exported[name] {
			t.Errorf("metric %s is not exported", name)
		}
	}
}

func TestProcessingLatencyIsSplitByTransportFromTheStart(t *testing.T) {
	m := New()
	// Both series exist before any traffic, so a dashboard sees the SQS path without waiting for a message.
	if _, series := valueOf(t, m, "wager_processing_duration_seconds"); series != 2 {
		t.Fatalf("expected the http and sqs series, got %d", series)
	}
	m.Duration.WithLabelValues("sqs").Observe(0.01)
	if _, series := valueOf(t, m, "wager_processing_duration_seconds"); series != 2 {
		t.Fatalf("observing must not add series: %d", series)
	}
}

func TestMetricsOfDifferentInstancesDoNotShareState(t *testing.T) {
	a, b := New(), New()
	a.Replays.Inc()
	if got, _ := valueOf(t, a, "idempotent_replays_total"); got != 1 {
		t.Fatalf("the instance that counted must see it: %v", got)
	}
	if got, _ := valueOf(t, b, "idempotent_replays_total"); got != 0 {
		t.Fatal("every New must own its registry and counters (the process and each test build their own)")
	}
}
