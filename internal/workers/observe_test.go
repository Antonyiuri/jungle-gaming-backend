package workers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// gauge reads the current value of a gauge through the registry, the way a scrape would.
func gauge(g prometheus.Gauge) float64 {
	var out dto.Metric
	if err := g.Write(&out); err != nil {
		panic(err)
	}
	return out.GetGauge().GetValue()
}

type fakeStats struct {
	stats store.OutboxStats
	err   error
	calls atomic.Int32
}

func (f *fakeStats) OutboxStats(context.Context) (store.OutboxStats, error) {
	f.calls.Add(1)
	return f.stats, f.err
}

func newObserver(stats *fakeStats, dlq func(context.Context) (float64, error)) (*Observer, *observability.Metrics) {
	m := observability.New()
	return &Observer{Store: stats, DLQDepth: dlq, Metrics: m, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Interval: 10 * time.Millisecond}, m
}

func TestObserverPublishesOutboxLagAndDeadLetterDepth(t *testing.T) {
	stats := &fakeStats{stats: store.OutboxStats{Pending: 7, OldestAge: 42 * time.Second}}
	o, m := newObserver(stats, func(context.Context) (float64, error) { return 3, nil })
	o.refresh(context.Background())
	if gauge(m.OutboxPending) != 7 || gauge(m.OutboxAge) != 42 || gauge(m.DLQ) != 3 {
		t.Fatalf("gauges: pending=%v age=%v dlq=%v", gauge(m.OutboxPending), gauge(m.OutboxAge), gauge(m.DLQ))
	}
}

// A probe that fails must leave the last known value alone: reporting zero would look like a healthy, empty queue.
func TestObserverKeepsTheLastValueWhenAProbeFails(t *testing.T) {
	stats := &fakeStats{stats: store.OutboxStats{Pending: 5, OldestAge: time.Minute}}
	failing := false
	o, m := newObserver(stats, func(context.Context) (float64, error) {
		if failing {
			return 0, errors.New("broker unavailable")
		}
		return 9, nil
	})
	o.refresh(context.Background())
	failing, stats.err = true, errors.New("database unavailable")
	o.refresh(context.Background())
	if gauge(m.OutboxPending) != 5 || gauge(m.DLQ) != 9 {
		t.Fatalf("a failed probe must not reset the gauges: pending=%v dlq=%v", gauge(m.OutboxPending), gauge(m.DLQ))
	}
}

func TestObserverWithoutADeadLetterQueueStillReportsTheOutbox(t *testing.T) {
	o, m := newObserver(&fakeStats{stats: store.OutboxStats{Pending: 2}}, nil)
	o.refresh(context.Background())
	if gauge(m.OutboxPending) != 2 {
		t.Fatal("the outbox gauge must not depend on the dead-letter probe")
	}
}

func TestObserverRefreshesPeriodicallyAndStopsWhenCancelled(t *testing.T) {
	stats := &fakeStats{}
	o, _ := newObserver(stats, nil)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { defer close(finished); o.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for stats.calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stats.calls.Load() < 3 {
		t.Fatalf("the observer must keep refreshing, got %d calls", stats.calls.Load())
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("the observer must stop when its context ends")
	}
}
