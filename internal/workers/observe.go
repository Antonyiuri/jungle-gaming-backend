package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
)

// Observer refreshes the gauges that cannot be derived from request traffic: the unpublished outbox
// backlog and the depth of the input dead-letter queue.
type Observer struct {
	Store interface {
		OutboxStats(context.Context) (store.OutboxStats, error)
	}
	DLQDepth func(context.Context) (float64, error) // nil when no dead-letter queue is configured
	Metrics  *observability.Metrics
	Log      *slog.Logger
	Interval time.Duration
}

func (o *Observer) Run(ctx context.Context) {
	o.refresh(ctx)
	t := time.NewTicker(o.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			o.refresh(ctx)
		}
	}
}

// refresh gives each probe its own deadline so a slow one cannot starve the other.
func (o *Observer) refresh(ctx context.Context) {
	statsCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	stats, err := o.Store.OutboxStats(statsCtx)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			o.Log.Warn("outbox statistics unavailable", "error", err)
		}
	} else {
		o.Metrics.OutboxPending.Set(float64(stats.Pending))
		o.Metrics.OutboxAge.Set(stats.OldestAge.Seconds())
	}
	if o.DLQDepth == nil {
		return
	}
	dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if depth, err := o.DLQDepth(dlqCtx); err != nil {
		if ctx.Err() == nil {
			o.Log.Warn("dead-letter queue depth unavailable", "error", err)
		}
	} else {
		o.Metrics.DLQ.Set(depth)
	}
}
