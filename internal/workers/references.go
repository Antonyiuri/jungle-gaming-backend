// Package workers runs supervised background loops.
package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
)

// References periodically retries pending reference transactions.
type References struct {
	Store    *store.Store
	Log      *slog.Logger
	Interval time.Duration
}

func (w *References) Run(ctx context.Context) {
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := w.Store.ProcessReferences(ctx, 20); err != nil && ctx.Err() == nil {
				w.Log.Error("reference processing failed", "error", err)
			}
		}
	}
}
