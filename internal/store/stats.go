package store

import (
	"context"
	"time"
)

// OutboxStats describes the unpublished backlog; a growing age signals a stuck publisher or broker.
type OutboxStats struct {
	Pending   int64
	OldestAge time.Duration
}

func (s *Store) OutboxStats(ctx context.Context) (OutboxStats, error) {
	var pending int64
	var seconds float64
	err := s.pool.QueryRow(ctx, `SELECT count(*), COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(occurred_at)), 0)::FLOAT8
		FROM outbox_events WHERE published_at IS NULL`).Scan(&pending, &seconds)
	if err != nil {
		return OutboxStats{}, err
	}
	if seconds < 0 {
		seconds = 0
	}
	return OutboxStats{Pending: pending, OldestAge: time.Duration(seconds * float64(time.Second))}, nil
}
