//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/trace"
	"github.com/google/uuid"
)

func TestEventsCarryTheRequestCorrelationAndFallBackToTheTransaction(t *testing.T) {
	s, pool := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	correlation := "req-" + uuid.NewString()[:8]

	traced, err := s.Process(trace.WithCorrelation(context.Background(), correlation), p.command(w, "BET", "traced", 100, ""), &store.Delivery{MessageID: "msg-" + correlation, ConsumerName: "wager-processor", Hash: "h", SenderID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	plain := mustProcess(t, s, p.command(w, "BET", "plain", 100, ""))

	read := func(transaction string) (correlationID, causationID string, n int) {
		rows, err := pool.Query(context.Background(), "SELECT payload->>'correlationId', COALESCE(payload->>'causationId','') FROM outbox_events WHERE transaction_id=$1", transaction)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var c, k string
			if err := rows.Scan(&c, &k); err != nil {
				t.Fatal(err)
			}
			if n > 0 && (c != correlationID || k != causationID) {
				t.Fatalf("all events of one transaction share their correlation: %q/%q vs %q/%q", c, k, correlationID, causationID)
			}
			correlationID, causationID, n = c, k, n+1
		}
		return
	}
	if c, k, n := read(traced.TransactionID); n != 2 || c != correlation || k != "msg-"+correlation {
		t.Fatalf("traced events: correlation=%q causation=%q count=%d", c, k, n)
	}
	if c, k, n := read(plain.TransactionID); n != 2 || c != plain.TransactionID || k != "" {
		t.Fatalf("untraced events fall back to the transaction ID: correlation=%q causation=%q count=%d", c, k, n)
	}
	// An unsafe identifier is ignored instead of failing the financial operation.
	unsafe := mustProcessCtx(t, s, trace.WithCorrelation(context.Background(), "bad id\twith control"), p.command(w, "BET", "unsafe", 100, ""))
	if c, _, n := read(unsafe.TransactionID); n != 2 || c != unsafe.TransactionID {
		t.Fatalf("an unsafe correlation must be dropped: %q (%d events)", c, n)
	}
}

func mustProcessCtx(t *testing.T, s *store.Store, ctx context.Context, c domain.Command) domain.Result {
	t.Helper()
	r, err := process(ctx, s, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOutboxStatsReflectTheUnpublishedBacklog(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	before, err := s.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := wallet(t, s, 1000) // two opening events
	after, err := s.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.Pending < before.Pending+2 {
		t.Fatalf("the two new events must be counted: before=%d after=%d", before.Pending, after.Pending)
	}
	if after.OldestAge < 0 || after.OldestAge > 24*time.Hour*365 {
		t.Fatalf("implausible age %s", after.OldestAge)
	}
	// Drain everything this wallet produced and make sure the count follows.
	if _, err := pool.Exec(ctx, "UPDATE outbox_events SET published_at=clock_timestamp() WHERE wallet_id=$1", w.ID); err != nil {
		t.Fatal(err)
	}
	drained, err := s.OutboxStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if drained.Pending > after.Pending-2 {
		t.Fatalf("published events must leave the backlog: after=%d drained=%d", after.Pending, drained.Pending)
	}
}
