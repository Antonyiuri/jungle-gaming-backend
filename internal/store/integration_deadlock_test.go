//go:build integration

package store_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The documented lock order (inbox, then wallet) prevents deadlocks between the application's own
// transactions, but the repeat-on-deadlock safety net must work when someone else breaks that order.
// The test builds the cycle by hand: it holds the wallet lock, lets Process insert its inbox row and
// block on the wallet, then asks for that same inbox row. PostgreSQL resolves the cycle by aborting the
// first waiter that runs its deadlock check, which is Process because it began waiting first.
func TestDeadlockVictimIsRepeatedTransparentlyAndCounted(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	var conflicts atomic.Int32
	s.OnConflict(func() { conflicts.Add(1) })

	w, p := wallet(t, s, 10000), newProvider()
	command := p.command(w, "BET", "deadlock", 1000, "")
	message := uuid.NewString()
	delivery := &store.Delivery{MessageID: message, ConsumerName: "wager-processor", Hash: "h", SenderID: "client"}

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	if _, err := holder.Exec(ctx, "SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE", w.ID); err != nil {
		t.Fatal(err)
	}

	type outcome struct {
		result domain.Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		r, err := s.Process(ctx, command, delivery)
		done <- outcome{r, err}
	}()

	// Process has inserted its inbox row and is now waiting for the wallet lock.
	deadline := time.Now().Add(10 * time.Second)
	for queryLockWaiters(t, pool) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Process never blocked on the wallet lock")
		}
		time.Sleep(20 * time.Millisecond)
	}

	inserted := make(chan error, 1)
	go func() {
		_, err := holder.Exec(ctx, `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash,sender_id) VALUES('wager-processor',$1,'other','client')`, message)
		inserted <- err
	}()
	select {
	case err := <-inserted:
		if err != nil {
			t.Fatalf("the hand-made transaction must outlive the deadlock, it was chosen as the victim instead: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the deadlock was never resolved")
	}
	// Release both locks: the repeated attempt can now run to completion.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-done:
		if got.err != nil || got.result.Status != "PROCESSED" || got.result.Balance.MinorUnits() != 9000 || got.result.IdempotentReplay {
			t.Fatalf("the victim must be repeated and succeed: %+v %v", got.result, got.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the repeated attempt never finished")
	}
	if n := conflicts.Load(); n < 1 {
		t.Fatalf("the deadlock must be counted, got %d", n)
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 9000 || got.Version != 2 {
		t.Fatalf("the bet must be applied exactly once: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

func queryLockWaiters(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%FOR UPDATE%' AND query LIKE '%wallets%' AND pid<>pg_backend_pid()").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
