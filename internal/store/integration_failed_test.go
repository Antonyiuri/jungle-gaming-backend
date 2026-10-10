//go:build integration

package store_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/store"
)

// setWalletVersion rewrites a wallet's version as the table owner. The movement trigger protects the version
// even from the owner, so it is switched off for the edit. It is how a test reaches a state no real workload
// reaches in practice (2^63-1 movements) without faking the database.
func setWalletVersion(t *testing.T, admin *pgxpool.Pool, walletID string, version int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range []string{
		"ALTER TABLE wallets DISABLE TRIGGER wallet_movement_consistency",
		fmt.Sprintf("UPDATE wallets SET version = %d WHERE id = '%s'", version, walletID),
		"ALTER TABLE wallets ENABLE TRIGGER wallet_movement_consistency",
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func transactionRow(t *testing.T, pool *pgxpool.Pool, id string) (status, code string, ledger, events int) {
	t.Helper()
	ctx := context.Background()
	var failure *string
	if err := pool.QueryRow(ctx, "SELECT status, failure_code FROM wager_transactions WHERE id=$1", id).Scan(&status, &failure); err != nil {
		t.Fatal(err)
	}
	if failure != nil {
		code = *failure
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM wallet_ledger_entries WHERE transaction_id=$1", id).Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1", id).Scan(&events); err != nil {
		t.Fatal(err)
	}
	return status, code, ledger, events
}

// FAILED is a permanent failure recorded for audit. The only one the system can establish is a wallet that
// can no longer advance its version: it needs no timing, no database fault and no guess, and retrying it can
// never succeed. Database errors, timeouts and publisher failures are never promoted to FAILED.
func TestAMovementThatCannotAdvanceTheWalletVersionFailsPermanentlyAndIsAudited(t *testing.T) {
	admin := adminPool(t)
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	setWalletVersion(t, admin, w.ID, math.MaxInt64)
	defer setWalletVersion(t, admin, w.ID, 1)

	bet := p.command(w, "BET", "exhausted-bet", 1000, "")
	first := mustProcess(t, s, bet)
	if first.Status != "FAILED" || first.FailureCode != domain.FailureWalletVersionExhausted || first.IdempotentReplay {
		t.Fatalf("a movement the wallet cannot record must fail permanently: %+v", first)
	}
	if first.Balance == nil || first.Balance.MinorUnits() != 10000 || first.WalletVersion != math.MaxInt64 {
		t.Fatalf("the failure reports the untouched wallet: %+v", first)
	}
	status, code, ledger, events := transactionRow(t, pool, first.TransactionID)
	if status != "FAILED" || code != domain.FailureWalletVersionExhausted {
		t.Fatalf("the failure must be persisted for audit: %s %s", status, code)
	}
	if ledger != 0 || events != 0 {
		t.Fatalf("a failed transaction moves nothing and announces nothing: %d ledger entries, %d events", ledger, events)
	}
	if got, err := s.Wallet(ctx, w.ID); err != nil || got.Balance.MinorUnits() != 10000 || got.Version != math.MaxInt64 {
		t.Fatalf("the wallet must not change: %+v %v", got, err)
	}

	// Permanent means the same answer for the same identity, and never a second attempt.
	again := mustProcess(t, s, bet)
	if again.Status != "FAILED" || again.TransactionID != first.TransactionID || !again.IdempotentReplay || again.FailureCode != first.FailureCode {
		t.Fatalf("a retry must replay the recorded failure: %+v", again)
	}
	var rows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM wager_transactions WHERE provider_id=$1", string(p)).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("a replay must not create another transaction: %d %v", rows, err)
	}
	if _, err := s.Process(ctx, p.command(w, "BET", "exhausted-bet", 2000, ""), nil); domain.Code(err) != "IDEMPOTENCY_CONFLICT" && domain.Code(err) != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("a failed transaction keeps its identity: %v", err)
	}

	// Queue deliveries of a failed transaction are dead-lettered, and a redelivery sees the same failure.
	delivery := &store.Delivery{MessageID: "msg-exhausted-" + string(p), ConsumerName: "wager-processor", Hash: "hash-exhausted", SenderID: string(p)}
	win := p.command(w, "WIN", "exhausted-win", 500, "")
	delivered, err := process(ctx, s, win, delivery)
	if err != nil || delivered.Status != "FAILED" {
		t.Fatalf("a delivery must record the failure too: %+v %v", delivered, err)
	}
	var disposition string
	if err := pool.QueryRow(ctx, "SELECT disposition FROM inbox_messages WHERE consumer_name=$1 AND message_id=$2", delivery.ConsumerName, delivery.MessageID).Scan(&disposition); err != nil || disposition != "DLQ" {
		t.Fatalf("a failed transaction must be dead-lettered, not acknowledged: %q %v", disposition, err)
	}
	redelivered, err := process(ctx, s, win, delivery)
	if err != nil || redelivered.Status != "FAILED" || !redelivered.IdempotentReplay {
		t.Fatalf("a redelivery must see the recorded failure: %+v %v", redelivered, err)
	}

	// Only decisions that need a movement fail: a loss still works, and a bet that is a business rejection
	// anyway stays a rejection.
	if loss := mustProcess(t, s, p.command(w, "LOSS", "exhausted-loss", 0, "")); loss.Status != "PROCESSED" {
		t.Fatalf("a loss needs no movement: %+v", loss)
	}
	if broke := mustProcess(t, s, p.command(w, "BET", "exhausted-broke", 20000, "")); broke.Status != "REJECTED" || broke.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("insufficient funds stays a rejection: %+v", broke)
	}

	// The failure is confined to that wallet.
	healthy := wallet(t, s, 10000)
	if ok := mustProcess(t, s, p.command(healthy, "BET", "healthy-bet", 1000, "")); ok.Status != "PROCESSED" || ok.Balance.MinorUnits() != 9000 {
		t.Fatalf("other wallets are unaffected: %+v", ok)
	}
	assertConsistent(t, s, healthy.ID)
}

// The reference worker takes the same path: a pending reversal whose wallet can no longer record the movement
// ends as FAILED instead of retrying for the whole reference lifetime.
func TestAPendingReferenceFailsPermanentlyWhenItsMovementCannotBeRecorded(t *testing.T) {
	admin := adminPool(t)
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()

	refund := mustProcess(t, s, p.command(w, "REFUND", "late-refund", 1000, "late-bet"))
	if refund.Status != "PENDING_REFERENCE" {
		t.Fatalf("the refund must wait for its bet: %+v", refund)
	}
	if bet := mustProcess(t, s, p.command(w, "BET", "late-bet", 1000, "")); bet.Status != "PROCESSED" {
		t.Fatalf("the bet must be processed: %+v", bet)
	}
	setWalletVersion(t, admin, w.ID, math.MaxInt64)
	defer setWalletVersion(t, admin, w.ID, 2)

	waitFor := func() string {
		for i := 0; i < 40; i++ {
			if err := s.ProcessReferences(ctx, 100); err != nil {
				t.Fatal(err)
			}
			if status, _, _, _ := transactionRow(t, pool, refund.TransactionID); status != "PENDING_REFERENCE" {
				return status
			}
			// The worker backs off between attempts; make the retry due instead of sleeping through the delay.
			if _, err := pool.Exec(ctx, "UPDATE wager_transactions SET next_attempt_at=clock_timestamp() WHERE id=$1 AND status='PENDING_REFERENCE'", refund.TransactionID); err != nil {
				t.Fatal(err)
			}
		}
		return "PENDING_REFERENCE"
	}
	if got := waitFor(); got != "FAILED" {
		t.Fatalf("the refund must end as FAILED, got %s", got)
	}
	status, code, ledger, events := transactionRow(t, pool, refund.TransactionID)
	if status != "FAILED" || code != domain.FailureWalletVersionExhausted || ledger != 0 || events != 1 {
		// events == 1: the earlier PENDING_REFERENCE announcement; FAILED adds none.
		t.Fatalf("unexpected final state: %s %s ledger=%d events=%d", status, code, ledger, events)
	}
	stored, err := s.Transaction(ctx, string(p), refund.TransactionID)
	if err != nil || stored.Status != "FAILED" || stored.FailureCode != domain.FailureWalletVersionExhausted {
		t.Fatalf("the result API must report the failure: %+v %v", stored, err)
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 9000 {
		t.Fatalf("the wallet must not change: %+v", got)
	}
}
