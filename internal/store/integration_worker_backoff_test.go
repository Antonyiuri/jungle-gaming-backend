//go:build integration

package store_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestAFailingReferenceIsRetriedWithBackoffAndNeverEndsAsFailed(t *testing.T) {
	admin := adminPool(t)
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()

	refund := mustProcess(t, s, p.command(w, "REFUND", "backoff-refund", 1000, "backoff-bet"))
	if refund.Status != "PENDING_REFERENCE" {
		t.Fatalf("the refund must wait for its bet: %+v", refund)
	}
	if bet := mustProcess(t, s, p.command(w, "BET", "backoff-bet", 1000, "")); bet.Status != "PROCESSED" {
		t.Fatalf("the bet must be processed: %+v", bet)
	}

	// A database fault that only affects this transaction: its status cannot be changed, so every attempt of
	// the worker fails with a genuine PostgreSQL error, the way a persistent storage problem would.
	name := "inject_" + strings.ReplaceAll(refund.TransactionID, "-", "")
	for _, statement := range []string{
		fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected storage failure' USING ERRCODE = '58000'; END $$`, name),
		fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE ON wager_transactions FOR EACH ROW WHEN (OLD.id = '%s' AND NEW.status IS DISTINCT FROM OLD.status) EXECUTE FUNCTION %s()`, name, refund.TransactionID, name),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	removed := false
	removeFault := func() {
		if removed {
			return
		}
		removed = true
		for _, statement := range []string{
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON wager_transactions", name),
			fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", name),
		} {
			if _, err := admin.Exec(ctx, statement); err != nil {
				t.Errorf("%s: %v", statement, err)
			}
		}
	}
	defer removeFault()

	state := func() (status string, attempts int, wait time.Duration) {
		var due *time.Time // NULL once the transaction left PENDING_REFERENCE
		var now time.Time
		if err := pool.QueryRow(ctx, "SELECT status, attempt_count, next_attempt_at, clock_timestamp() FROM wager_transactions WHERE id=$1", refund.TransactionID).Scan(&status, &attempts, &due, &now); err != nil {
			t.Fatal(err)
		}
		if due != nil {
			wait = due.Sub(now)
		}
		return status, attempts, wait
	}
	makeDue := func() {
		if _, err := pool.Exec(ctx, "UPDATE wager_transactions SET next_attempt_at=clock_timestamp() WHERE id=$1", refund.TransactionID); err != nil {
			t.Fatal(err)
		}
	}

	// The first attempt fails and the next one is pushed into the future instead of being retried at lease expiry.
	makeDue()
	if err := s.ProcessReferences(ctx, 100); err == nil {
		t.Fatal("the injected failure must surface to the worker")
	}
	status, attempts, wait := state()
	if status != "PENDING_REFERENCE" || attempts != 1 || wait < 500*time.Millisecond || wait > 3*time.Second {
		t.Fatalf("after one failure: status=%s attempts=%d next attempt in %s", status, attempts, wait)
	}
	// Not due yet: the worker must leave it alone, so a persistent error cannot turn into a hot loop.
	if err := s.ProcessReferences(ctx, 100); err != nil {
		t.Fatalf("nothing is due, nothing should fail: %v", err)
	}
	if _, again, _ := state(); again != 1 {
		t.Fatalf("a deferred job must not be claimed before it is due: attempts=%d", again)
	}
	// The delay grows with the number of attempts.
	makeDue()
	if err := s.ProcessReferences(ctx, 100); err == nil {
		t.Fatal("the failure is still there")
	}
	if status, attempts, wait = state(); status != "PENDING_REFERENCE" || attempts != 2 || wait < 1*time.Second || wait > 5*time.Second {
		t.Fatalf("after two failures: status=%s attempts=%d next attempt in %s", status, attempts, wait)
	}

	// The fault goes away: the same transaction completes, because the error never decided its fate.
	removeFault()
	makeDue()
	if err := s.ProcessReferences(ctx, 100); err != nil {
		t.Fatalf("after the fault is gone: %v", err)
	}
	if status, _, _ := state(); status != "PROCESSED" {
		t.Fatalf("the refund must complete once storage recovers, got %s", status)
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 10000 || got.Version != 3 {
		t.Fatalf("the refund moved the wallet exactly once: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}
