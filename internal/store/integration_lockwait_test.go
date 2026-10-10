//go:build integration

package store_test

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The wait for a wallet lock is what contention on a hot wallet looks like, so it must be measured,
// including the part spent behind a lock held by someone else.
func TestLockWaitIsReportedIncludingTheTimeSpentBehindAnotherTransaction(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()

	var mu sync.Mutex
	var waits []time.Duration
	s.OnLockWait(func(d time.Duration) { mu.Lock(); waits = append(waits, d); mu.Unlock() })

	mustProcess(t, s, p.command(w, "BET", "uncontended", 100, ""))
	mu.Lock()
	if len(waits) != 1 || waits[0] > 500*time.Millisecond {
		t.Fatalf("an uncontended operation reports one short wait, got %v", waits)
	}
	waits = nil
	mu.Unlock()

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback(ctx)
	if _, err := holder.Exec(ctx, "SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE", w.ID); err != nil {
		t.Fatal(err)
	}
	var holderPID int
	if err := holder.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&holderPID); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mustProcess(t, s, p.command(w, "BET", "contended", 100, ""))
	}()
	// Hold the lock for a known time only once the operation is really queued behind it: a fixed sleep measured
	// from the goroutine start also counted the time the operation needed to reach the lock, which is not
	// bounded on a loaded machine (the test failed with 414 ms under -race while other suites ran).
	deadline := time.Now().Add(15 * time.Second)
	for {
		var queued int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", holderPID).Scan(&queued); err != nil {
			t.Fatal(err)
		}
		if queued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the operation never queued behind the held lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(600 * time.Millisecond)
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(waits) != 1 || waits[0] < 500*time.Millisecond {
		t.Fatalf("the operation queued behind a held lock must report the time it waited, got %v", waits)
	}
	assertConsistent(t, s, w.ID)
}
