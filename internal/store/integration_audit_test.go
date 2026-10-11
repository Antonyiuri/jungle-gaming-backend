//go:build integration

package store_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentWalletCreationCreditsOnce(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	player := uuid.NewString()
	const workers = 12
	results := make([]error, workers)
	wallets := make([]domain.WalletSnapshot, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			wallets[i], results[i] = s.CreateWallet(ctx, player, money(t, 5000))
		}()
	}
	close(start)
	wg.Wait()
	created := 0
	var winner domain.WalletSnapshot
	for i, err := range results {
		switch {
		case err == nil:
			created++
			winner = wallets[i]
		case domain.Code(err) != "WALLET_ALREADY_EXISTS":
			t.Fatalf("unexpected error %v", err)
		}
	}
	if created != 1 {
		t.Fatalf("exactly one creation may win, got %d", created)
	}
	var credits int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM wallet_ledger_entries l JOIN wallets w ON w.id=l.wallet_id WHERE w.player_id=$1", player).Scan(&credits); err != nil || credits != 1 {
		t.Fatalf("the opening credit must exist once, got %d (%v)", credits, err)
	}
	assertConsistent(t, s, winner.ID)
}

func TestReplayKeepsTheOriginalBalanceAfterTheWalletMoves(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	bet := p.command(w, "BET", "snap-bet", 1000, "")
	first := mustProcess(t, s, bet)
	mustProcess(t, s, p.command(w, "WIN", "snap-win", 5000, ""))
	mustProcess(t, s, p.command(w, "BET", "snap-bet-2", 2000, ""))
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 12000 || got.Version != 4 {
		t.Fatalf("wallet before the replay: %+v", got)
	}
	replay := mustProcess(t, s, bet)
	if !replay.IdempotentReplay || replay.Balance.MinorUnits() != 9000 || replay.WalletVersion != first.WalletVersion || replay.TransactionID != first.TransactionID {
		t.Fatalf("a replay must return the balance of the original operation: %+v (original %+v)", replay, first)
	}
	stored, err := s.Transaction(ctx, string(p), first.TransactionID)
	if err != nil || stored.Balance.MinorUnits() != 9000 {
		t.Fatalf("the stored result must stay frozen: %+v %v", stored, err)
	}
}

func TestReversalOfAWinNeverOverdraws(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 1000), newProvider()
	mustProcess(t, s, p.command(w, "WIN", "big-win", 5000, ""))
	mustProcess(t, s, p.command(w, "BET", "spend", 5500, ""))
	r := mustProcess(t, s, p.command(w, "ROLLBACK", "undo-win", 5000, "big-win"))
	if r.Status != "REJECTED" || r.FailureCode != "REVERSAL_INSUFFICIENT_FUNDS" || r.Balance.MinorUnits() != 500 {
		t.Fatalf("a win cannot be rolled back without the funds: %+v", r)
	}
	assertConsistent(t, s, w.ID)
}

// The runtime role is deliberately allowed to INSERT ledger rows, so the database itself must
// refuse entries that break the books, even when the application is the one asking.
func TestDatabaseRefusesInconsistentFinancialWrites(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	r := mustProcess(t, s, p.command(w, "BET", "legit", 1000, ""))
	txnID, walletID := r.TransactionID, w.ID
	entry := func(txn, currency string, before, after int64, version int) string {
		return fmt.Sprintf(`INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount_minor,currency,balance_before,balance_after,wallet_version) VALUES(gen_random_uuid(),'%s','%s','DEBIT',1000,'%s',%d,%d,%d)`, walletID, txn, currency, before, after, version)
	}
	statements := map[string]string{
		"duplicate entry for one transaction": entry(txnID, "BRL", 9000, 8000, 3),
		"entry equation does not hold":        entry(uuid.NewString(), "BRL", 9000, 9000, 3),
		"entry for an unknown transaction":    entry(uuid.NewString(), "BRL", 9000, 8000, 3),
		"entry in the wrong currency":         entry(txnID, "USD", 9000, 8000, 3),
		"negative resulting balance":          entry(txnID, "BRL", 500, -500, 3),
		"transaction without a ledger entry":  fmt.Sprintf(`INSERT INTO wager_transactions(id,origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,status,result) VALUES(gen_random_uuid(),'EXTERNAL','%s','ghost','ghost','%s','%s','%s','r','g','BET',100,'BRL','PROCESSED','{}')`, string(p), strings.Repeat("a", 64), walletID, w.PlayerID),
		"movement without a ledger entry":     fmt.Sprintf(`UPDATE wallets SET balance_minor = 1, version = version + 1 WHERE id = '%s'`, walletID),
		"balance change without a version":    fmt.Sprintf(`UPDATE wallets SET balance_minor = 1 WHERE id = '%s'`, walletID),
		"wallet identity rewrite":             fmt.Sprintf(`UPDATE wallets SET player_id = gen_random_uuid() WHERE id = '%s'`, walletID),
		"business field of a transaction":     fmt.Sprintf(`UPDATE wager_transactions SET payload_hash = '%s' WHERE id = '%s'`, strings.Repeat("b", 64), txnID),
	}
	for name, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err == nil {
			t.Errorf("%s: the database accepted it", name)
		}
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 9000 || got.Version != 2 {
		t.Fatalf("wallet changed behind the application: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

// A divergence must be reported, never silently repaired. The test corrupts a wallet as the table
// owner (the runtime role cannot) and then restores it.
func TestReconciliationReportsDivergenceWithoutRepairingIt(t *testing.T) {
	url := os.Getenv("TEST_ADMIN_DATABASE_URL")
	if url == "" {
		url = "postgres://jungle:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	admin, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	mustProcess(t, s, p.command(w, "BET", "audit", 1000, ""))
	assertConsistent(t, s, w.ID)

	// Even the table owner is stopped by the movement trigger, so it is switched off for the edit.
	shift := func(delta int) error {
		tx, err := admin.Begin(ctx)
		if err != nil {
			return fmt.Errorf("owner connection (set TEST_ADMIN_DATABASE_URL): %w", err)
		}
		defer tx.Rollback(ctx)
		for _, statement := range []string{
			"ALTER TABLE wallets DISABLE TRIGGER wallet_movement_consistency",
			fmt.Sprintf("UPDATE wallets SET balance_minor = balance_minor + (%d) WHERE id='%s'", delta, w.ID),
			"ALTER TABLE wallets ENABLE TRIGGER wallet_movement_consistency",
		} {
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return tx.Commit(ctx)
	}
	if err := shift(1); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := shift(-1); err != nil {
			t.Errorf("restore: %v", err)
		}
	}()

	rec, err := s.Reconcile(ctx, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Consistent || rec.Difference.Amount != "0.01" || rec.CalculatedBalance.Amount != "90.00" || rec.StoredBalance.MinorUnits() != 9001 || rec.CheckedEntries != 2 {
		t.Fatalf("the divergence must be reported exactly: %+v", rec)
	}
	if again, _ := s.Reconcile(ctx, w.ID); again.Consistent {
		t.Fatal("reconciliation must not repair the wallet")
	}
}

func TestReconciliationIsConsistentWhileWritesAreInFlight(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 100000), newProvider()
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for g := 0; g < 4; g++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := process(ctx, s, p.command(w, "BET", fmt.Sprintf("flight-%d-%d", g, i), 10, ""), nil); err != nil {
					t.Errorf("writer %d: %v", g, err)
					return
				}
			}
		}()
	}
	for i := 0; i < 40; i++ {
		rec, err := s.Reconcile(ctx, w.ID)
		if err != nil || !rec.Consistent {
			close(stop)
			writers.Wait()
			t.Fatalf("reconciliation observed a torn state: %+v %v", rec, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(stop)
	writers.Wait()
	assertConsistent(t, s, w.ID)
}

func TestReferenceWorkerHonoursLeasesHeldByOtherWorkers(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	refund := mustProcess(t, s, p.command(w, "REFUND", "leased-refund", 500, "leased-bet"))
	mustProcess(t, s, p.command(w, "BET", "leased-bet", 500, ""))
	// Another (now dead) worker holds the lease: nobody else may process the row yet.
	if _, err := pool.Exec(ctx, "UPDATE wager_transactions SET lease_token=gen_random_uuid(), lease_until=clock_timestamp()+interval '1 hour', next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1", refund.TransactionID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.ProcessReferences(ctx, 100); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := s.Transaction(ctx, string(p), refund.TransactionID); got.Status != "PENDING_REFERENCE" {
		t.Fatalf("a row leased by another worker must be left alone: %+v", got)
	}
	// The lease expires (the worker died): the work is taken over.
	if _, err := pool.Exec(ctx, "UPDATE wager_transactions SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", refund.TransactionID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.ProcessReferences(ctx, 100); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.Transaction(ctx, string(p), refund.TransactionID); got.Status == "PROCESSED" {
			assertConsistent(t, s, w.ID)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("an expired lease must be taken over")
}
