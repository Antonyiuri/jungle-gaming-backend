//go:build integration

package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/store"
)

// These tests need the migrated PostgreSQL database described in README.md and connect with the
// least-privilege runtime role, so they also exercise the grants and triggers.
func connect(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://jungle_app:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("test database unavailable: %v", err)
	}
	return store.New(pool), pool
}

func money(t *testing.T, minor int64) domain.Money {
	t.Helper()
	m, err := domain.NewMoney(minor, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func wallet(t *testing.T, s *store.Store, minor int64) domain.WalletSnapshot {
	t.Helper()
	w, err := s.CreateWallet(context.Background(), uuid.NewString(), money(t, minor))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

type provider string

func newProvider() provider { return provider("it-" + uuid.NewString()[:8]) }

func (p provider) command(w domain.WalletSnapshot, kind, external string, minor int64, reference string) domain.Command {
	return domain.Command{ProviderID: string(p), ExternalTransactionID: external, IdempotencyKey: string(p) + ":" + external, PlayerID: w.PlayerID, WalletID: w.ID,
		RoundID: "round-1", GameID: "game-1", Kind: kind, Money: moneyOrPanic(minor), ReferenceExternalTransactionID: reference}
}

func moneyOrPanic(minor int64) domain.Money {
	m, err := domain.NewMoney(minor, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}

// process retries transient failures with the same identity, as the documented client contract requires.
func process(ctx context.Context, s *store.Store, c domain.Command, d *store.Delivery) (domain.Result, error) {
	var last error
	for i := 0; i < 20; i++ {
		r, err := s.Process(ctx, c, d)
		if err == nil || domain.Code(err) != "TEMPORARILY_UNAVAILABLE" {
			return r, err
		}
		last = err
		time.Sleep(20 * time.Millisecond)
	}
	return domain.Result{}, last
}

func mustProcess(t *testing.T, s *store.Store, c domain.Command) domain.Result {
	t.Helper()
	r, err := process(context.Background(), s, c, nil)
	if err != nil {
		t.Fatalf("process %s: %v", c.ExternalTransactionID, err)
	}
	return r
}

func assertConsistent(t *testing.T, s *store.Store, walletID string) {
	t.Helper()
	rec, err := s.Reconcile(context.Background(), walletID)
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Consistent {
		t.Fatalf("wallet %s ledger and balance diverge: %+v", walletID, rec)
	}
}

func TestCreateWalletWritesOpeningLedgerAndRejectsDuplicates(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()
	player := uuid.NewString()
	w, err := s.CreateWallet(ctx, player, money(t, 5000))
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance.MinorUnits() != 5000 || w.Version != 1 {
		t.Fatalf("unexpected wallet %+v", w)
	}
	page, err := s.Ledger(ctx, w.ID, "", 10)
	if err != nil || len(page.Entries) != 1 || page.Entries[0].Direction != "CREDIT" || page.Entries[0].BalanceBefore.MinorUnits() != 0 || page.Entries[0].BalanceAfter.MinorUnits() != 5000 {
		t.Fatalf("opening ledger entry missing: %+v %v", page, err)
	}
	if _, err := s.CreateWallet(ctx, player, money(t, 5000)); domain.Code(err) != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("a second wallet for the same player and currency must conflict: %v", err)
	}
	empty := wallet(t, s, 0)
	if page, _ := s.Ledger(ctx, empty.ID, "", 10); len(page.Entries) != 0 {
		t.Fatalf("a zero opening balance creates no ledger entry: %+v", page)
	}
	assertConsistent(t, s, w.ID)
	assertConsistent(t, s, empty.ID)
}

func TestIdempotentReplayAndConflicts(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	bet := p.command(w, "BET", "bet-1", 2500, "")
	first := mustProcess(t, s, bet)
	if first.Status != "PROCESSED" || first.IdempotentReplay || first.Balance.MinorUnits() != 7500 {
		t.Fatalf("first result: %+v", first)
	}
	replay := mustProcess(t, s, bet)
	if !replay.IdempotentReplay || replay.TransactionID != first.TransactionID || replay.Balance.MinorUnits() != 7500 {
		t.Fatalf("replay must return the stored result: %+v", replay)
	}
	changed := bet
	changed.Money = money(t, 2600)
	if _, err := s.Process(context.Background(), changed, nil); domain.Code(err) != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same key with another payload: %v", err)
	}
	rebound := bet
	rebound.IdempotencyKey = "another-key"
	if _, err := s.Process(context.Background(), rebound, nil); domain.Code(err) != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("external id bound to another key: %v", err)
	}
	if got, _ := s.Wallet(context.Background(), w.ID); got.Balance.MinorUnits() != 7500 || got.Version != 2 {
		t.Fatalf("the balance must change exactly once: %+v", got)
	}
	other := newProvider()
	again := other.command(w, "BET", "bet-1", 2500, "")
	if r := mustProcess(t, s, again); r.IdempotentReplay || r.Balance.MinorUnits() != 5000 {
		t.Fatalf("idempotency is scoped per provider: %+v", r)
	}
	assertConsistent(t, s, w.ID)
}

func TestRejectionsAreDurableAndAuditable(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 1000), newProvider()
	bet := p.command(w, "BET", "too-big", 1001, "")
	r := mustProcess(t, s, bet)
	if r.Status != "REJECTED" || r.FailureCode != "INSUFFICIENT_FUNDS" || r.Balance.MinorUnits() != 1000 {
		t.Fatalf("rejection: %+v", r)
	}
	stored, err := s.Transaction(context.Background(), string(p), r.TransactionID)
	if err != nil || stored.Status != "REJECTED" {
		t.Fatalf("rejection must be queryable: %+v %v", stored, err)
	}
	if replay := mustProcess(t, s, bet); !replay.IdempotentReplay || replay.Status != "REJECTED" {
		t.Fatalf("a rejected request replays as rejected: %+v", replay)
	}
	if got, _ := s.Wallet(context.Background(), w.ID); got.Version != 1 {
		t.Fatalf("rejections do not bump the wallet version: %+v", got)
	}
	if _, err := s.Transaction(context.Background(), string(newProvider()), r.TransactionID); domain.Code(err) != "NOT_FOUND" {
		t.Fatalf("another provider must not see the transaction: %v", err)
	}
	if got, err := s.ExternalTransaction(context.Background(), string(p), "too-big"); err != nil || got.TransactionID != r.TransactionID {
		t.Fatalf("lookup by external id: %+v %v", got, err)
	}
}

func TestConcurrentBetsNeverOverdraw(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 1000), newProvider()
	const workers = 40
	results := make([]domain.Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = process(context.Background(), s, p.command(w, "BET", fmt.Sprintf("bet-%d", i), 100, ""), nil)
		}()
	}
	close(start)
	wg.Wait()
	processed, rejected := 0, 0
	for i, r := range results {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		switch r.Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			if r.FailureCode != "INSUFFICIENT_FUNDS" {
				t.Fatalf("unexpected rejection %+v", r)
			}
			rejected++
		default:
			t.Fatalf("unexpected status %+v", r)
		}
	}
	if processed != 10 || rejected != workers-10 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	got, _ := s.Wallet(context.Background(), w.ID)
	if got.Balance.MinorUnits() != 0 || got.Version != 11 {
		t.Fatalf("final wallet %+v", got)
	}
	page, _ := s.Ledger(context.Background(), w.ID, "", 100)
	if len(page.Entries) != 11 {
		t.Fatalf("expected opening plus ten debits, got %d", len(page.Entries))
	}
	for _, entry := range page.Entries {
		if entry.BalanceAfter.MinorUnits() < 0 {
			t.Fatalf("negative balance in ledger: %+v", entry)
		}
	}
	assertConsistent(t, s, w.ID)
}

func TestConcurrentRetriesOfOneKeyApplyOnce(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	bet := p.command(w, "BET", "same", 1000, "")
	const workers = 30
	results := make([]domain.Result, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = process(context.Background(), s, bet, nil)
		}()
	}
	close(start)
	wg.Wait()
	originals := 0
	for i, r := range results {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if r.TransactionID != results[0].TransactionID || r.Status != "PROCESSED" {
			t.Fatalf("all retries must resolve to one transaction: %+v vs %+v", r, results[0])
		}
		if !r.IdempotentReplay {
			originals++
		}
	}
	if originals != 1 {
		t.Fatalf("exactly one call may be the original, got %d", originals)
	}
	if got, _ := s.Wallet(context.Background(), w.ID); got.Balance.MinorUnits() != 9000 || got.Version != 2 {
		t.Fatalf("applied more than once: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

func TestReversalsWaitForTheirReferenceAndApplyOnce(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	refund := mustProcess(t, s, p.command(w, "REFUND", "refund-1", 3000, "bet-1"))
	if refund.Status != "PENDING_REFERENCE" || refund.Balance != nil {
		t.Fatalf("a reversal that arrives first must wait: %+v", refund)
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 10000 {
		t.Fatalf("a pending reversal must not move money: %+v", got)
	}
	if r := mustProcess(t, s, p.command(w, "BET", "bet-1", 3000, "")); r.Status != "PROCESSED" {
		t.Fatalf("bet: %+v", r)
	}
	deadline := time.Now().Add(15 * time.Second)
	var final domain.Result
	for {
		if err := s.ProcessReferences(ctx, 20); err != nil {
			t.Fatal(err)
		}
		var err error
		if final, err = s.Transaction(ctx, string(p), refund.TransactionID); err != nil {
			t.Fatal(err)
		}
		if final.Status != "PENDING_REFERENCE" || time.Now().After(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if final.Status != "PROCESSED" || final.Balance.MinorUnits() != 10000 {
		t.Fatalf("pending reversal must complete once its bet exists: %+v", final)
	}
	second := mustProcess(t, s, p.command(w, "ROLLBACK", "rollback-1", 3000, "bet-1"))
	if second.Status != "REJECTED" || second.FailureCode != "REFERENCE_ALREADY_REVERSED" {
		t.Fatalf("a bet is reversed at most once: %+v", second)
	}
	if got, _ := s.Wallet(ctx, w.ID); got.Balance.MinorUnits() != 10000 {
		t.Fatalf("final balance %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

func TestConcurrentReversalsOfOneBetCreditOnce(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	mustProcess(t, s, p.command(w, "BET", "bet-1", 4000, ""))
	const workers = 12
	results := make([]domain.Result, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			kind := "REFUND"
			if i%2 == 1 {
				kind = "ROLLBACK"
			}
			r, err := process(context.Background(), s, p.command(w, kind, fmt.Sprintf("rev-%d", i), 4000, "bet-1"), nil)
			if err != nil {
				t.Errorf("worker %d: %v", i, err)
			}
			results[i] = r
		}()
	}
	close(start)
	wg.Wait()
	applied := 0
	for _, r := range results {
		if r.Status == "PROCESSED" {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("exactly one reversal may apply, got %d", applied)
	}
	if got, _ := s.Wallet(context.Background(), w.ID); got.Balance.MinorUnits() != 10000 {
		t.Fatalf("the stake must be returned exactly once: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

func TestInboxDeduplicatesDeliveries(t *testing.T) {
	s, _ := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	bet := p.command(w, "BET", "inbox-bet", 1000, "")
	delivery := func(message, hash, sender string) *store.Delivery {
		return &store.Delivery{MessageID: message, ConsumerName: "wager-processor", Hash: hash, SenderID: sender}
	}
	message := uuid.NewString()
	first, err := process(context.Background(), s, bet, delivery(message, "hash-1", "client-a"))
	if err != nil || first.Status != "PROCESSED" || first.IdempotentReplay {
		t.Fatalf("first delivery: %+v %v", first, err)
	}
	again, err := process(context.Background(), s, bet, delivery(message, "hash-1", "client-a"))
	if err != nil || !again.IdempotentReplay || again.TransactionID != first.TransactionID {
		t.Fatalf("redelivery must be recognised: %+v %v", again, err)
	}
	if _, err := s.Process(context.Background(), bet, delivery(message, "hash-2", "client-a")); domain.Code(err) != "INBOX_CONFLICT" {
		t.Fatalf("same message with another body: %v", err)
	}
	if _, err := s.Process(context.Background(), bet, delivery(message, "hash-1", "client-b")); domain.Code(err) != "INBOX_CONFLICT" {
		t.Fatalf("same message from another sender: %v", err)
	}
	// A different message carrying the same financial identity resolves to the stored result.
	viaHTTP, err := process(context.Background(), s, bet, nil)
	if err != nil || !viaHTTP.IdempotentReplay || viaHTTP.TransactionID != first.TransactionID {
		t.Fatalf("HTTP and SQS must share idempotency: %+v %v", viaHTTP, err)
	}
	other, err := process(context.Background(), s, bet, delivery(uuid.NewString(), "hash-3", "client-a"))
	if err != nil || !other.IdempotentReplay || other.TransactionID != first.TransactionID {
		t.Fatalf("a new message with the same identity is a replay: %+v %v", other, err)
	}
	if got, _ := s.Wallet(context.Background(), w.ID); got.Balance.MinorUnits() != 9000 {
		t.Fatalf("applied more than once: %+v", got)
	}
	if _, err := s.Process(context.Background(), bet, &store.Delivery{MessageID: "x"}); domain.Code(err) != "INVALID_MESSAGE" {
		t.Fatalf("incomplete delivery identity must be refused: %v", err)
	}
	assertConsistent(t, s, w.ID)
}

func TestInboxMessageIDCannotCarryAnotherProvidersTransaction(t *testing.T) {
	s, _ := connect(t)
	w := wallet(t, s, 10000)
	a, b := newProvider(), newProvider()
	message := uuid.NewString()
	d := &store.Delivery{MessageID: message, ConsumerName: "wager-processor", Hash: "h", SenderID: "client"}
	if _, err := process(context.Background(), s, a.command(w, "BET", "one", 100, ""), d); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Process(context.Background(), b.command(w, "BET", "two", 100, ""), d); domain.Code(err) != "INBOX_CONFLICT" {
		t.Fatalf("a message id is bound to its first provider: %v", err)
	}
}

func TestRuntimeRoleCannotRewriteHistory(t *testing.T) {
	s, pool := connect(t)
	w, p := wallet(t, s, 10000), newProvider()
	r := mustProcess(t, s, p.command(w, "BET", "protected", 1000, ""))
	statements := []string{
		fmt.Sprintf("UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = '%s'", w.ID),
		fmt.Sprintf("DELETE FROM wallet_ledger_entries WHERE wallet_id = '%s'", w.ID),
		"TRUNCATE wallet_ledger_entries",
		fmt.Sprintf("UPDATE wallets SET balance_minor = -1 WHERE id = '%s'", w.ID),
		fmt.Sprintf("UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = '%s'", w.ID),
		fmt.Sprintf("UPDATE wager_transactions SET status = 'PENDING' WHERE id = '%s'", r.TransactionID),
		fmt.Sprintf("UPDATE wager_transactions SET amount_minor = 1 WHERE id = '%s'", r.TransactionID),
		fmt.Sprintf("DELETE FROM wager_transactions WHERE id = '%s'", r.TransactionID),
		fmt.Sprintf("UPDATE outbox_events SET payload = '{}' WHERE transaction_id = '%s'", r.TransactionID),
		fmt.Sprintf("DELETE FROM outbox_events WHERE transaction_id = '%s'", r.TransactionID),
	}
	for _, statement := range statements {
		_, err := pool.Exec(context.Background(), statement)
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) {
			t.Errorf("must be refused by the database: %s (err=%v)", statement, err)
		}
	}
	got, _ := s.Wallet(context.Background(), w.ID)
	if got.Balance.MinorUnits() != 9000 {
		t.Fatalf("balance changed behind the application: %+v", got)
	}
	assertConsistent(t, s, w.ID)
}

func TestLedgerPaginationUsesAStableSnapshot(t *testing.T) {
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 100000), newProvider()
	for i := 0; i < 7; i++ {
		mustProcess(t, s, p.command(w, "BET", fmt.Sprintf("page-%d", i), 100, ""))
	}
	first, err := s.Ledger(ctx, w.ID, "", 3)
	if err != nil || len(first.Entries) != 3 || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	mustProcess(t, s, p.command(w, "BET", "late-1", 100, ""))
	mustProcess(t, s, p.command(w, "BET", "late-2", 100, ""))
	seen := map[int64]bool{}
	for _, e := range first.Entries {
		seen[e.WalletVersion] = true
	}
	cursor := first.NextCursor
	for cursor != "" {
		page, err := s.Ledger(ctx, w.ID, cursor, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			if seen[e.WalletVersion] {
				t.Fatalf("duplicate entry at version %d", e.WalletVersion)
			}
			seen[e.WalletVersion] = true
		}
		cursor = page.NextCursor
	}
	if len(seen) != 8 {
		t.Fatalf("entries committed after the first page must not appear in its cursor chain, got %d", len(seen))
	}
	if all, _ := s.Ledger(ctx, w.ID, "", 100); len(all.Entries) != 10 {
		t.Fatalf("a fresh listing sees everything: %d", len(all.Entries))
	}
	if _, err := s.Ledger(ctx, w.ID, "garbage", 3); domain.Code(err) != "INVALID_CURSOR" {
		t.Fatalf("malformed cursor: %v", err)
	}
	other := wallet(t, s, 0)
	if _, err := s.Ledger(ctx, other.ID, first.NextCursor, 3); domain.Code(err) != "INVALID_CURSOR" {
		t.Fatalf("a cursor is bound to its wallet: %v", err)
	}
	if _, err := s.Ledger(ctx, w.ID, "", 101); domain.Code(err) != "INVALID_REQUEST" {
		t.Fatalf("limit bound: %v", err)
	}
}

func TestOutboxClaimsAreExclusiveAndRecoverable(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	p := newProvider()
	for i := 0; i < 20; i++ {
		w := wallet(t, s, 1000)
		mustProcess(t, s, p.command(w, "BET", fmt.Sprintf("ob-%d", i), 100, ""))
	}
	var wg sync.WaitGroup
	claims := make([][]store.OutboxRecord, 3)
	for i := range claims {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if claims[i], err = s.ClaimOutbox(ctx, 100, time.Minute); err != nil {
				t.Errorf("claim %d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	owner := map[string]int{}
	total := 0
	for i, batch := range claims {
		for _, r := range batch {
			if prev, taken := owner[r.ID]; taken {
				t.Fatalf("event %s claimed by %d and %d", r.ID, prev, i)
			}
			owner[r.ID] = i
			total++
			if r.LeaseToken == "" || r.Attempts < 1 {
				t.Fatalf("claim without lease: %+v", r)
			}
		}
	}
	if total == 0 {
		t.Fatal("expected events to claim")
	}
	sample := claims[0]
	if len(sample) == 0 {
		for _, batch := range claims {
			if len(batch) > 0 {
				sample = batch
				break
			}
		}
	}
	record := sample[0]
	published := func(id string) bool {
		var at *time.Time
		if err := pool.QueryRow(ctx, "SELECT published_at FROM outbox_events WHERE id=$1", id).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at != nil
	}
	if err := s.Published(ctx, record.ID, uuid.NewString()); err != nil || published(record.ID) {
		t.Fatalf("a stale lease token must not mark the event (err=%v)", err)
	}
	if err := s.Published(ctx, record.ID, record.LeaseToken); err != nil || !published(record.ID) {
		t.Fatalf("the lease holder marks the event (err=%v)", err)
	}
	// Release everything else with backoff, then make one lease expire and verify it is reclaimed.
	var retried store.OutboxRecord
	for _, r := range sample[1:] {
		if err := s.RetryOutbox(ctx, r.ID, r.LeaseToken); err != nil {
			t.Fatal(err)
		}
		retried = r
	}
	if retried.ID != "" {
		var next time.Time
		var attempts int
		if err := pool.QueryRow(ctx, "SELECT next_attempt_at, attempts FROM outbox_events WHERE id=$1", retried.ID).Scan(&next, &attempts); err != nil {
			t.Fatal(err)
		}
		if !next.After(time.Now()) || attempts != retried.Attempts {
			t.Fatalf("retry must schedule a future attempt (next=%v attempts=%d)", next, attempts)
		}
	}
}

func TestExpiredOutboxLeaseIsReclaimed(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 1000), newProvider()
	r := mustProcess(t, s, p.command(w, "BET", "lease", 100, ""))
	var id string
	if err := pool.QueryRow(ctx, "SELECT id::text FROM outbox_events WHERE transaction_id=$1 AND event_type LIKE '%Processed%' LIMIT 1", r.TransactionID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	var first, second *store.OutboxRecord
	for first == nil && time.Now().Before(deadline) {
		batch, err := s.ClaimOutbox(ctx, 100, 200*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range batch {
			if rec.ID == id {
				copy := rec
				first = &copy
			}
		}
	}
	if first == nil {
		t.Fatal("event was never claimed")
	}
	time.Sleep(400 * time.Millisecond)
	for second == nil && time.Now().Before(deadline) {
		batch, err := s.ClaimOutbox(ctx, 100, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, rec := range batch {
			if rec.ID == id {
				copy := rec
				second = &copy
			}
		}
	}
	if second == nil || second.LeaseToken == first.LeaseToken || second.Attempts != first.Attempts+1 {
		t.Fatalf("an expired lease must be taken over with a new token: first=%+v second=%+v", first, second)
	}
	if err := s.Published(ctx, id, first.LeaseToken); err != nil {
		t.Fatal(err)
	}
	var at *time.Time
	_ = pool.QueryRow(ctx, "SELECT published_at FROM outbox_events WHERE id=$1", id).Scan(&at)
	if at != nil {
		t.Fatal("the previous holder must not complete the event after losing its lease")
	}
	if err := s.Published(ctx, id, second.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestEveryProcessedTransactionEmitsItsEvents(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 1000), newProvider()
	bet := mustProcess(t, s, p.command(w, "BET", "ev-1", 100, ""))
	loss := mustProcess(t, s, p.command(w, "LOSS", "ev-2", 0, ""))
	rejected := mustProcess(t, s, p.command(w, "BET", "ev-3", 999999, ""))
	pending := mustProcess(t, s, p.command(w, "REFUND", "ev-4", 100, "missing"))
	counts := map[string]int{}
	for name, id := range map[string]string{"bet": bet.TransactionID, "loss": loss.TransactionID, "rejected": rejected.TransactionID, "pending": pending.TransactionID} {
		var n int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1", id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[name] = n
	}
	want := map[string]int{"bet": 2, "loss": 1, "rejected": 1, "pending": 1}
	for name, n := range want {
		if counts[name] != n {
			t.Errorf("%s emitted %d events, want %d", name, counts[name], n)
		}
	}
	replayed := mustProcess(t, s, p.command(w, "BET", "ev-1", 100, ""))
	var n int
	_ = pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1", replayed.TransactionID).Scan(&n)
	if n != 2 {
		t.Errorf("a replay must not emit events again, got %d", n)
	}
}

func TestPendingReferenceExpiresIntoADurableRejection(t *testing.T) {
	s, pool := connect(t)
	s.SetReferenceTTL(time.Second)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	refund := mustProcess(t, s, p.command(w, "REFUND", "orphan-refund", 500, "bet-that-never-arrives"))
	if refund.Status != "PENDING_REFERENCE" {
		t.Fatalf("expected a pending reversal, got %+v", refund)
	}
	deadline := time.Now().Add(30 * time.Second)
	var final domain.Result
	for {
		if err := s.ProcessReferences(ctx, 20); err != nil {
			t.Fatal(err)
		}
		var err error
		if final, err = s.Transaction(ctx, string(p), refund.TransactionID); err != nil {
			t.Fatal(err)
		}
		if final.Status != "PENDING_REFERENCE" || time.Now().After(deadline) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if final.Status != "REJECTED" || final.FailureCode != "REFERENCE_NOT_FOUND" || final.Balance == nil || final.Balance.MinorUnits() != 10000 {
		t.Fatalf("an expired reference must become a final rejection without moving money: %+v", final)
	}
	var rejectedEvents int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1 AND event_type='WagerTransactionRejected'", refund.TransactionID).Scan(&rejectedEvents); err != nil || rejectedEvents != 1 {
		t.Fatalf("the rejection must be announced exactly once: %d (%v)", rejectedEvents, err)
	}
	// Even if the bet shows up later, the terminal result is never reopened.
	mustProcess(t, s, p.command(w, "BET", "bet-that-never-arrives", 500, ""))
	if err := s.ProcessReferences(ctx, 20); err != nil {
		t.Fatal(err)
	}
	if again, _ := s.Transaction(ctx, string(p), refund.TransactionID); again.Status != "REJECTED" {
		t.Fatalf("terminal transactions are immutable: %+v", again)
	}
	assertConsistent(t, s, w.ID)
}
