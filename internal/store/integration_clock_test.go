//go:build integration

package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func adminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_ADMIN_DATABASE_URL")
	if url == "" {
		url = "postgres://jungle:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("owner connection (set TEST_ADMIN_DATABASE_URL): %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// shiftCreatedAt rewrites a wallet's creation time as the table owner. The movement trigger protects the
// wallet's identity even from the owner, so it is switched off for the edit.
func shiftCreatedAt(t *testing.T, admin *pgxpool.Pool, walletID, interval string) {
	t.Helper()
	ctx := context.Background()
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range []string{
		"ALTER TABLE wallets DISABLE TRIGGER wallet_movement_consistency",
		fmt.Sprintf("UPDATE wallets SET created_at = created_at + interval '%s' WHERE id = '%s'", interval, walletID),
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

// Regression for the production failure seen when the Docker VM clock stepped back 17 s right after a
// wallet was created: updated_at (database clock, at the next movement) landed before created_at, the
// wallet could no longer be restored, and every later operation failed with INVALID_WALLET forever.
func TestWalletStaysUsableWhenItsCreationTimeIsAheadOfTheClock(t *testing.T) {
	admin := adminPool(t)
	s, _ := connect(t)
	ctx := context.Background()
	w, p := wallet(t, s, 10000), newProvider()
	shiftCreatedAt(t, admin, w.ID, "1 hour")
	defer shiftCreatedAt(t, admin, w.ID, "-1 hour")

	bet := mustProcess(t, s, p.command(w, "BET", "skew-bet", 1000, ""))
	win := mustProcess(t, s, p.command(w, "WIN", "skew-win", 500, ""))
	if bet.Status != "PROCESSED" || bet.Balance.MinorUnits() != 9000 || win.Status != "PROCESSED" || win.Balance.MinorUnits() != 9500 {
		t.Fatalf("operations on a skewed wallet must keep working: bet=%+v win=%+v", bet, win)
	}
	assertConsistent(t, s, w.ID)
	got, err := s.Wallet(ctx, w.ID)
	if err != nil || got.Balance.MinorUnits() != 9500 || got.Version != 3 {
		t.Fatalf("final wallet: %+v %v", got, err)
	}
}

func TestCreateWalletTimestampsComeFromTheDatabaseClock(t *testing.T) {
	s, pool := connect(t)
	ctx := context.Background()
	before := time.Now().Add(-time.Minute)
	w := wallet(t, s, 1000)
	var created, updated time.Time
	if err := pool.QueryRow(ctx, "SELECT created_at, updated_at FROM wallets WHERE id=$1", w.ID).Scan(&created, &updated); err != nil {
		t.Fatal(err)
	}
	if !w.CreatedAt.Equal(created) || !w.UpdatedAt.Equal(updated) {
		t.Fatalf("the returned snapshot must carry the stored timestamps: returned %s/%s, stored %s/%s", w.CreatedAt, w.UpdatedAt, created, updated)
	}
	if created.Before(before) || created.After(time.Now().Add(time.Minute)) {
		t.Fatalf("implausible creation time %s", created)
	}
}
