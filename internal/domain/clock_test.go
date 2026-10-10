package domain

import (
	"testing"
	"time"
)

// Regression: a wallet whose updated_at ended up before its created_at (the Docker VM clock stepped back
// 17 seconds between the creation and the next bet) was rejected with INVALID_WALLET on every later
// operation, so nothing could ever repair it. Timestamp order is audit data, not a financial invariant.
func TestRestoreWalletToleratesClockSkewBetweenItsTimestamps(t *testing.T) {
	created := time.Now().UTC()
	w, err := RestoreWallet(WalletSnapshot{ID: testWallet, PlayerID: testPlayer, Balance: brl(t, 100), Version: 3, CreatedAt: created, UpdatedAt: created.Add(-17 * time.Second)})
	if err != nil {
		t.Fatalf("a skewed timestamp must not invalidate the wallet: %v", err)
	}
	if w.UpdatedAt().Before(w.CreatedAt()) {
		t.Fatalf("the restored wallet must be internally consistent: created %s, updated %s", w.CreatedAt(), w.UpdatedAt())
	}
	if err := w.Debit(brl(t, 10)); err != nil {
		t.Fatalf("the wallet must stay fully operational: %v", err)
	}
	if w.Balance().MinorUnits() != 90 || w.Version() != 4 {
		t.Fatalf("unexpected state after the debit: %+v", w.Snapshot())
	}
}

func TestRestoreWalletStillEnforcesTheFinancialInvariants(t *testing.T) {
	now := time.Now().UTC()
	valid := WalletSnapshot{ID: testWallet, PlayerID: testPlayer, Balance: brl(t, 100), Version: 1, CreatedAt: now, UpdatedAt: now}
	cases := map[string]func(*WalletSnapshot){
		"negative balance":    func(s *WalletSnapshot) { s.Balance = brl(t, -1) },
		"version zero":        func(s *WalletSnapshot) { s.Version = 0 },
		"missing created_at":  func(s *WalletSnapshot) { s.CreatedAt = time.Time{} },
		"invalid wallet id":   func(s *WalletSnapshot) { s.ID = "not-a-uuid" },
		"invalid player id":   func(s *WalletSnapshot) { s.PlayerID = "not-a-uuid" },
		"zero-value currency": func(s *WalletSnapshot) { s.Balance = Money{} },
	}
	for name, mutate := range cases {
		snapshot := valid
		mutate(&snapshot)
		if _, err := RestoreWallet(snapshot); err == nil {
			t.Errorf("%s must still be rejected", name)
		}
	}
	if _, err := RestoreWallet(valid); err != nil {
		t.Fatalf("the control snapshot must be valid: %v", err)
	}
}

func TestRestoreTransactionToleratesClockSkewBetweenItsTimestamps(t *testing.T) {
	now := time.Now().UTC()
	amount := brl(t, 500)
	tx, err := RestoreWagerTransaction(TransactionSnapshot{ID: testWallet, Origin: "INTERNAL", WalletID: testWallet, PlayerID: testPlayer, Kind: KindOpening,
		Money: amount, Status: StatusProcessed, Balance: &amount, WalletVersion: 1, CreatedAt: now, UpdatedAt: now.Add(-time.Minute)})
	if err != nil {
		t.Fatalf("a skewed timestamp must not invalidate the transaction: %v", err)
	}
	if tx == nil {
		t.Fatal("expected a transaction")
	}
}
