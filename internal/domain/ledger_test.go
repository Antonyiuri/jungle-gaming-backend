package domain

import (
	"math"
	"strings"
	"testing"
	"time"
)

func ledgerEntry(t *testing.T, direction string, before, amount, after int64) LedgerSnapshot {
	t.Helper()
	return LedgerSnapshot{ID: "7d1c3a52-0f55-4e5e-9a5d-3a7a8f9d2b10", WalletID: testWallet, TransactionID: testTransaction, Direction: direction,
		Money: brl(t, amount), BalanceBefore: brl(t, before), BalanceAfter: brl(t, after), CreatedAt: epoch}
}

// The ledger equation: balanceAfter = balanceBefore + amount for a credit and balanceBefore - amount for a debit.
func TestLedgerEntryEnforcesTheBalanceEquation(t *testing.T) {
	valid := []LedgerSnapshot{
		ledgerEntry(t, DirectionCredit, 0, 5000, 5000),
		ledgerEntry(t, DirectionCredit, 1000, 250, 1250),
		ledgerEntry(t, DirectionDebit, 1000, 250, 750),
		ledgerEntry(t, DirectionDebit, 250, 250, 0),
	}
	for _, snapshot := range valid {
		entry, err := NewWalletLedgerEntry(snapshot)
		if err != nil {
			t.Errorf("%+v: %v", snapshot, err)
			continue
		}
		if err := entry.Validate(); err != nil {
			t.Errorf("a built entry must stay valid: %v", err)
		}
	}
	invalid := map[string]LedgerSnapshot{
		"credit that does not add up":         ledgerEntry(t, DirectionCredit, 1000, 250, 1251),
		"credit applied as a debit":           ledgerEntry(t, DirectionDebit, 1000, 250, 1250),
		"debit that does not subtract":        ledgerEntry(t, DirectionDebit, 1000, 250, 749),
		"debit applied as a credit":           ledgerEntry(t, DirectionCredit, 1000, 250, 750),
		"debit below zero":                    ledgerEntry(t, DirectionDebit, 100, 250, -150),
		"negative balance before":             ledgerEntry(t, DirectionCredit, -100, 250, 150),
		"zero amount":                         ledgerEntry(t, DirectionCredit, 1000, 0, 1000),
		"negative amount":                     ledgerEntry(t, DirectionCredit, 1000, -250, 750),
		"unknown direction":                   ledgerEntry(t, "SIDEWAYS", 1000, 250, 1250),
		"empty direction":                     ledgerEntry(t, "", 1000, 250, 1250),
		"credit overflowing the range":        ledgerEntry(t, DirectionCredit, math.MaxInt64, 1, 0),
		"balance lost in the equation (same)": ledgerEntry(t, DirectionCredit, 1000, 250, 1000),
	}
	for name, snapshot := range invalid {
		if _, err := NewWalletLedgerEntry(snapshot); err == nil {
			t.Errorf("%s: must be refused", name)
		} else if code := Code(err); code != "INVALID_LEDGER_ENTRY" && code != "MONEY_OVERFLOW" {
			t.Errorf("%s: unexpected code %q (%v)", name, code, err)
		}
	}
}

func TestLedgerEntryRejectsMixedCurrenciesAndBadIdentity(t *testing.T) {
	mixed := ledgerEntry(t, DirectionCredit, 1000, 250, 1250)
	usd, _ := NewMoney(250, "USD")
	mixed.Money = usd
	if _, err := NewWalletLedgerEntry(mixed); Code(err) != "CURRENCY_MISMATCH" {
		t.Errorf("mixed currencies must be refused: %v", err)
	}
	for name, mutate := range map[string]func(*LedgerSnapshot){
		"bad entry id":       func(s *LedgerSnapshot) { s.ID = "x" },
		"bad wallet id":      func(s *LedgerSnapshot) { s.WalletID = "" },
		"bad transaction id": func(s *LedgerSnapshot) { s.TransactionID = "0" },
		"no timestamp":       func(s *LedgerSnapshot) { s.CreatedAt = time.Time{} },
		"no currency":        func(s *LedgerSnapshot) { s.Money = Money{} },
	} {
		s := ledgerEntry(t, DirectionCredit, 1000, 250, 1250)
		mutate(&s)
		if _, err := NewWalletLedgerEntry(s); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
}

func TestLedgerEntryNormalizesIdentityAndIsImmutable(t *testing.T) {
	s := ledgerEntry(t, DirectionCredit, 1000, 250, 1250)
	s.ID = strings.ToUpper(s.ID)
	s.WalletID = strings.ToUpper(s.WalletID)
	entry, err := NewWalletLedgerEntry(s)
	if err != nil {
		t.Fatal(err)
	}
	got := entry.Snapshot()
	if got.ID != strings.ToLower(s.ID) || got.WalletID != strings.ToLower(s.WalletID) || got.CreatedAt.Location().String() != "UTC" {
		t.Fatalf("identity must be canonical: %+v", got)
	}
	restored, err := RestoreWalletLedgerEntry(got)
	if err != nil || restored.Snapshot() != got {
		t.Fatalf("restoring a snapshot must give the same entry: %v", err)
	}
	// Entries have no mutators: changing a copy of the snapshot cannot change the stored entry.
	got.BalanceAfter = brl(t, 1)
	if entry.Snapshot().BalanceAfter.MinorUnits() != 1250 {
		t.Fatal("the entry changed through a snapshot")
	}
	if err := (WalletLedgerEntry{}).Validate(); err == nil {
		t.Fatal("the zero entry is not a valid ledger entry")
	}
}
