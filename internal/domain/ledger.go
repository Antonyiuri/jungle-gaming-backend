package domain

import (
	"strings"
	"time"
)

type LedgerSnapshot struct {
	ID, WalletID, TransactionID, Direction string
	Money, BalanceBefore, BalanceAfter     Money
	CreatedAt                              time.Time
}

// WalletLedgerEntry has no mutation methods; corrections need a new entry.
type WalletLedgerEntry struct{ state LedgerSnapshot }

func NewWalletLedgerEntry(snapshot LedgerSnapshot) (WalletLedgerEntry, error) {
	if !validUUID(snapshot.ID) || !validUUID(snapshot.WalletID) || !validUUID(snapshot.TransactionID) || snapshot.CreatedAt.IsZero() {
		return WalletLedgerEntry{}, fail("INVALID_LEDGER_ENTRY", "ledger identity or timestamp is invalid")
	}
	if err := snapshot.Money.Validate(); err != nil {
		return WalletLedgerEntry{}, err
	}
	if snapshot.Money.MinorUnits() <= 0 || snapshot.BalanceBefore.MinorUnits() < 0 || snapshot.BalanceAfter.MinorUnits() < 0 {
		return WalletLedgerEntry{}, fail("INVALID_LEDGER_ENTRY", "ledger amounts violate an invariant")
	}
	var calculated Money
	var err error
	switch snapshot.Direction {
	case DirectionCredit:
		calculated, err = snapshot.BalanceBefore.Add(snapshot.Money)
	case DirectionDebit:
		calculated, err = snapshot.BalanceBefore.Subtract(snapshot.Money)
	default:
		return WalletLedgerEntry{}, fail("INVALID_LEDGER_ENTRY", "ledger direction is invalid")
	}
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	comparison, err := calculated.Compare(snapshot.BalanceAfter)
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if comparison != 0 {
		return WalletLedgerEntry{}, fail("INVALID_LEDGER_ENTRY", "ledger balance equation does not hold")
	}
	snapshot.ID = strings.ToLower(snapshot.ID)
	snapshot.WalletID = strings.ToLower(snapshot.WalletID)
	snapshot.TransactionID = strings.ToLower(snapshot.TransactionID)
	snapshot.CreatedAt = snapshot.CreatedAt.UTC()
	return WalletLedgerEntry{state: snapshot}, nil
}

func RestoreWalletLedgerEntry(snapshot LedgerSnapshot) (WalletLedgerEntry, error) {
	return NewWalletLedgerEntry(snapshot)
}
func (e WalletLedgerEntry) Snapshot() LedgerSnapshot { return e.state }
func (e WalletLedgerEntry) Validate() error          { _, err := NewWalletLedgerEntry(e.state); return err }
