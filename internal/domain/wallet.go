package domain

import (
	"math"
	"strings"
	"time"
)

// WalletSnapshot is the persisted wallet state and the wire representation returned by the API.
type WalletSnapshot struct {
	ID        string    `json:"id"`
	PlayerID  string    `json:"playerId"`
	Balance   Money     `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Wallet struct{ state WalletSnapshot }

func NewWallet(id, player string, initial Money, now time.Time) (*Wallet, error) {
	return RestoreWallet(WalletSnapshot{ID: id, PlayerID: player, Balance: initial, Version: 1, CreatedAt: now, UpdatedAt: now})
}

func RestoreWallet(snapshot WalletSnapshot) (*Wallet, error) {
	if !validUUID(snapshot.ID) || !validUUID(snapshot.PlayerID) {
		return nil, fail("INVALID_WALLET", "wallet and player identifiers must be UUIDs")
	}
	if err := snapshot.Balance.Validate(); err != nil {
		return nil, err
	}
	if snapshot.Balance.MinorUnits() < 0 || snapshot.Version < 1 || snapshot.CreatedAt.IsZero() {
		return nil, fail("INVALID_WALLET", "wallet state violates an invariant")
	}
	// The two timestamps can come from different clocks (the application's when a wallet was created by an
	// older build, the database's afterwards) and clocks step or skew. Their order is audit data, not a
	// financial invariant: rejecting it would make an otherwise valid wallet permanently unusable, because
	// every later operation starts by restoring it.
	if snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		snapshot.UpdatedAt = snapshot.CreatedAt
	}
	snapshot.ID = strings.ToLower(snapshot.ID)
	snapshot.PlayerID = strings.ToLower(snapshot.PlayerID)
	snapshot.CreatedAt = snapshot.CreatedAt.UTC()
	snapshot.UpdatedAt = snapshot.UpdatedAt.UTC()
	return &Wallet{state: snapshot}, nil
}

func (w *Wallet) Validate() error {
	if w == nil {
		return fail("INVALID_WALLET", "wallet is nil")
	}
	_, err := RestoreWallet(w.state)
	return err
}
func (w *Wallet) Snapshot() WalletSnapshot { return w.state }
func (w *Wallet) ID() string               { return w.state.ID }
func (w *Wallet) PlayerID() string         { return w.state.PlayerID }
func (w *Wallet) Balance() Money           { return w.state.Balance }
func (w *Wallet) Version() int64           { return w.state.Version }
func (w *Wallet) CreatedAt() time.Time     { return w.state.CreatedAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.state.UpdatedAt }

func (w *Wallet) Credit(amount Money) error { return w.move(amount, DirectionCredit) }
func (w *Wallet) Debit(amount Money) error  { return w.move(amount, DirectionDebit) }

func (w *Wallet) move(amount Money, direction string) error {
	if err := w.Validate(); err != nil {
		return err
	}
	if err := amount.Validate(); err != nil {
		return err
	}
	if amount.MinorUnits() <= 0 {
		return fail("INVALID_MONEY", "wallet movement requires a positive amount")
	}
	if w.state.Version == math.MaxInt64 {
		return fail("VERSION_OVERFLOW", "wallet version exceeds the supported range")
	}
	var balance Money
	var err error
	if direction == DirectionCredit {
		balance, err = w.state.Balance.Add(amount)
	} else {
		balance, err = w.state.Balance.Subtract(amount)
	}
	if err != nil {
		return err
	}
	if balance.MinorUnits() < 0 {
		return fail("INSUFFICIENT_FUNDS", "wallet has insufficient funds")
	}
	w.state.Balance = balance
	w.state.Version++
	now := time.Now().UTC()
	if now.Before(w.state.UpdatedAt) {
		now = w.state.UpdatedAt
	}
	w.state.UpdatedAt = now
	return nil
}
