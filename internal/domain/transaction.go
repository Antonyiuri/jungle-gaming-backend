package domain

import (
	"strings"
	"time"
)

type TransactionSnapshot struct {
	ID, Origin                       string
	Command                          Command
	WalletID, PlayerID, Kind         string
	Money                            Money
	Status, ReferenceID, FailureCode string
	Balance                          *Money
	WalletVersion                    int64
	CreatedAt, UpdatedAt             time.Time
}

type WagerTransaction struct{ state TransactionSnapshot }

func NewWagerTransaction(id string, command Command, now time.Time) (*WagerTransaction, error) {
	return RestoreWagerTransaction(TransactionSnapshot{ID: id, Origin: "EXTERNAL", Command: command, WalletID: command.WalletID, PlayerID: command.PlayerID, Kind: command.Kind, Money: command.Money, Status: StatusPending, CreatedAt: now, UpdatedAt: now})
}

func NewOpeningTransaction(id, walletID, playerID string, amount Money, now time.Time) (*WagerTransaction, error) {
	return RestoreWagerTransaction(TransactionSnapshot{ID: id, Origin: "INTERNAL", WalletID: walletID, PlayerID: playerID, Kind: KindOpening, Money: amount, Status: StatusProcessed, Balance: &amount, WalletVersion: 1, CreatedAt: now, UpdatedAt: now})
}

func RestoreWagerTransaction(snapshot TransactionSnapshot) (*WagerTransaction, error) {
	if !validUUID(snapshot.ID) || !validUUID(snapshot.WalletID) || !validUUID(snapshot.PlayerID) || snapshot.CreatedAt.IsZero() {
		return nil, fail("INVALID_TRANSACTION", "transaction identity or timestamps are invalid")
	}
	// Timestamp order is audit data that clock skew or a clock step can violate; see RestoreWallet.
	if snapshot.UpdatedAt.Before(snapshot.CreatedAt) {
		snapshot.UpdatedAt = snapshot.CreatedAt
	}
	if err := snapshot.Money.Validate(); err != nil {
		return nil, err
	}
	if snapshot.Origin == "EXTERNAL" {
		if err := snapshot.Command.Validate(); err != nil {
			return nil, err
		}
		if !strings.EqualFold(snapshot.Command.WalletID, snapshot.WalletID) || !strings.EqualFold(snapshot.Command.PlayerID, snapshot.PlayerID) || snapshot.Command.Kind != snapshot.Kind || snapshot.Command.Money != snapshot.Money {
			return nil, fail("INVALID_TRANSACTION", "transaction metadata must match its command")
		}
	} else if snapshot.Origin == "INTERNAL" {
		if snapshot.Kind != KindOpening || snapshot.Money.MinorUnits() <= 0 || snapshot.Command != (Command{}) || snapshot.Status != StatusProcessed || snapshot.WalletVersion != 1 {
			return nil, fail("INVALID_TRANSACTION", "opening transaction must contain only internal metadata")
		}
	} else {
		return nil, fail("INVALID_TRANSACTION", "transaction origin is invalid")
	}
	switch snapshot.Status {
	case StatusPending:
		if snapshot.FailureCode != "" || snapshot.Balance != nil || snapshot.WalletVersion != 0 {
			return nil, fail("INVALID_TRANSACTION", "pending transaction cannot contain a final result")
		}
	case StatusPendingReference:
		if snapshot.Command.ReferenceExternalTransactionID == "" || snapshot.FailureCode != "" {
			return nil, fail("INVALID_TRANSACTION", "pending reference requires an external reference")
		}
	case StatusProcessed:
		if snapshot.FailureCode != "" || snapshot.Balance == nil || snapshot.WalletVersion < 1 {
			return nil, fail("INVALID_TRANSACTION", "processed transaction requires a financial result")
		}
	case StatusRejected, StatusFailed:
		if !validIdentifier(snapshot.FailureCode) {
			return nil, fail("INVALID_TRANSACTION", "terminal failure requires a failure code")
		}
	default:
		return nil, fail("INVALID_TRANSACTION", "transaction status is invalid")
	}
	if snapshot.Balance != nil {
		if err := snapshot.Balance.Validate(); err != nil {
			return nil, err
		}
		if snapshot.Balance.MinorUnits() < 0 {
			return nil, fail("INVALID_TRANSACTION", "result balance cannot be negative")
		}
		balance := *snapshot.Balance
		snapshot.Balance = &balance
	}
	if snapshot.ReferenceID != "" && !validUUID(snapshot.ReferenceID) {
		return nil, fail("INVALID_TRANSACTION", "internal reference identifier must be a UUID")
	}
	snapshot.ID = strings.ToLower(snapshot.ID)
	snapshot.WalletID = strings.ToLower(snapshot.WalletID)
	snapshot.PlayerID = strings.ToLower(snapshot.PlayerID)
	snapshot.CreatedAt = snapshot.CreatedAt.UTC()
	snapshot.UpdatedAt = snapshot.UpdatedAt.UTC()
	return &WagerTransaction{state: snapshot}, nil
}

func (t *WagerTransaction) Snapshot() TransactionSnapshot {
	snapshot := t.state
	if snapshot.Balance != nil {
		balance := *snapshot.Balance
		snapshot.Balance = &balance
	}
	return snapshot
}
func (t *WagerTransaction) ID() string     { return t.state.ID }
func (t *WagerTransaction) Status() string { return t.state.Status }
func (t *WagerTransaction) IsTerminal() bool {
	return t.state.Status == StatusProcessed || t.state.Status == StatusRejected || t.state.Status == StatusFailed
}

// Apply persists no effects and emits no events; it validates an explicit transition.
func (t *WagerTransaction) Apply(outcome Outcome, walletVersion int64, now time.Time) error {
	if t == nil {
		return fail("INVALID_TRANSACTION", "transaction is nil")
	}
	if _, err := RestoreWagerTransaction(t.state); err != nil {
		return err
	}
	if t.IsTerminal() {
		return fail("INVALID_TRANSITION", "terminal transactions are immutable")
	}
	// Audit timestamps never run backwards, but a clock step or skew between the application and the database
	// must not make a transaction impossible to finish (see RestoreWallet), so the time is clamped, not refused.
	if now.Before(t.state.UpdatedAt) {
		now = t.state.UpdatedAt
	}
	switch outcome.Status {
	case StatusProcessed, StatusRejected, StatusFailed, StatusPendingReference:
	default:
		return fail("INVALID_TRANSITION", "target state is not allowed")
	}
	snapshot := t.Snapshot()
	snapshot.Status = outcome.Status
	snapshot.FailureCode = outcome.FailureCode
	snapshot.ReferenceID = outcome.ReferenceID
	snapshot.UpdatedAt = now.UTC()
	snapshot.WalletVersion = walletVersion
	if outcome.After.Validate() == nil {
		balance := outcome.After
		snapshot.Balance = &balance
	}
	restored, err := RestoreWagerTransaction(snapshot)
	if err != nil {
		return err
	}
	t.state = restored.state
	return nil
}

func (t *WagerTransaction) Reject(code string, now time.Time) error {
	return t.Apply(Outcome{Status: StatusRejected, FailureCode: code}, 0, now)
}
func (t *WagerTransaction) Fail(code string, now time.Time) error {
	return t.Apply(Outcome{Status: StatusFailed, FailureCode: code}, 0, now)
}
