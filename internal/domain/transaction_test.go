package domain

import (
	"math"
	"testing"
	"time"
)

const testTransaction = "0b5f6c8e-5c1d-4d3a-9a58-7f0b2e1c4a11"

var epoch = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

func pendingTransaction(t *testing.T, kind, reference string) *WagerTransaction {
	t.Helper()
	amount := int64(1000)
	if kind == KindLoss {
		amount = 0
	}
	tx, err := NewWagerTransaction(testTransaction, command(t, kind, amount, reference), epoch)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func processedOutcome(t *testing.T, after int64) Outcome {
	t.Helper()
	return Outcome{Status: StatusProcessed, Direction: DirectionDebit, Before: brl(t, after+1000), After: brl(t, after), Changed: true}
}

func TestNewTransactionStartsPendingWithItsCommandMetadata(t *testing.T) {
	cmd := command(t, KindBet, 1000, "")
	tx, err := NewWagerTransaction(testTransaction, cmd, epoch.In(time.FixedZone("BRT", -3*3600)))
	if err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if s.Status != StatusPending || s.Origin != "EXTERNAL" || s.Kind != KindBet || s.Money != cmd.Money || s.WalletID != testWallet || s.PlayerID != testPlayer {
		t.Fatalf("unexpected initial state: %+v", s)
	}
	if s.Balance != nil || s.WalletVersion != 0 || s.FailureCode != "" || s.ReferenceID != "" || tx.IsTerminal() {
		t.Fatalf("a new transaction carries no result: %+v", s)
	}
	if s.CreatedAt.Location() != time.UTC || !s.CreatedAt.Equal(epoch) || !s.UpdatedAt.Equal(s.CreatedAt) {
		t.Fatalf("timestamps must be UTC and equal at creation: %v %v", s.CreatedAt, s.UpdatedAt)
	}
}

func TestNewTransactionRejectsInvalidIdentityOrCommand(t *testing.T) {
	good := command(t, KindBet, 1000, "")
	bad := good
	bad.Kind = "TRANSFER"
	zeroID := "00000000-0000-0000-0000-000000000000"
	cases := map[string]struct {
		id  string
		cmd Command
		at  time.Time
	}{
		"bad transaction id":  {"not-a-uuid", good, epoch},
		"nil transaction id":  {zeroID, good, epoch},
		"zero creation time":  {testTransaction, good, time.Time{}},
		"invalid command":     {testTransaction, bad, epoch},
		"empty command":       {testTransaction, Command{}, epoch},
		"missing idempotency": {testTransaction, Command{ProviderID: "p"}, epoch},
	}
	for name, c := range cases {
		if _, err := NewWagerTransaction(c.id, c.cmd, c.at); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
}

func TestOpeningTransactionCarriesOnlyInternalMetadata(t *testing.T) {
	tx, err := NewOpeningTransaction(testWallet, testWallet, testPlayer, brl(t, 5000), epoch)
	if err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	if s.Origin != "INTERNAL" || s.Kind != KindOpening || s.Status != StatusProcessed || s.WalletVersion != 1 || s.Balance == nil || s.Balance.MinorUnits() != 5000 {
		t.Fatalf("unexpected opening: %+v", s)
	}
	if s.Command != (Command{}) {
		t.Fatalf("an opening has no provider command: %+v", s.Command)
	}
	if !tx.IsTerminal() {
		t.Fatal("an opening is born processed")
	}
	if err := tx.Fail("ANYTHING", epoch.Add(time.Second)); Code(err) != "INVALID_TRANSITION" {
		t.Fatalf("an opening is immutable: %v", err)
	}
	for name, amount := range map[string]Money{"zero": brl(t, 0), "negative": brl(t, -1)} {
		if _, err := NewOpeningTransaction(testWallet, testWallet, testPlayer, amount, epoch); Code(err) != "INVALID_TRANSACTION" {
			t.Errorf("%s opening must be refused: %v", name, err)
		}
	}
}

func TestTransactionTransitions(t *testing.T) {
	later := epoch.Add(time.Minute)
	type step struct {
		outcome Outcome
		version int64
		wantErr string
	}
	cases := []struct {
		name      string
		kind, ref string
		from      string // PENDING, or PENDING_REFERENCE reached first
		step      step
		want      string
	}{
		{"pending to processed", KindBet, "", StatusPending, step{outcome: processedOutcome(t, 9000), version: 2}, StatusProcessed},
		{"pending to rejected", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusRejected, FailureCode: "INSUFFICIENT_FUNDS"}}, StatusRejected},
		{"pending to failed", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusFailed, FailureCode: FailureWalletVersionExhausted}}, StatusFailed},
		{"pending to pending reference", KindRefund, "tx-BET", StatusPending, step{outcome: Outcome{Status: StatusPendingReference}}, StatusPendingReference},
		{"pending reference to processed", KindRefund, "tx-BET", StatusPendingReference, step{outcome: processedOutcome(t, 9000), version: 3}, StatusProcessed},
		{"pending reference to rejected", KindRefund, "tx-BET", StatusPendingReference, step{outcome: Outcome{Status: StatusRejected, FailureCode: "REFERENCE_NOT_FOUND"}}, StatusRejected},
		{"pending reference to failed", KindRefund, "tx-BET", StatusPendingReference, step{outcome: Outcome{Status: StatusFailed, FailureCode: FailureWalletVersionExhausted}}, StatusFailed},
		{"pending reference stays pending", KindRefund, "tx-BET", StatusPendingReference, step{outcome: Outcome{Status: StatusPendingReference}}, StatusPendingReference},

		{"pending cannot be re-entered", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusPending}, wantErr: "INVALID_TRANSITION"}, StatusPending},
		{"unknown target", KindBet, "", StatusPending, step{outcome: Outcome{Status: "CANCELLED"}, wantErr: "INVALID_TRANSITION"}, StatusPending},
		{"empty target", KindBet, "", StatusPending, step{outcome: Outcome{}, wantErr: "INVALID_TRANSITION"}, StatusPending},
		{"a bet has no reference to wait for", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusPendingReference}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"processed needs a result balance", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusProcessed}, version: 2, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"processed needs a wallet version", KindBet, "", StatusPending, step{outcome: processedOutcome(t, 9000), version: 0, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"processed cannot leave a negative balance", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusProcessed, After: brl(t, -1)}, version: 2, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"rejected needs a failure code", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusRejected}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"failed needs a failure code", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusFailed}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"failure code must be well formed", KindBet, "", StatusPending, step{outcome: Outcome{Status: StatusFailed, FailureCode: " padded "}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"a pending result cannot carry a failure code", KindRefund, "tx-BET", StatusPending, step{outcome: Outcome{Status: StatusPendingReference, FailureCode: "X"}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
		{"reference id must be a UUID", KindRefund, "tx-BET", StatusPending, step{outcome: Outcome{Status: StatusRejected, FailureCode: "X", ReferenceID: "nope"}, wantErr: "INVALID_TRANSACTION"}, StatusPending},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tx := pendingTransaction(t, c.kind, c.ref)
			if c.from == StatusPendingReference {
				if err := tx.Apply(Outcome{Status: StatusPendingReference}, 0, epoch.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			}
			before := tx.Snapshot()
			err := tx.Apply(c.step.outcome, c.step.version, later)
			if c.step.wantErr != "" {
				if Code(err) != c.step.wantErr {
					t.Fatalf("want %s, got %v", c.step.wantErr, err)
				}
				if after := tx.Snapshot(); after.Status != before.Status || after.UpdatedAt != before.UpdatedAt || after.FailureCode != before.FailureCode {
					t.Fatalf("a refused transition must leave the transaction untouched: %+v -> %+v", before, after)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := tx.Status(); got != c.want {
				t.Fatalf("status %s, want %s", got, c.want)
			}
			if s := tx.Snapshot(); !s.UpdatedAt.Equal(later) || !s.CreatedAt.Equal(epoch) {
				t.Fatalf("only updatedAt moves: %+v", s)
			}
		})
	}
}

func TestTerminalTransactionsAreImmutable(t *testing.T) {
	for _, target := range []Outcome{
		processedOutcome(t, 9000),
		{Status: StatusRejected, FailureCode: "INSUFFICIENT_FUNDS"},
		{Status: StatusFailed, FailureCode: FailureWalletVersionExhausted},
	} {
		tx := pendingTransaction(t, KindBet, "")
		if err := tx.Apply(target, 2, epoch.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if !tx.IsTerminal() {
			t.Fatalf("%s is terminal", target.Status)
		}
		frozen := tx.Snapshot()
		for _, next := range []Outcome{
			processedOutcome(t, 8000),
			{Status: StatusRejected, FailureCode: "OTHER"},
			{Status: StatusFailed, FailureCode: "OTHER"},
			{Status: StatusPendingReference},
		} {
			if err := tx.Apply(next, 3, epoch.Add(time.Hour)); Code(err) != "INVALID_TRANSITION" {
				t.Fatalf("%s -> %s must be refused, got %v", target.Status, next.Status, err)
			}
		}
		if got := tx.Snapshot(); got.Status != frozen.Status || got.FailureCode != frozen.FailureCode || got.WalletVersion != frozen.WalletVersion || !got.UpdatedAt.Equal(frozen.UpdatedAt) {
			t.Fatalf("a terminal transaction changed: %+v -> %+v", frozen, got)
		}
	}
}

func TestRejectAndFailRecordTheirCodeWithoutAFinancialResult(t *testing.T) {
	rejected := pendingTransaction(t, KindBet, "")
	if err := rejected.Reject("INSUFFICIENT_FUNDS", epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if s := rejected.Snapshot(); s.Status != StatusRejected || s.FailureCode != "INSUFFICIENT_FUNDS" || s.WalletVersion != 0 {
		t.Fatalf("unexpected rejection: %+v", s)
	}
	failed := pendingTransaction(t, KindBet, "")
	if err := failed.Fail(FailureWalletVersionExhausted, epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if s := failed.Snapshot(); s.Status != StatusFailed || s.FailureCode != FailureWalletVersionExhausted || s.Balance != nil {
		t.Fatalf("a permanent failure records its code and no balance: %+v", s)
	}
	if err := pendingTransaction(t, KindBet, "").Fail("", epoch.Add(time.Second)); Code(err) != "INVALID_TRANSACTION" {
		t.Fatalf("a failure needs a code: %v", err)
	}
	var missing *WagerTransaction
	if err := missing.Apply(Outcome{Status: StatusFailed, FailureCode: "X"}, 0, epoch); Code(err) != "INVALID_TRANSACTION" {
		t.Fatalf("a nil aggregate must be refused: %v", err)
	}
}

// Clock steps and skew must not make a transaction impossible to finish (compare the wallet): the transition
// time is clamped to the last update, so updatedAt never runs backwards and the transition still succeeds.
func TestTransitionTimeNeverMovesBackwards(t *testing.T) {
	tx := pendingTransaction(t, KindBet, "")
	if err := tx.Fail(FailureWalletVersionExhausted, epoch.Add(-time.Hour)); err != nil {
		t.Fatalf("a skewed clock must not block a transition: %v", err)
	}
	if s := tx.Snapshot(); !s.UpdatedAt.Equal(epoch) || s.Status != StatusFailed {
		t.Fatalf("updatedAt must be clamped to the previous update: %+v", s)
	}
}

func TestSnapshotsAreDefensiveCopies(t *testing.T) {
	tx := pendingTransaction(t, KindBet, "")
	if err := tx.Apply(processedOutcome(t, 9000), 2, epoch.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	s := tx.Snapshot()
	*s.Balance = brl(t, 1)
	s.Status = StatusPending
	if got := tx.Snapshot(); got.Balance.MinorUnits() != 9000 || got.Status != StatusProcessed {
		t.Fatalf("mutating a snapshot reached the aggregate: %+v", got)
	}
}

func TestRestoreRefusesInconsistentTransactions(t *testing.T) {
	cmd := command(t, KindBet, 1000, "")
	other := command(t, KindBet, 2000, "")
	balance := brl(t, 9000)
	negative := brl(t, -5)
	base := func() TransactionSnapshot {
		return TransactionSnapshot{ID: testTransaction, Origin: "EXTERNAL", Command: cmd, WalletID: testWallet, PlayerID: testPlayer, Kind: KindBet, Money: cmd.Money, Status: StatusPending, CreatedAt: epoch, UpdatedAt: epoch}
	}
	mutations := map[string]func(*TransactionSnapshot){
		"pending with a balance":      func(s *TransactionSnapshot) { s.Balance = &balance },
		"pending with a version":      func(s *TransactionSnapshot) { s.WalletVersion = 2 },
		"pending with a failure code": func(s *TransactionSnapshot) { s.FailureCode = "X" },
		"processed without a balance": func(s *TransactionSnapshot) { s.Status = StatusProcessed; s.WalletVersion = 2 },
		"processed without a version": func(s *TransactionSnapshot) { s.Status = StatusProcessed; s.Balance = &balance },
		"processed with a failure": func(s *TransactionSnapshot) {
			s.Status = StatusProcessed
			s.Balance = &balance
			s.WalletVersion = 2
			s.FailureCode = "X"
		},
		"negative result balance":      func(s *TransactionSnapshot) { s.Status = StatusProcessed; s.Balance = &negative; s.WalletVersion = 2 },
		"rejected without a code":      func(s *TransactionSnapshot) { s.Status = StatusRejected },
		"failed without a code":        func(s *TransactionSnapshot) { s.Status = StatusFailed },
		"unknown status":               func(s *TransactionSnapshot) { s.Status = "DONE" },
		"unknown origin":               func(s *TransactionSnapshot) { s.Origin = "OTHER" },
		"command and metadata differ":  func(s *TransactionSnapshot) { s.Command = other },
		"wallet differs from command":  func(s *TransactionSnapshot) { s.WalletID = "6f1c3a52-0f55-4e5e-9a5d-3a7a8f9d2b10" },
		"invalid money":                func(s *TransactionSnapshot) { s.Money = Money{} },
		"internal origin with command": func(s *TransactionSnapshot) { s.Origin = "INTERNAL" },
		"bad wallet identifier":        func(s *TransactionSnapshot) { s.WalletID = "wallet" },
	}
	for name, mutate := range mutations {
		s := base()
		mutate(&s)
		if _, err := RestoreWagerTransaction(s); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
	if _, err := RestoreWagerTransaction(base()); err != nil {
		t.Fatalf("the base snapshot is valid: %v", err)
	}
}

func TestMoneyRejectsNonNumericSpecialValues(t *testing.T) {
	for _, amount := range []string{"NaN", "NaN.00", "Infinity", "+Inf", "-Inf", "Inf.00", "1.0e", "0x10.00", "١٢.٠٠", "1_0.00"} {
		if _, err := ParseMoney(amount, "BRL"); Code(err) != "INVALID_MONEY" {
			t.Errorf("%q must be INVALID_MONEY, got %v", amount, err)
		}
	}
	var m Money
	for _, raw := range []string{`{"amount":NaN,"currency":"BRL"}`, `{"amount":"NaN","currency":"BRL"}`, `{"amount":1e400,"currency":"BRL"}`, `{"amount":"Infinity","currency":"BRL"}`} {
		if err := m.UnmarshalJSON([]byte(raw)); err == nil {
			t.Errorf("%s must be refused", raw)
		}
	}
	if _, err := NewMoney(math.MaxInt64, "BRL"); err != nil {
		t.Fatalf("the largest representable amount is valid: %v", err)
	}
}

func TestEvaluateFailsPermanentlyOnlyWhenTheMovementCannotBeRecorded(t *testing.T) {
	exhausted := func(minor int64) *Wallet {
		w, err := RestoreWallet(WalletSnapshot{ID: testWallet, PlayerID: testPlayer, Balance: brl(t, minor), Version: math.MaxInt64, CreatedAt: epoch, UpdatedAt: epoch})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}
	bet := command(t, KindBet, 1000, "")
	got, err := Evaluate(bet, exhausted(5000), nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusFailed || got.FailureCode != FailureWalletVersionExhausted || got.Changed || got.After != got.Before || got.Direction != "" {
		t.Fatalf("a movement on an exhausted wallet is a permanent failure that moves nothing: %+v", got)
	}
	if again, _ := Evaluate(bet, exhausted(5000), nil, false); again != got {
		t.Fatalf("the same facts must always give the same outcome: %+v vs %+v", again, got)
	}
	if loss, err := Evaluate(command(t, KindLoss, 0, ""), exhausted(5000), nil, false); err != nil || loss.Status != StatusProcessed || loss.Changed {
		t.Fatalf("a loss needs no movement and still works: %+v %v", loss, err)
	}
	// A decision that is already a business rejection never becomes a failure.
	if broke, err := Evaluate(bet, exhausted(500), nil, false); err != nil || broke.Status != StatusRejected || broke.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("insufficient funds stays a rejection: %+v %v", broke, err)
	}
	// One version below the limit still moves normally.
	almost, _ := RestoreWallet(WalletSnapshot{ID: testWallet, PlayerID: testPlayer, Balance: brl(t, 5000), Version: math.MaxInt64 - 1, CreatedAt: epoch, UpdatedAt: epoch})
	if ok, err := Evaluate(bet, almost, nil, false); err != nil || ok.Status != StatusProcessed || !ok.Changed {
		t.Fatalf("the last available version is usable: %+v %v", ok, err)
	}
}
