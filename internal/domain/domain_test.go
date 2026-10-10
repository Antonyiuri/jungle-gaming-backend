package domain

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

const (
	testWallet = "05f3842e-55de-4e81-85a9-b547a85b0ef8"
	testPlayer = "b734f231-163d-4bed-8d91-23d81a31821e"
)

func brl(t *testing.T, minor int64) Money {
	t.Helper()
	m, err := NewMoney(minor, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func wallet(t *testing.T, minor int64) *Wallet {
	t.Helper()
	w, err := NewWallet(testWallet, testPlayer, brl(t, minor), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func command(t *testing.T, kind string, minor int64, ref string) Command {
	t.Helper()
	return Command{ProviderID: "provider-a", ExternalTransactionID: "tx-" + kind, IdempotencyKey: "key-" + kind, PlayerID: testPlayer, WalletID: testWallet,
		RoundID: "round-1", GameID: "game-1", Kind: kind, Money: brl(t, minor), ReferenceExternalTransactionID: ref}
}

func TestParseMoneyAcceptsOnlyCanonicalDecimals(t *testing.T) {
	valid := map[string]int64{"0.00": 0, "0.01": 1, "10.00": 1000, "123456.78": 12345678, "92233720368547758.07": math.MaxInt64}
	for amount, minor := range valid {
		m, err := ParseMoney(amount, "BRL")
		if err != nil || m.MinorUnits() != minor || m.Amount() != amount {
			t.Errorf("%q: got %v, %v", amount, m, err)
		}
	}
	for _, amount := range []string{"", "1", "1.0", "1.000", "01.00", "-1.00", "+1.00", "1,00", ".50", "1e2", " 1.00", "1.00 ", "1.0a", "92233720368547758.08", "99999999999999999999.99"} {
		if _, err := ParseMoney(amount, "BRL"); Code(err) != "INVALID_MONEY" {
			t.Errorf("%q must be rejected as INVALID_MONEY, got %v", amount, err)
		}
	}
	if _, err := ParseMoney("1.00", "XYZ"); Code(err) != "UNSUPPORTED_CURRENCY" {
		t.Errorf("unsupported currency accepted: %v", err)
	}
	if _, err := ParseMoney("1.00", ""); Code(err) != "INVALID_MONEY" {
		t.Errorf("empty currency accepted: %v", err)
	}
}

func TestMoneyArithmeticDetectsOverflowAndCurrencyMismatch(t *testing.T) {
	if _, err := brl(t, math.MaxInt64).Add(brl(t, 1)); Code(err) != "MONEY_OVERFLOW" {
		t.Errorf("add overflow not detected: %v", err)
	}
	if _, err := brl(t, math.MinInt64).Subtract(brl(t, 1)); Code(err) != "MONEY_OVERFLOW" {
		t.Errorf("subtract overflow not detected: %v", err)
	}
	if _, err := brl(t, math.MinInt64).Negate(); Code(err) != "MONEY_OVERFLOW" {
		t.Errorf("negate overflow not detected: %v", err)
	}
	usd, _ := NewMoney(1, "USD")
	if _, err := brl(t, 1).Add(usd); Code(err) != "CURRENCY_MISMATCH" {
		t.Errorf("currency mismatch not detected: %v", err)
	}
	sum, err := brl(t, 10).Add(brl(t, 5))
	if err != nil || sum.MinorUnits() != 15 {
		t.Errorf("unexpected sum %v, %v", sum, err)
	}
	if got := brl(t, -5).Amount(); got != "-0.05" {
		t.Errorf("negative formatting: %s", got)
	}
	if got := brl(t, math.MinInt64).Amount(); got != "-92233720368547758.08" {
		t.Errorf("minimum formatting: %s", got)
	}
	var zero Money
	if zero.Validate() == nil {
		t.Error("the zero value must be invalid")
	}
}

func TestMoneyJSONIsStrict(t *testing.T) {
	var m Money
	if err := json.Unmarshal([]byte(`{"amount":"10.00","currency":"BRL"}`), &m); err != nil || m.MinorUnits() != 1000 {
		t.Fatalf("valid money rejected: %v", err)
	}
	for _, body := range []string{`{"amount":10.00,"currency":"BRL"}`, `{"amount":"10.00"}`, `{"amount":"10.00","currency":"BRL","extra":"x"}`,
		`{"amount":"10.00","amount":"11.00","currency":"BRL"}`, `"10.00"`, `{"amount":"10.0","currency":"BRL"}`, `{"amount":"10.00","currency":"BRL"} {}`} {
		var out Money
		if err := json.Unmarshal([]byte(body), &out); err == nil {
			t.Errorf("%s must be rejected", body)
		}
	}
	encoded, err := json.Marshal(brl(t, 1234))
	if err != nil || string(encoded) != `{"amount":"12.34","currency":"BRL"}` {
		t.Errorf("unexpected encoding %s, %v", encoded, err)
	}
}

func TestCommandValidation(t *testing.T) {
	cases := map[string]func(*Command){
		"IDEMPOTENCY_KEY_REQUIRED": func(c *Command) { c.IdempotencyKey = "" },
		"INVALID_REQUEST":          func(c *Command) { c.PlayerID = "not-a-uuid" },
		"INVALID_MONEY":            func(c *Command) { c.Money = Money{minorUnits: 0, currency: "BRL"} },
		"INVALID_REFERENCE":        func(c *Command) { c.ReferenceExternalTransactionID = "ref" },
	}
	for code, mutate := range cases {
		c := command(t, KindBet, 1000, "")
		mutate(&c)
		if err := c.Validate(); Code(err) != code {
			t.Errorf("%s: got %v", code, err)
		}
	}
	if err := command(t, KindRefund, 1000, "").Validate(); Code(err) != "INVALID_REFERENCE" {
		t.Errorf("a refund without a reference must fail: %v", err)
	}
	if err := command(t, KindLoss, 100, "").Validate(); Code(err) != "INVALID_MONEY" {
		t.Errorf("a loss must be zero: %v", err)
	}
	if err := command(t, KindBet, -100, "").Validate(); Code(err) != "INVALID_MONEY" {
		t.Errorf("negative external amounts must fail: %v", err)
	}
	self := command(t, KindWin, 100, "tx-WIN")
	if err := self.Validate(); Code(err) != "INVALID_REFERENCE" {
		t.Errorf("self reference must fail: %v", err)
	}
	if err := command(t, KindOpening, 100, "").Validate(); err == nil {
		t.Error("OPENING must not be accepted as an external operation")
	}
}

// The expected hash was computed independently from the canonical text with sha256sum.
func TestCanonicalJSONAndHashVector(t *testing.T) {
	c := command(t, KindBet, 1000, "")
	c.PlayerID = strings.ToUpper(c.PlayerID)
	canonical, err := c.CanonicalJSON()
	want := `{"externalTransactionId":"tx-BET","gameId":"game-1","kind":"BET","money":{"amount":"10.00","currency":"BRL"},"playerId":"b734f231-163d-4bed-8d91-23d81a31821e","providerId":"provider-a","roundId":"round-1","walletId":"05f3842e-55de-4e81-85a9-b547a85b0ef8"}`
	if err != nil || string(canonical) != want {
		t.Fatalf("canonical mismatch:\n got %s\nwant %s\nerr %v", canonical, want, err)
	}
	hash, _ := c.Hash()
	if hash != "1b92d8c5f921c18b89321d9785b2f2d31188653a1ac49b213f788fff4bcc9dc1" {
		t.Fatalf("hash vector mismatch: %s", hash)
	}
	changed := command(t, KindBet, 1001, "")
	if h, _ := changed.Hash(); h == hash {
		t.Error("different amounts must change the hash")
	}
	differentKey := command(t, KindBet, 1000, "")
	differentKey.IdempotencyKey = "another-key"
	if h, _ := differentKey.Hash(); h != hash {
		t.Error("the idempotency key must not participate in the hash")
	}
}

func TestEvaluateBetAndWin(t *testing.T) {
	out, err := Evaluate(command(t, KindBet, 400, ""), wallet(t, 1000), nil, false)
	if err != nil || out.Status != StatusProcessed || out.Direction != DirectionDebit || out.After.MinorUnits() != 600 || !out.Changed {
		t.Fatalf("bet: %+v %v", out, err)
	}
	out, _ = Evaluate(command(t, KindBet, 1001, ""), wallet(t, 1000), nil, false)
	if out.Status != StatusRejected || out.FailureCode != "INSUFFICIENT_FUNDS" || out.After.MinorUnits() != 1000 || out.Changed {
		t.Fatalf("overdraft must be rejected without movement: %+v", out)
	}
	out, _ = Evaluate(command(t, KindBet, 1000, ""), wallet(t, 1000), nil, false)
	if out.Status != StatusProcessed || out.After.MinorUnits() != 0 {
		t.Fatalf("spending the whole balance is allowed: %+v", out)
	}
	out, _ = Evaluate(command(t, KindLoss, 0, ""), wallet(t, 1000), nil, false)
	if out.Status != StatusProcessed || out.Changed || out.After.MinorUnits() != 1000 {
		t.Fatalf("loss must not move money: %+v", out)
	}
	out, _ = Evaluate(command(t, KindWin, 250, ""), wallet(t, 1000), nil, false)
	if out.Status != StatusProcessed || out.Direction != DirectionCredit || out.After.MinorUnits() != 1250 {
		t.Fatalf("win: %+v", out)
	}
	out, _ = Evaluate(command(t, KindWin, math.MaxInt64, ""), wallet(t, 1000), nil, false)
	if out.Status != StatusRejected || out.FailureCode != "MONEY_OVERFLOW" {
		t.Fatalf("overflow must be a rejection: %+v", out)
	}
}

func TestEvaluateReferences(t *testing.T) {
	ref := &Reference{ID: "r1", ProviderID: "provider-a", ExternalTransactionID: "bet-1", PlayerID: testPlayer, WalletID: testWallet, RoundID: "round-1", GameID: "game-1", Kind: KindBet, Status: StatusProcessed, Money: brl(t, 500)}
	cases := []struct {
		name     string
		command  Command
		ref      *Reference
		reversed bool
		status   string
		code     string
		after    int64
	}{
		{"refund credits the bet back", command(t, KindRefund, 500, "bet-1"), ref, false, StatusProcessed, "", 1500},
		{"rollback of a bet credits", command(t, KindRollback, 500, "bet-1"), ref, false, StatusProcessed, "", 1500},
		{"missing reference stays pending", command(t, KindRefund, 500, "bet-1"), nil, false, StatusPendingReference, "", 1000},
		{"partial reversal is rejected", command(t, KindRefund, 100, "bet-1"), ref, false, StatusRejected, "REFERENCE_AMOUNT_MISMATCH", 1000},
		{"double reversal is rejected", command(t, KindRefund, 500, "bet-1"), ref, true, StatusRejected, "REFERENCE_ALREADY_REVERSED", 1000},
		{"win over a bet credits", command(t, KindWin, 900, "bet-1"), ref, false, StatusProcessed, "", 1900},
	}
	for _, tc := range cases {
		out, err := Evaluate(tc.command, wallet(t, 1000), tc.ref, tc.reversed)
		if err != nil || out.Status != tc.status || out.FailureCode != tc.code || out.After.MinorUnits() != tc.after {
			t.Errorf("%s: %+v %v", tc.name, out, err)
		}
	}
	pending := *ref
	pending.Status = StatusPendingReference
	if out, _ := Evaluate(command(t, KindRefund, 500, "bet-1"), wallet(t, 1000), &pending, false); out.Status != StatusPendingReference {
		t.Errorf("a pending reference keeps the dependent pending: %+v", out)
	}
	rejected := *ref
	rejected.Status = StatusRejected
	if out, _ := Evaluate(command(t, KindRefund, 500, "bet-1"), wallet(t, 1000), &rejected, false); out.FailureCode != "REFERENCE_NOT_PROCESSED" {
		t.Errorf("a rejected reference cannot be reversed: %+v", out)
	}
	other := *ref
	other.ProviderID = "provider-b"
	if out, _ := Evaluate(command(t, KindRefund, 500, "bet-1"), wallet(t, 1000), &other, false); out.FailureCode != "REFERENCE_CONTEXT_MISMATCH" {
		t.Errorf("references cannot cross providers: %+v", out)
	}
	winRef := *ref
	winRef.Kind = KindWin
	winRef.ExternalTransactionID = "win-1"
	if out, _ := Evaluate(command(t, KindRefund, 500, "win-1"), wallet(t, 1000), &winRef, false); out.FailureCode != "REFERENCE_KIND_NOT_ALLOWED" {
		t.Errorf("refund only applies to bets: %+v", out)
	}
	out, _ := Evaluate(command(t, KindRollback, 500, "win-1"), wallet(t, 1000), &winRef, false)
	if out.Status != StatusProcessed || out.Direction != DirectionDebit || out.After.MinorUnits() != 500 {
		t.Errorf("rolling back a win debits the wallet: %+v", out)
	}
	out, _ = Evaluate(command(t, KindRollback, 500, "win-1"), wallet(t, 100), &winRef, false)
	if out.FailureCode != "REVERSAL_INSUFFICIENT_FUNDS" || out.After.MinorUnits() != 100 {
		t.Errorf("a win rollback must never overdraw: %+v", out)
	}
	mismatch := command(t, KindBet, 100, "")
	mismatch.WalletID = "11111111-1111-4111-8111-111111111111"
	if out, _ := Evaluate(mismatch, wallet(t, 1000), nil, false); out.FailureCode != "WALLET_CONTEXT_MISMATCH" {
		t.Errorf("wallet mismatch must be rejected: %+v", out)
	}
}

func TestWalletInvariants(t *testing.T) {
	w := wallet(t, 100)
	if err := w.Debit(brl(t, 101)); Code(err) != "INSUFFICIENT_FUNDS" || w.Balance().MinorUnits() != 100 || w.Version() != 1 {
		t.Fatalf("failed debit must leave state untouched: %v", err)
	}
	if err := w.Debit(brl(t, 0)); err == nil {
		t.Error("zero movements are invalid")
	}
	if err := w.Credit(brl(t, 50)); err != nil || w.Balance().MinorUnits() != 150 || w.Version() != 2 {
		t.Errorf("credit: %v", err)
	}
	if _, err := NewWallet(testWallet, testPlayer, brl(t, -1), time.Now()); err == nil {
		t.Error("negative opening balance accepted")
	}
	if _, err := NewWallet("bad", testPlayer, brl(t, 1), time.Now()); err == nil {
		t.Error("invalid wallet identifier accepted")
	}
	var nilWallet *Wallet
	if err := nilWallet.Validate(); err == nil {
		t.Error("nil wallet accepted")
	}
}

func TestErrorsAreClassifiable(t *testing.T) {
	err := error(&Error{Code: "X", Message: "m"})
	wrapped := errors.Join(errors.New("context"), err)
	if Code(wrapped) != "X" || !errors.Is(wrapped, &Error{Code: "X"}) {
		t.Error("domain errors must survive wrapping")
	}
	if Code(errors.New("plain")) != "" {
		t.Error("foreign errors have no code")
	}
}

func TestWalletSnapshotWireFormatIsCamelCase(t *testing.T) {
	encoded, err := json.Marshal(wallet(t, 1234).Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"id", "playerId", "balance", "version", "createdAt", "updatedAt"} {
		if _, ok := fields[name]; !ok {
			t.Errorf("missing %q in %s", name, encoded)
		}
	}
	if len(fields) != 6 {
		t.Errorf("unexpected fields: %s", encoded)
	}
}
