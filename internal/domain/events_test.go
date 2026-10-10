package domain

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

var eventUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func eventMetadata() EventMetadata {
	return EventMetadata{CorrelationID: "corr-1", CausationID: "msg-1", OccurredAt: time.Date(2026, 10, 1, 9, 0, 0, 0, time.FixedZone("BRT", -3*3600)), ExpiresAt: epoch.Add(24 * time.Hour)}
}

func decodeEvent(t *testing.T, e Event) map[string]any {
	t.Helper()
	raw, err := e.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("event is not JSON: %v\n%s", err, raw)
	}
	return out
}

func dataOf(t *testing.T, event map[string]any) map[string]any {
	t.Helper()
	data, ok := event["data"].(map[string]any)
	if !ok {
		t.Fatalf("event has no data object: %v", event)
	}
	return data
}

func moneyOf(t *testing.T, value any) (string, string) {
	t.Helper()
	m, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("not a money object: %v", value)
	}
	amount, _ := m["amount"].(string)
	currency, _ := m["currency"].(string)
	return amount, currency
}

func TestProcessedBetEmitsTheTransactionAndTheBalanceChange(t *testing.T) {
	cmd := command(t, KindBet, 1000, "")
	outcome := Outcome{Status: StatusProcessed, Direction: DirectionDebit, Before: brl(t, 10000), After: brl(t, 9000), Changed: true}
	events, err := BuildEvents(testTransaction, cmd, outcome, 2, eventMetadata())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].EventType() != EventWagerTransactionProcessed || events[1].EventType() != EventWalletBalanceChanged {
		t.Fatalf("a movement emits the transaction result then the balance change: %v", events)
	}
	seen := map[string]bool{}
	for _, e := range events {
		body := decodeEvent(t, e)
		id, _ := body["eventId"].(string)
		if !eventUUID.MatchString(id) || id != e.ID() || seen[id] {
			t.Errorf("event ids must be distinct UUIDv4 values: %q", id)
		}
		seen[id] = true
		if body["eventType"] != e.EventType() || body["version"] != float64(1) || body["aggregateId"] != testWallet || body["correlationId"] != "corr-1" || body["causationId"] != "msg-1" {
			t.Errorf("envelope fields are wrong: %v", body)
		}
		if e.Version() != 1 || e.AggregateID() != testWallet || e.TransactionID() != testTransaction {
			t.Errorf("accessors disagree with the payload: %v %v %v", e.Version(), e.AggregateID(), e.TransactionID())
		}
		if occurred, _ := body["occurredAt"].(string); occurred != "2026-10-01T12:00:00Z" || !e.OccurredAt().Equal(eventMetadata().OccurredAt) {
			t.Errorf("occurredAt must be the UTC instant of the metadata: %v", body["occurredAt"])
		}
	}
	processed := dataOf(t, decodeEvent(t, events[0]))
	if amount, currency := moneyOf(t, processed["money"]); amount != "10.00" || currency != "BRL" {
		t.Errorf("amount is a decimal string with its currency: %v", processed["money"])
	}
	if balance, _ := moneyOf(t, processed["balance"]); balance != "90.00" || processed["walletVersion"] != float64(2) || processed["origin"] != "EXTERNAL" ||
		processed["providerId"] != "provider-a" || processed["externalTransactionId"] != "tx-BET" || processed["kind"] != KindBet || processed["transactionId"] != testTransaction {
		t.Errorf("processed payload is wrong: %v", processed)
	}
	changed := dataOf(t, decodeEvent(t, events[1]))
	before, _ := moneyOf(t, changed["balanceBefore"])
	after, _ := moneyOf(t, changed["balanceAfter"])
	if changed["direction"] != DirectionDebit || before != "100.00" || after != "90.00" || changed["walletVersion"] != float64(2) || changed["walletId"] != testWallet {
		t.Errorf("balance change payload is wrong: %v", changed)
	}
}

func TestProcessedWithoutMovementEmitsOnlyTheTransactionResult(t *testing.T) {
	loss := command(t, KindLoss, 0, "")
	events, err := BuildEvents(testTransaction, loss, Outcome{Status: StatusProcessed, Before: brl(t, 5000), After: brl(t, 5000)}, 4, eventMetadata())
	if err != nil || len(events) != 1 || events[0].EventType() != EventWagerTransactionProcessed {
		t.Fatalf("a loss moves nothing, so there is no balance change: %v %v", events, err)
	}
}

func TestRejectedAndPendingReferenceEvents(t *testing.T) {
	refund := command(t, KindRefund, 1000, "tx-BET")
	rejected, err := BuildEvents(testTransaction, refund, Outcome{Status: StatusRejected, FailureCode: "REFERENCE_NOT_FOUND", Before: brl(t, 100), After: brl(t, 100)}, 0, eventMetadata())
	if err != nil || len(rejected) != 1 || rejected[0].EventType() != EventWagerTransactionRejected {
		t.Fatalf("a rejection emits one event: %v %v", rejected, err)
	}
	data := dataOf(t, decodeEvent(t, rejected[0]))
	if data["failureCode"] != "REFERENCE_NOT_FOUND" || data["rejectedAt"] != "2026-10-01T12:00:00Z" {
		t.Errorf("rejection payload is wrong: %v", data)
	}

	pending, err := BuildEvents(testTransaction, refund, Outcome{Status: StatusPendingReference}, 0, eventMetadata())
	if err != nil || len(pending) != 1 || pending[0].EventType() != EventWagerTransactionPendingReference {
		t.Fatalf("a pending reference emits one event: %v %v", pending, err)
	}
	data = dataOf(t, decodeEvent(t, pending[0]))
	if data["referenceExternalTransactionId"] != "tx-BET" || data["expiresAt"] != "2026-10-02T12:00:00Z" {
		t.Errorf("pending payload is wrong: %v", data)
	}
	late := eventMetadata()
	late.ExpiresAt = late.OccurredAt
	if _, err := BuildEvents(testTransaction, refund, Outcome{Status: StatusPendingReference}, 0, late); Code(err) != "INVALID_EVENT" {
		t.Errorf("a pending reference must expire after it was announced: %v", err)
	}
}

// FAILED is recorded for audit and has no integration event: nothing happened to the wallet that a downstream
// consumer could act on. Statuses that are not outcomes cannot be turned into events at all.
func TestFailedEmitsNoEventAndOtherStatusesAreRefused(t *testing.T) {
	bet := command(t, KindBet, 1000, "")
	events, err := BuildEvents(testTransaction, bet, Outcome{Status: StatusFailed, FailureCode: FailureWalletVersionExhausted}, 0, eventMetadata())
	if err != nil || events == nil || len(events) != 0 {
		t.Fatalf("FAILED must emit an empty, non-nil list: %#v %v", events, err)
	}
	for _, status := range []string{StatusPending, "", "CANCELLED"} {
		if _, err := BuildEvents(testTransaction, bet, Outcome{Status: status}, 0, eventMetadata()); Code(err) != "INVALID_EVENT" {
			t.Errorf("status %q cannot emit an event: %v", status, err)
		}
	}
}

func TestOpeningEventsCarryOnlyInternalMetadata(t *testing.T) {
	opening := Command{WalletID: testWallet, PlayerID: testPlayer, Kind: KindOpening, Money: brl(t, 5000)}
	outcome := Outcome{Status: StatusProcessed, Direction: DirectionCredit, Before: brl(t, 0), After: brl(t, 5000), Changed: true}
	events, err := BuildEvents(testWallet, opening, outcome, 1, eventMetadata())
	if err != nil || len(events) != 2 {
		t.Fatalf("an opening credit emits the result and the balance change: %v %v", events, err)
	}
	data := dataOf(t, decodeEvent(t, events[0]))
	if data["origin"] != "INTERNAL" || data["kind"] != KindOpening || data["walletVersion"] != float64(1) {
		t.Errorf("opening payload is wrong: %v", data)
	}
	for _, field := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if _, present := data[field]; present {
			t.Errorf("an internal event must not carry provider metadata (%s): %v", field, data)
		}
	}
	if changed := dataOf(t, decodeEvent(t, events[1])); changed["direction"] != DirectionCredit || changed["walletVersion"] != float64(1) {
		t.Errorf("opening balance change is wrong: %v", changed)
	}
}

func TestEventsRefuseInvalidMetadataAndResults(t *testing.T) {
	bet := command(t, KindBet, 1000, "")
	ok := Outcome{Status: StatusProcessed, Direction: DirectionDebit, Before: brl(t, 10000), After: brl(t, 9000), Changed: true}
	badMetadata := map[string]func(*EventMetadata){
		"no correlation":     func(m *EventMetadata) { m.CorrelationID = "" },
		"padded correlation": func(m *EventMetadata) { m.CorrelationID = " x " },
		"bad causation":      func(m *EventMetadata) { m.CausationID = "a\nb" },
		"no timestamp":       func(m *EventMetadata) { m.OccurredAt = time.Time{} },
	}
	for name, mutate := range badMetadata {
		m := eventMetadata()
		mutate(&m)
		if _, err := BuildEvents(testTransaction, bet, ok, 2, m); Code(err) != "INVALID_EVENT" {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := BuildEvents("not-a-uuid", bet, ok, 2, eventMetadata()); Code(err) != "INVALID_EVENT" {
		t.Errorf("bad transaction id: %v", err)
	}
	if _, err := BuildEvents(testTransaction, bet, ok, 0, eventMetadata()); Code(err) != "INVALID_EVENT" {
		t.Errorf("a processed event needs a wallet version: %v", err)
	}
	negative := ok
	negative.After = brl(t, -1)
	if _, err := BuildEvents(testTransaction, bet, negative, 2, eventMetadata()); err == nil {
		t.Error("a negative resulting balance must be refused")
	}
	broken := ok
	broken.After = brl(t, 9001) // the ledger equation does not hold
	if _, err := BuildEvents(testTransaction, bet, broken, 2, eventMetadata()); Code(err) != "INVALID_LEDGER_ENTRY" {
		t.Errorf("a balance change must satisfy the ledger equation: %v", err)
	}
	incomplete := bet
	incomplete.GameID = ""
	if _, err := BuildEvents(testTransaction, incomplete, ok, 2, eventMetadata()); Code(err) != "INVALID_EVENT" {
		t.Errorf("an external event needs its provider metadata: %v", err)
	}
	alien := bet
	alien.Kind = "TRANSFER"
	if _, err := BuildEvents(testTransaction, alien, ok, 2, eventMetadata()); Code(err) != "INVALID_EVENT" {
		t.Errorf("an external event needs a known kind: %v", err)
	}
}

func TestEventPayloadIsACopy(t *testing.T) {
	events, err := BuildEvents(testTransaction, command(t, KindBet, 1000, ""), Outcome{Status: StatusProcessed, Direction: DirectionDebit, Before: brl(t, 10000), After: brl(t, 9000), Changed: true}, 2, eventMetadata())
	if err != nil {
		t.Fatal(err)
	}
	first, _ := events[0].JSON()
	first[0] = 'X'
	again, _ := events[0].JSON()
	if again[0] != '{' {
		t.Fatal("mutating the returned payload reached the event")
	}
	if encoded, err := json.Marshal(events[0]); err != nil || string(encoded) != string(again) {
		t.Fatalf("an event serializes as its payload: %v", err)
	}
	if _, err := (WagerTransactionProcessed{}).JSON(); Code(err) != "INVALID_EVENT" {
		t.Fatalf("an uninitialized event must not serialize: %v", err)
	}
}
