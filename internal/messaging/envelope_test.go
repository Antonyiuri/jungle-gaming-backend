package messaging

import (
	"strings"
	"testing"
)

const validBody = `{"messageId":"ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d","type":"WagerTransactionRequested","occurredAt":"2026-10-09T12:00:00Z","data":{"providerId":"provider-a","externalTransactionId":"bet-002","idempotencyKey":"provider-a:bet-002","playerId":"b734f231-163d-4bed-8d91-23d81a31821e","walletId":"05f3842e-55de-4e81-85a9-b547a85b0ef8","roundId":"round-002","gameId":"game-001","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}}`

func TestParseEnvelopeAcceptsDocumentedContract(t *testing.T) {
	envelope, err := ParseEnvelope([]byte(validBody))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Data.IdempotencyKey != "provider-a:bet-002" || envelope.Data.Money.MinorUnits() != 1000 {
		t.Fatalf("unexpected data %+v", envelope.Data)
	}
}

func TestParseEnvelopeRejectsMalformedInput(t *testing.T) {
	replace := func(old, new string) string { return strings.Replace(validBody, old, new, 1) }
	bodies := map[string]string{
		"empty":                     ``,
		"not json":                  `nope`,
		"unknown field":             replace(`"type"`, `"extra":1,"type"`),
		"unknown data":              replace(`"kind":"BET"`, `"kind":"BET","bonus":"x"`),
		"duplicate key":             replace(`"type":"WagerTransactionRequested"`, `"type":"WagerTransactionRequested","type":"WagerTransactionRequested"`),
		"wrong type":                replace(`WagerTransactionRequested`, `Other`),
		"missing message":           replace(`"messageId":"ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d",`, ``),
		"missing time":              replace(`"occurredAt":"2026-10-09T12:00:00Z",`, ``),
		"bad time":                  replace(`2026-10-09T12:00:00Z`, `yesterday`),
		"float money":               replace(`"amount":"10.00"`, `"amount":10.00`),
		"trailing document":         validBody + `{}`,
		"missing key":               replace(`"idempotencyKey":"provider-a:bet-002",`, ``),
		"negative money":            replace(`"10.00"`, `"-10.00"`),
		"invalid unicode":           replace(`round-002`, `\ud800`),
		"message id with a space":   replace(`ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d`, `not safe`),
		"message id with a control": replace(`ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d`, `tab\there`),
		"message id too long":       replace(`ce111fe2-27e5-4e0f-9a17-f6af0ae78f3d`, strings.Repeat("a", 129)),
	}
	for name, body := range bodies {
		if _, err := ParseEnvelope([]byte(body)); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestEnvelopeHashBindsEnvelopeAndSender(t *testing.T) {
	base, err := ParseEnvelope([]byte(validBody))
	if err != nil {
		t.Fatal(err)
	}
	reference, err := base.Hash("provider-a")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := base.Hash("provider-a"); again != reference {
		t.Fatal("hash must be deterministic")
	}
	if other, _ := base.Hash("provider-b"); other == reference {
		t.Error("the authenticated sender must change the hash")
	}
	variants := map[string]func(*Envelope){
		"message id":      func(e *Envelope) { e.MessageID = "another" },
		"occurred at":     func(e *Envelope) { e.OccurredAt = e.OccurredAt.Add(1) },
		"idempotency key": func(e *Envelope) { e.Data.IdempotencyKey = "different" },
		"financial data":  func(e *Envelope) { e.Data.RoundID = "round-999" },
	}
	for name, mutate := range variants {
		copy := base
		mutate(&copy)
		if h, _ := copy.Hash("provider-a"); h == reference {
			t.Errorf("%s must change the envelope hash", name)
		}
	}
	zoned, err := ParseEnvelope([]byte(strings.Replace(validBody, `2026-10-09T12:00:00Z`, `2026-10-09T09:00:00-03:00`, 1)))
	if err != nil {
		t.Fatal(err)
	}
	if h, _ := zoned.Hash("provider-a"); h != reference {
		t.Error("equivalent instants must hash identically after UTC normalization")
	}
}

func TestBackoffSchedule(t *testing.T) {
	want := map[int]int32{0: 5, 1: 5, 2: 10, 3: 20, 4: 40, 5: 40, 100: 40}
	for receives, seconds := range want {
		if got := backoff(receives); got != seconds {
			t.Errorf("receive %d: got %d, want %d", receives, got, seconds)
		}
	}
}
