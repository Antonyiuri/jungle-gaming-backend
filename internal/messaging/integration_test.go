//go:build integration

package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/application"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/observability"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeQueue records broker interactions; the database behind the consumer is real.
type fakeQueue struct {
	mu         sync.Mutex
	deleted    []string
	visibility map[string]int32
	sent       []sqs.SendMessageInput
	failDelete int
	failSend   bool
	hangSend   bool // block until the caller's context expires, like a frozen broker
}

func (f *fakeQueue) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeQueue) DeleteMessage(_ context.Context, in *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failDelete > 0 {
		f.failDelete--
		return nil, errors.New("connection lost before the delete was acknowledged")
	}
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeQueue) ChangeMessageVisibility(_ context.Context, in *sqs.ChangeMessageVisibilityInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.visibility == nil {
		f.visibility = map[string]int32{}
	}
	f.visibility[aws.ToString(in.ReceiptHandle)] = in.VisibilityTimeout
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeQueue) SendMessage(ctx context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.mu.Lock()
	hang := f.hangSend
	f.mu.Unlock()
	if hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSend {
		return nil, errors.New("broker unavailable")
	}
	f.sent = append(f.sent, *in)
	return &sqs.SendMessageOutput{MessageId: aws.String(uuid.NewString())}, nil
}

// deadLettered reports whether the message was left for the broker's redrive: never acknowledged, and only
// given the short delay that lets its receives be counted quickly.
func (f *fakeQueue) deadLettered() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.deleted) != 0 || len(f.visibility) != 1 {
		return false
	}
	for _, seconds := range f.visibility {
		if seconds != deadLetterDelay {
			return false
		}
	}
	return true
}

type tokens map[string]auth.Principal

func (v tokens) Verify(_ context.Context, header string) (auth.Principal, error) {
	token := strings.TrimPrefix(header, "Bearer ")
	switch token {
	case "boom":
		return auth.Principal{}, errors.New("identity provider unreachable")
	case "stranger":
		return auth.Principal{}, auth.ErrForbidden
	}
	if p, ok := v[token]; ok {
		return p, nil
	}
	return auth.Principal{}, auth.ErrUnauthenticated
}

func testPrincipals() tokens {
	roles := map[string]bool{"wager:write": true, "wager:read": true}
	return tokens{
		"provider-a": {ClientID: "provider-a", ProviderID: "it-provider-a", Roles: roles},
		"provider-b": {ClientID: "provider-b", ProviderID: "it-provider-b", Roles: roles},
	}
}

func connect(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		url = "postgres://jungle_app:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("test database unavailable: %v", err)
	}
	return pool
}

func money(minor int64) domain.Money {
	m, err := domain.NewMoney(minor, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}

func newWallet(t *testing.T, s *store.Store, minor int64) domain.WalletSnapshot {
	t.Helper()
	w, err := s.CreateWallet(context.Background(), uuid.NewString(), money(minor))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// The test database persists between runs, so external identities are unique per run.
var runID = uuid.NewString()[:8]

func ext(name string) string { return name + "-" + runID }

func betCommand(w domain.WalletSnapshot, name string, minor int64) domain.Command {
	external := ext(name)
	return domain.Command{ProviderID: "it-provider-a", ExternalTransactionID: external, IdempotencyKey: "it-provider-a:" + external, PlayerID: w.PlayerID, WalletID: w.ID,
		RoundID: "round", GameID: "game", Kind: "BET", Money: money(minor)}
}

func envelopeBody(t *testing.T, messageID string, c domain.Command) string {
	t.Helper()
	body, err := json.Marshal(Envelope{MessageID: messageID, Type: EnvelopeType, OccurredAt: time.Now().UTC(), Data: c})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func sqsMessage(receipt, group, body, token string, receives int) sqstypes.Message {
	m := sqstypes.Message{
		MessageId: aws.String(uuid.NewString()), ReceiptHandle: aws.String(receipt), Body: aws.String(body),
		Attributes: map[string]string{"MessageGroupId": group, "ApproximateReceiveCount": fmt.Sprint(receives)},
	}
	if token != "" {
		m.MessageAttributes = map[string]sqstypes.MessageAttributeValue{"Authorization": {DataType: aws.String("String"), StringValue: aws.String("Bearer " + token)}}
	}
	return m
}

func newConsumer(s *store.Store, q Queue, v Verifier) *Consumer {
	return &Consumer{Queue: q, QueueURL: "input", Verifier: v, Service: application.New(s), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Timeout: 10 * time.Second}
}

func balance(t *testing.T, s *store.Store, id string) int64 {
	t.Helper()
	w, err := s.Wallet(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return w.Balance.MinorUnits()
}

func TestLostAckAfterCommitCausesNoSecondEffect(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	w := newWallet(t, s, 10000)
	q := &fakeQueue{failDelete: 1}
	c := newConsumer(s, q, testPrincipals())
	msg := sqsMessage("rh-1", w.ID, envelopeBody(t, uuid.NewString(), betCommand(w, "lost-ack", 1500)), "provider-a", 1)

	c.handle(context.Background(), msg) // commit succeeds, the delete is lost (process dies before acknowledging)
	if got := balance(t, s, w.ID); got != 8500 || len(q.deleted) != 0 {
		t.Fatalf("after the first delivery: balance=%d deleted=%v", got, q.deleted)
	}
	redelivered := msg
	redelivered.Attributes = map[string]string{"MessageGroupId": w.ID, "ApproximateReceiveCount": "2"}
	c.handle(context.Background(), redelivered) // the broker redelivers the same message
	if got := balance(t, s, w.ID); got != 8500 {
		t.Fatalf("redelivery changed the balance: %d", got)
	}
	if len(q.deleted) != 1 {
		t.Fatalf("the redelivery must be acknowledged now: %v", q.deleted)
	}
	page, _ := s.Ledger(context.Background(), w.ID, "", 10)
	if len(page.Entries) != 2 {
		t.Fatalf("expected opening plus one debit, got %d entries", len(page.Entries))
	}
}

func TestUntrustedMessagesHaveNoEffectAndAreLeftForRedrive(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	w := newWallet(t, s, 10000)
	good := betCommand(w, "base", 1000)
	foreign := good
	foreign.ProviderID = "it-provider-b"
	foreign.IdempotencyKey = "it-provider-b:" + good.ExternalTransactionID
	wrongType := strings.Replace(envelopeBody(t, uuid.NewString(), good), EnvelopeType, "SomethingElse", 1)
	cases := map[string]sqstypes.Message{
		"no authorization attribute":   sqsMessage("a", w.ID, envelopeBody(t, uuid.NewString(), good), "", 1),
		"forged token":                 sqsMessage("b", w.ID, envelopeBody(t, uuid.NewString(), good), "forged", 1),
		"unmapped client":              sqsMessage("c", w.ID, envelopeBody(t, uuid.NewString(), good), "stranger", 1),
		"token of another provider":    sqsMessage("d", w.ID, envelopeBody(t, uuid.NewString(), good), "provider-b", 1),
		"command of another provider":  sqsMessage("e", w.ID, envelopeBody(t, uuid.NewString(), foreign), "provider-a", 1),
		"group does not match wallet":  sqsMessage("f", uuid.NewString(), envelopeBody(t, uuid.NewString(), good), "provider-a", 1),
		"malformed body":               sqsMessage("g", w.ID, `{"messageId":`, "provider-a", 1),
		"unknown envelope type":        sqsMessage("h", w.ID, wrongType, "provider-a", 1),
		"unknown field in the command": sqsMessage("i", w.ID, strings.Replace(envelopeBody(t, uuid.NewString(), good), `"kind"`, `"bonus":"1","kind"`, 1), "provider-a", 1),
	}
	for name, msg := range cases {
		q := &fakeQueue{}
		newConsumer(s, q, testPrincipals()).handle(context.Background(), msg)
		if !q.deadLettered() {
			t.Errorf("%s: an untrusted message must be left for redrive, neither acknowledged nor given a transient backoff (deleted=%v visibility=%v)", name, q.deleted, q.visibility)
		}
	}
	if got := balance(t, s, w.ID); got != 10000 {
		t.Fatalf("untrusted messages moved money: %d", got)
	}
	if page, _ := s.Ledger(context.Background(), w.ID, "", 10); len(page.Entries) != 1 {
		t.Fatalf("untrusted messages wrote ledger entries: %d", len(page.Entries))
	}
}

func TestConflictingIdentitiesAreLeftForRedriveWithoutTouchingTheOriginal(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	w := newWallet(t, s, 10000)
	q := &fakeQueue{}
	c := newConsumer(s, q, testPrincipals())
	messageID := uuid.NewString()
	c.handle(context.Background(), sqsMessage("ok", w.ID, envelopeBody(t, messageID, betCommand(w, "orig", 1000)), "provider-a", 1))
	if len(q.deleted) != 1 {
		t.Fatalf("the legitimate message must be acknowledged: %v", q.deleted)
	}
	changed := betCommand(w, "orig", 9999) // same idempotency key, different payload
	cases := map[string]string{
		"same message id, other content":    envelopeBody(t, messageID, changed),
		"new message id, same key, payload": envelopeBody(t, uuid.NewString(), changed),
	}
	for name, body := range cases {
		conflict := &fakeQueue{}
		newConsumer(s, conflict, testPrincipals()).handle(context.Background(), sqsMessage("x", w.ID, body, "provider-a", 1))
		if !conflict.deadLettered() {
			t.Errorf("%s: must be left for redrive (deleted=%v visibility=%v)", name, conflict.deleted, conflict.visibility)
		}
	}
	if got := balance(t, s, w.ID); got != 9000 {
		t.Fatalf("the original result must stand: %d", got)
	}
}

func TestTransientFailuresAreDelayedNotRejected(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	w := newWallet(t, s, 10000)
	body := envelopeBody(t, uuid.NewString(), betCommand(w, "transient", 1000))

	q := &fakeQueue{}
	newConsumer(s, q, testPrincipals()).handle(context.Background(), sqsMessage("idp-down", w.ID, body, "boom", 2))
	if len(q.deleted) != 0 || q.visibility["idp-down"] != 10 {
		t.Fatalf("an identity provider outage must delay redelivery (second receive => 10s): deleted=%v visibility=%v", q.deleted, q.visibility)
	}

	broken, err := pgxpool.New(context.Background(), pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	broken.Close() // every use of this pool fails, like an unavailable database
	q = &fakeQueue{}
	newConsumer(store.New(broken), q, testPrincipals()).handle(context.Background(), sqsMessage("db-down", w.ID, body, "provider-a", 1))
	if len(q.deleted) != 0 || q.visibility["db-down"] != 5 {
		t.Fatalf("a database outage must not acknowledge or reject the message: deleted=%v visibility=%v", q.deleted, q.visibility)
	}
	if got := balance(t, s, w.ID); got != 10000 {
		t.Fatalf("no effect expected yet: %d", got)
	}

	q = &fakeQueue{}
	newConsumer(s, q, testPrincipals()).handle(context.Background(), sqsMessage("recovered", w.ID, body, "provider-a", 2))
	if len(q.deleted) != 1 || balance(t, s, w.ID) != 9000 {
		t.Fatalf("after recovery the message is processed once and acknowledged: deleted=%v", q.deleted)
	}
}

func TestBusinessRejectionsAreAcknowledgedBecauseTheyArePersisted(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	w := newWallet(t, s, 1000)
	q := &fakeQueue{}
	c := newConsumer(s, q, testPrincipals())
	msg := sqsMessage("broke", w.ID, envelopeBody(t, uuid.NewString(), betCommand(w, "too-much", 5000)), "provider-a", 1)
	c.handle(context.Background(), msg)
	c.handle(context.Background(), msg)
	if len(q.deleted) != 2 {
		t.Fatalf("a persisted rejection is final and acknowledged, also on redelivery: %v", q.deleted)
	}
	if got := balance(t, s, w.ID); got != 1000 {
		t.Fatalf("balance %d", got)
	}
	stored, err := s.ExternalTransaction(context.Background(), "it-provider-a", ext("too-much"))
	if err != nil || stored.Status != "REJECTED" || stored.FailureCode != "INSUFFICIENT_FUNDS" {
		t.Fatalf("the rejection must be auditable: %+v %v", stored, err)
	}
}

func TestPublisherSendsOutboxEventsWithStableIdentityAndRetriesFailures(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	ctx := context.Background()
	w := newWallet(t, s, 10000)
	if _, err := s.Process(ctx, betCommand(w, "pub-1", 1000), nil); err != nil {
		t.Fatal(err)
	}
	var expected []string
	rows, err := pool.Query(ctx, "SELECT id::text FROM outbox_events WHERE wallet_id=$1 ORDER BY id", w.ID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		expected = append(expected, id)
	}
	rows.Close()
	if len(expected) != 4 { // opening: processed + balance, bet: processed + balance
		t.Fatalf("expected 4 events, got %d", len(expected))
	}

	q := &fakeQueue{failSend: true}
	p := &Publisher{Store: s, Queue: q, QueueURL: "output", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Interval: 10 * time.Millisecond}
	deadline := time.Now().Add(30 * time.Second)
	var postponed int
	for postponed < len(expected) && time.Now().Before(deadline) {
		p.Once(ctx)
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL AND attempts>0 AND next_attempt_at>clock_timestamp() AND lease_token IS NULL", w.ID).Scan(&postponed); err != nil {
			t.Fatal(err)
		}
	}
	if postponed != len(expected) {
		t.Fatalf("a failed send must schedule a retry and keep the event: %d of %d", postponed, len(expected))
	}

	q.mu.Lock()
	q.failSend = false
	q.mu.Unlock()
	if _, err := pool.Exec(ctx, "UPDATE outbox_events SET next_attempt_at=clock_timestamp() WHERE wallet_id=$1", w.ID); err != nil {
		t.Fatal(err)
	}
	// Other tests leave older events behind and batches are small, so drain until this wallet is done.
	var pending int
	for deadline = time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		p.Once(ctx)
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			break
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	got := map[string]bool{}
	for _, in := range q.sent {
		if aws.ToString(in.MessageGroupId) == w.ID {
			id := aws.ToString(in.MessageDeduplicationId)
			got[id] = true
			var payload struct {
				EventID     string `json:"eventId"`
				AggregateID string `json:"aggregateId"`
			}
			if err := json.Unmarshal([]byte(aws.ToString(in.MessageBody)), &payload); err != nil || payload.EventID != id || payload.AggregateID != w.ID {
				t.Fatalf("the deduplication id must be the stable event id: %v %+v", err, payload)
			}
		}
	}
	for _, id := range expected {
		if !got[id] {
			t.Errorf("event %s was not published", id)
		}
	}
	var unpublished int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID).Scan(&unpublished); err != nil || unpublished != 0 {
		t.Fatalf("every published event must be marked: %d (%v)", unpublished, err)
	}
}

// A frozen broker makes SendMessage consume its whole deadline. The retry must still be recorded
// (backoff scheduled, lease released) instead of waiting for the lease to expire.
func TestPublisherRecordsTheRetryWhenTheSendUsesItsWholeDeadline(t *testing.T) {
	pool := connect(t)
	s := store.New(pool)
	ctx := context.Background()
	w := newWallet(t, s, 10000)
	q := &fakeQueue{hangSend: true}
	p := &Publisher{Store: s, Queue: q, QueueURL: "output", Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: observability.New(), Interval: 10 * time.Millisecond, SendTimeout: 150 * time.Millisecond}
	deadline := time.Now().Add(60 * time.Second)
	var postponed int
	for postponed < 2 && time.Now().Before(deadline) {
		p.Once(ctx)
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL AND attempts>0 AND lease_token IS NULL AND next_attempt_at>clock_timestamp()", w.ID).Scan(&postponed); err != nil {
			t.Fatal(err)
		}
	}
	if postponed != 2 {
		t.Fatalf("both opening events must have a recorded retry after the deadline expired, got %d", postponed)
	}
}
