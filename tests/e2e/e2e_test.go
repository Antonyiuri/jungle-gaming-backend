//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

func wagerErr(base, provider, bearer string, command map[string]any, key string) (response, error) {
	return callErr(base, "POST", "/wagering/transactions", bearer, command, map[string]string{"Idempotency-Key": key})
}

func outboxIDs(t testing.TB, walletID string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT id::text FROM outbox_events WHERE wallet_id=$1", walletID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func assertEventsDelivered(t testing.TB, w wallet, timeout time.Duration) {
	t.Helper()
	ids := outboxIDs(t, w.ID)
	if len(ids) == 0 {
		t.Fatal("no outbox events to compare")
	}
	wait(t, timeout, fmt.Sprintf("%d events of wallet %s to reach the output queue", len(ids), w.ID), func() bool {
		got := events.forWallet(w.ID)
		for _, id := range ids {
			if got[id] == 0 {
				return false
			}
		}
		return true
	})
	wait(t, timeout, "outbox rows to be marked published", func() bool {
		return queryInt(t, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID) == 0
	})
}

func TestAuthorizationMatrix(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	a, b, internal := token(t, "provider-a"), token(t, "provider-b"), token(t, "wallet-service")
	external := uid("auth")
	bet := w.command("provider-a", "BET", external, "10.00", "")
	key := map[string]string{"Idempotency-Key": "k-" + external}
	forged := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl"
	walletBody := map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}}
	cases := []struct {
		name, method, path, bearer string
		body                       any
		headers                    map[string]string
		status                     int
	}{
		{"wager without a token", "POST", "/wagering/transactions", "", bet, key, 401},
		{"wager with a forged token", "POST", "/wagering/transactions", forged, bet, key, 401},
		{"provider-b submits provider-a's command", "POST", "/wagering/transactions", b, bet, key, 403},
		{"internal client submits a wager", "POST", "/wagering/transactions", internal, bet, key, 403},
		{"wallet without a token", "GET", "/wallets/" + w.ID, "", nil, nil, 401},
		{"provider reads a wallet", "GET", "/wallets/" + w.ID, a, nil, nil, 403},
		{"provider creates a wallet", "POST", "/wallets", a, walletBody, nil, 403},
		{"provider reads a ledger", "GET", "/wallets/" + w.ID + "/ledger", a, nil, nil, 403},
		{"provider reconciles", "POST", "/wallets/" + w.ID + "/reconciliation", a, nil, nil, 403},
		{"provider reads metrics", "GET", "/metrics", a, nil, nil, 403},
		{"internal client reads a provider namespace", "GET", "/providers/provider-a/wagering/transactions/x", internal, nil, nil, 403},
	}
	for _, c := range cases {
		if r := call(t, bases[1], c.method, c.path, c.bearer, c.body, c.headers); r.Status != c.status {
			t.Errorf("%s: want %d, got %d %s", c.name, c.status, r.Status, r.Body)
		}
	}
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("a refused request moved money: %s", got)
	}
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1", external); n != 0 {
		t.Fatalf("a refused request left %d transactions", n)
	}

	ok := postWager(t, bases[2], "provider-a", bet, "k-"+external)
	if ok.Status != 200 || ok.str("status") != "PROCESSED" {
		t.Fatalf("authorized wager: %d %s", ok.Status, ok.Body)
	}
	id := ok.str("transactionId")
	if r := call(t, bases[0], "GET", "/wagering/transactions/"+id, b, nil, nil); r.Status != 404 {
		t.Errorf("another provider must not see the transaction by id: %d %s", r.Status, r.Body)
	}
	if r := call(t, bases[0], "GET", "/providers/provider-a/wagering/transactions/"+external, b, nil, nil); r.Status != 403 {
		t.Errorf("another provider must not read this namespace: %d", r.Status)
	}
	if r := call(t, bases[0], "GET", "/providers/provider-a/wagering/transactions/"+external, a, nil, nil); r.Status != 200 || r.str("transactionId") != id {
		t.Errorf("the owner can read it: %d %s", r.Status, r.Body)
	}
	// Idempotency is scoped to the provider: provider-b reusing the same identifiers is a separate operation.
	other := w.command("provider-b", "BET", external, "10.00", "")
	if r := postWager(t, bases[1], "provider-b", other, "k-"+external); r.Status != 200 || r.str("transactionId") == id || r.field("idempotentReplay") != false {
		t.Errorf("provider-b's identical identifiers must not collide with provider-a: %d %s", r.Status, r.Body)
	}
	if got := balanceMinor(t, w); got != "80.00" {
		t.Errorf("two independent bets of 10.00: %s", got)
	}
	reconcile(t, w)
}

func TestWalletLifecycleAcrossInstances(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	betID, refundID, winID := uid("bet"), uid("refund"), uid("win")

	pending := postWager(t, bases[2], "provider-a", w.command("provider-a", "REFUND", refundID, "20.00", betID), uid("k"))
	if pending.Status != 202 || pending.str("status") != "PENDING_REFERENCE" || !strings.Contains(pending.Header.Get("Location"), pending.str("transactionId")) {
		t.Fatalf("a reversal that arrives before its bet must be accepted as pending: %d %s", pending.Status, pending.Body)
	}
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("a pending reversal moves no money: %s", got)
	}
	correlation := "e2e-" + uuid.NewString()[:8]
	bet := call(t, bases[1], "POST", "/wagering/transactions", token(t, "provider-a"), w.command("provider-a", "BET", betID, "20.00", ""),
		map[string]string{"Idempotency-Key": uid("k"), "X-Correlation-ID": correlation})
	if bet.Status != 200 || bet.str("balance", "amount") != "80.00" {
		t.Fatalf("bet: %d %s", bet.Status, bet.Body)
	}
	if bet.Header.Get("X-Correlation-ID") != correlation {
		t.Fatalf("the correlation ID must be echoed: %q", bet.Header.Get("X-Correlation-ID"))
	}
	if n := queryInt(t, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1 AND payload->>'correlationId'=$2", bet.str("transactionId"), correlation); n != 2 {
		t.Fatalf("both events of the bet must carry the request correlation ID, found %d", n)
	}
	wait(t, 90*time.Second, "the pending refund to be resolved by a background worker", func() bool {
		r := call(t, bases[0], "GET", "/providers/provider-a/wagering/transactions/"+refundID, token(t, "provider-a"), nil, nil)
		return r.Status == 200 && r.str("status") == "PROCESSED"
	})
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("after the refund: %s", got)
	}
	win := postWager(t, bases[0], "provider-a", w.command("provider-a", "WIN", winID, "50.00", ""), uid("k"))
	if win.Status != 200 || win.str("balance", "amount") != "150.00" {
		t.Fatalf("win: %d %s", win.Status, win.Body)
	}
	dup := postWager(t, bases[2], "provider-a", w.command("provider-a", "ROLLBACK", uid("rb"), "20.00", betID), uid("k"))
	if dup.Status != 422 || dup.str("failureCode") != "REFERENCE_ALREADY_REVERSED" || dup.str("balance", "amount") != "150.00" {
		t.Fatalf("a bet can be reversed only once: %d %s", dup.Status, dup.Body)
	}
	reconcile(t, w)

	var entries, pages int
	cursor := ""
	for {
		path := "/wallets/" + w.ID + "/ledger?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		r := call(t, bases[1], "GET", path, token(t, "wallet-service"), nil, nil)
		if r.Status != 200 {
			t.Fatalf("ledger: %d %s", r.Status, r.Body)
		}
		var page struct {
			Entries    []json.RawMessage `json:"entries"`
			NextCursor string            `json:"nextCursor"`
		}
		if err := json.Unmarshal(r.Body, &page); err != nil {
			t.Fatal(err)
		}
		entries += len(page.Entries)
		pages++
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if entries != 4 || pages != 2 {
		t.Fatalf("expected opening, debit, refund credit and win credit over 2 pages, got %d entries in %d pages", entries, pages)
	}
	assertEventsDelivered(t, w, 90*time.Second)
	types := map[string]int{}
	for id := range events.forWallet(w.ID) {
		types[events.typeOf(id)]++
	}
	if types["WagerTransactionPendingReference"] != 1 || types["WagerTransactionRejected"] != 1 || types["WagerTransactionProcessed"] < 4 || types["WalletBalanceChanged"] != 4 {
		t.Fatalf("unexpected event mix: %v", types)
	}
}

func TestFiftyDeliveriesOfOneBetAcrossProcessesAndTransports(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	external := uid("dup")
	key := "provider-a:" + external
	command := w.command("provider-a", "BET", external, "10.00", "")
	sqsData := w.command("provider-a", "BET", external, "10.00", "")
	sqsData["idempotencyKey"] = key
	bearer := token(t, "provider-a")

	const n = 25
	messageIDs := make([]string, n)
	responses := make([]response, n)
	var failures []string
	var mu sync.Mutex
	fail := func(format string, args ...any) {
		mu.Lock()
		failures = append(failures, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		messageIDs[i] = uuid.NewString()
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			r, err := wagerErr(bases[i%3], "provider-a", bearer, command, key)
			if err != nil {
				fail("http %d: %v", i, err)
				return
			}
			responses[i] = r
		}()
		go func() {
			defer wg.Done()
			<-start
			// A fresh deduplication ID per copy defeats the FIFO filter, so every copy reaches the consumer.
			if err := publishErr("provider-a", envelope(messageIDs[i], sqsData), w.ID, publishOptions{Token: bearer, Dedup: uuid.NewString()}); err != nil {
				fail("sqs %d: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(failures) > 0 {
		t.Fatalf("delivery failures: %v", failures)
	}
	first := ""
	for i, r := range responses {
		if r.Status != 200 || r.str("status") != "PROCESSED" {
			t.Fatalf("http copy %d: %d %s", i, r.Status, r.Body)
		}
		if first == "" {
			first = r.str("transactionId")
		}
		if r.str("transactionId") != first {
			t.Fatalf("copies resolved to different transactions: %s vs %s", r.str("transactionId"), first)
		}
	}
	wait(t, 120*time.Second, "all 25 SQS copies to be handled by the consumers", func() bool {
		return queryInt(t, "SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1) AND disposition='ACK'", messageIDs) == n
	})
	if got := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id=$1", external); got != 1 {
		t.Fatalf("expected one transaction, got %d", got)
	}
	if got := queryInt(t, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1", w.ID); got != 2 {
		t.Fatalf("expected the opening entry plus one debit, got %d", got)
	}
	if got := balanceMinor(t, w); got != "90.00" {
		t.Fatalf("the bet must be debited once: %s", got)
	}
	reconcile(t, w)
}

func publishErr(provider string, body any, group string, opts publishOptions) error {
	raw, ok := body.(string)
	if !ok {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		raw = string(b)
	}
	in := newSendInput(raw, group, opts)
	_, err := providerQ.SendMessage(context.Background(), in)
	return err
}

func TestTwoEightyBetsOnAHundredWallet(t *testing.T) {
	ensureInstancesReady(t)
	bearer := token(t, "provider-a")
	for round := 0; round < 5; round++ {
		w := newWallet(t, "100.00")
		var res [2]response
		var errs [2]error
		var commands [2]map[string]any
		var keys [2]string
		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := 0; i < 2; i++ {
			external := uid("eighty")
			commands[i], keys[i] = w.command("provider-a", "BET", external, "80.00", ""), "k-"+external
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				res[i], errs[i] = wagerErr(bases[i], "provider-a", bearer, commands[i], keys[i])
			}()
		}
		close(start)
		wg.Wait()
		processed, rejected := 0, 0
		for i := range res {
			if errs[i] != nil {
				t.Fatal(errs[i])
			}
			switch {
			case res[i].Status == 200 && res[i].str("status") == "PROCESSED":
				processed++
			case res[i].Status == 422 && res[i].str("failureCode") == "INSUFFICIENT_FUNDS":
				rejected++
			default:
				t.Fatalf("round %d: unexpected %d %s", round, res[i].Status, res[i].Body)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("round %d: processed=%d rejected=%d", round, processed, rejected)
		}
		if got := balanceMinor(t, w); got != "20.00" {
			t.Fatalf("round %d: final balance %s", round, got)
		}
		if n := queryInt(t, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1", w.ID); n != 2 {
			t.Fatalf("round %d: the opening credit and exactly one bet must be in the ledger, got %d entries", round, n)
		}
		// Retrying either request, on a different instance, replays its recorded result and moves nothing.
		for i := range res {
			again, err := wagerErr(bases[(i+1)%3], "provider-a", bearer, commands[i], keys[i])
			if err != nil || again.Status != res[i].Status || again.str("status") != res[i].str("status") || again.str("transactionId") != res[i].str("transactionId") || again.field("idempotentReplay") != true {
				t.Fatalf("round %d: a retry must replay the original result: first %d %s, retry %v %d %s", round, res[i].Status, res[i].Body, err, again.Status, again.Body)
			}
		}
		if got := balanceMinor(t, w); got != "20.00" {
			t.Fatalf("round %d: the retries moved money: %s", round, got)
		}
		reconcile(t, w)
	}
}

func TestLockedWalletDoesNotBlockOtherWallets(t *testing.T) {
	ensureInstancesReady(t)
	locked, free := newWallet(t, "100.00"), newWallet(t, "100.00")
	bearer := token(t, "provider-a")
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = tx.Rollback(ctx)
		}
	}
	defer release()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE", locked.ID); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan response, 1)
	go func() {
		external := uid("locked")
		r, _ := wagerErr(bases[0], "provider-a", bearer, locked.command("provider-a", "BET", external, "10.00", ""), "k-"+external)
		blocked <- r
	}()
	began := time.Now()
	external := uid("free")
	r, err := wagerErr(bases[1], "provider-a", bearer, free.command("provider-a", "BET", external, "10.00", ""), "k-"+external)
	if err != nil || r.Status != 200 || time.Since(began) > 4*time.Second {
		t.Fatalf("a bet on another wallet must not wait for the lock: %v %d %s after %s", err, r.Status, r.Body, time.Since(began))
	}
	select {
	case r := <-blocked:
		t.Fatalf("the bet on the locked wallet finished while its lock was held: %d %s", r.Status, r.Body)
	case <-time.After(1500 * time.Millisecond):
	}
	release()
	select {
	case r := <-blocked:
		if r.Status != 200 {
			t.Fatalf("after the lock is released: %d %s", r.Status, r.Body)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the bet on the formerly locked wallet never completed")
	}
	reconcile(t, locked)
	reconcile(t, free)
}

func TestUntrustedSQSMessagesHaveNoEffectAndReachTheDLQ(t *testing.T) {
	ensureInstancesReady(t)
	setInputVisibility(t, 3)
	w := newWallet(t, "100.00")
	marker := uuid.NewString()[:8]
	data := func(name string) map[string]any {
		d := w.command("provider-a", "BET", uid(name), "10.00", "")
		d["idempotencyKey"] = "provider-a:" + uid(name)
		return d
	}
	foreignToken := token(t, "provider-b")
	type attempt struct {
		name, marker, body, group string
		opts                      publishOptions
	}
	var attempts []attempt
	add := func(name string, body any, group string, opts publishOptions) {
		raw, _ := body.(string)
		if raw == "" {
			b, _ := json.Marshal(body)
			raw = string(b)
		}
		attempts = append(attempts, attempt{name: name, marker: marker + "-" + name, body: raw, group: group, opts: opts})
	}
	id := func(name string) string { return marker + "-" + name }
	add("no-authorization", envelope(id("no-authorization"), data("a")), w.ID, publishOptions{NoAuthorization: true})
	add("forged-token", envelope(id("forged-token"), data("b")), w.ID, publishOptions{Token: "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"})
	add("other-providers-token", envelope(id("other-providers-token"), data("c")), w.ID, publishOptions{Token: foreignToken})
	add("malformed-body", `{"messageId":"`+id("malformed-body")+`"`, w.ID, publishOptions{})
	add("group-mismatch", envelope(id("group-mismatch"), data("d")), uuid.NewString(), publishOptions{})
	extra := envelope(id("unknown-field"), data("e"))
	extra["data"].(map[string]any)["bonus"] = "free"
	add("unknown-field", extra, w.ID, publishOptions{})
	negative := envelope(id("negative-amount"), data("f"))
	negative["data"].(map[string]any)["money"] = map[string]string{"amount": "-10.00", "currency": "BRL"}
	add("negative-amount", negative, w.ID, publishOptions{})

	for _, a := range attempts {
		publish(t, "provider-a", a.body, a.group, a.opts)
	}
	found := map[string]bool{}
	wait(t, 240*time.Second, "all untrusted messages to be moved to the DLQ by the broker's redrive policy", func() bool {
		out, err := operatorQ.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(inputDLQ), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false
		}
		for _, m := range out.Messages {
			body := aws.ToString(m.Body)
			for _, a := range attempts {
				if strings.Contains(body, a.marker) {
					found[a.name] = true
				}
			}
			_, _ = operatorQ.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(inputDLQ), ReceiptHandle: m.ReceiptHandle})
		}
		return len(found) == len(attempts)
	})
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("untrusted messages moved money: %s", got)
	}
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL'", w.ID); n != 0 {
		t.Fatalf("untrusted messages created %d transactions", n)
	}
	var ids []string
	for _, a := range attempts {
		ids = append(ids, a.marker)
	}
	if n := queryInt(t, "SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1)", ids); n != 0 {
		t.Fatalf("untrusted messages were recorded in the inbox: %d", n)
	}
}

// publishedAndProcessed sends count BET messages of 1.00 per wallet and returns their external IDs.
func sendBets(t *testing.T, wallets []wallet, perWallet int) []string {
	t.Helper()
	var externals []string
	for _, w := range wallets {
		for i := 0; i < perWallet; i++ {
			external := uid("crash")
			data := w.command("provider-a", "BET", external, "1.00", "")
			data["idempotencyKey"] = "provider-a:" + external
			publish(t, "provider-a", envelope(uuid.NewString(), data), w.ID, publishOptions{})
			externals = append(externals, external)
		}
	}
	return externals
}

func assertExactlyOnce(t *testing.T, wallets []wallet, perWallet int, externals []string) {
	t.Helper()
	wait(t, 240*time.Second, "every message to be processed after the failure", func() bool {
		return queryInt(t, "SELECT count(*) FROM wager_transactions WHERE provider_id='provider-a' AND status='PROCESSED' AND external_transaction_id = ANY($1)", externals) == len(externals)
	})
	for _, w := range wallets {
		if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL'", w.ID); n != perWallet {
			t.Fatalf("wallet %s has %d transactions, want %d", w.ID, n, perWallet)
		}
		want := fmt.Sprintf("%d.00", 100-perWallet)
		if got := balanceMinor(t, w); got != want {
			t.Fatalf("wallet %s balance %s, want %s", w.ID, got, want)
		}
		if n := queryInt(t, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1", w.ID); n != perWallet+1 {
			t.Fatalf("wallet %s has %d ledger entries, want %d", w.ID, n, perWallet+1)
		}
		reconcile(t, w)
	}
}

func TestRecoveryAfterAnInstanceCrashes(t *testing.T) {
	ensureInstancesReady(t)
	setInputVisibility(t, 5)
	wallets := []wallet{newWallet(t, "100.00"), newWallet(t, "100.00"), newWallet(t, "100.00"), newWallet(t, "100.00")}
	externals := sendBets(t, wallets, 10)
	// `stop -t 0` is an immediate SIGKILL that the restart policy does not undo, so the instance stays down.
	docker(t, "stop", "-t", "0", container("app-2"))
	t.Cleanup(func() { ensureInstancesReady(t) })
	assertExactlyOnce(t, wallets, 10, externals)
	ensureInstancesReady(t)
}

func TestRecoveryAfterEveryInstanceRestarts(t *testing.T) {
	ensureInstancesReady(t)
	setInputVisibility(t, 5)
	wallets := []wallet{newWallet(t, "100.00"), newWallet(t, "100.00")}
	// An operation completed over HTTP before the restart must still replay afterwards.
	replayWallet := newWallet(t, "100.00")
	replayExternal := uid("before-restart")
	replayCommand := replayWallet.command("provider-a", "BET", replayExternal, "30.00", "")
	original := postWager(t, bases[0], "provider-a", replayCommand, "k-"+replayExternal)
	if original.Status != 200 || original.field("idempotentReplay") != false {
		t.Fatalf("original: %d %s", original.Status, original.Body)
	}
	// A reversal that arrived before its bet is persisted as pending and must survive the restart.
	pendingWallet := newWallet(t, "100.00")
	lateBet, earlyRefund := uid("late-bet"), uid("early-refund")
	pending := postWager(t, bases[1], "provider-a", pendingWallet.command("provider-a", "REFUND", earlyRefund, "20.00", lateBet), "k-"+earlyRefund)
	if pending.Status != 202 || pending.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("the refund must wait for its bet: %d %s", pending.Status, pending.Body)
	}
	externals := sendBets(t, wallets, 10)
	for _, name := range instances {
		docker(t, "stop", "-t", "0", container(name))
	}
	t.Cleanup(func() { ensureInstancesReady(t) })
	for i := range bases {
		if ready(bases[i]) {
			t.Fatalf("%s should be down", instances[i])
		}
	}
	ensureInstancesReady(t)
	assertExactlyOnce(t, wallets, 10, externals)
	// The bet arrives only now; a worker of a restarted instance completes the pending refund exactly once.
	if bet := postWager(t, bases[0], "provider-a", pendingWallet.command("provider-a", "BET", lateBet, "20.00", ""), "k-"+lateBet); bet.Status != 200 {
		t.Fatalf("the bet must be processed after the restart: %d %s", bet.Status, bet.Body)
	}
	wait(t, 90*time.Second, "the pending refund to complete after the restart", func() bool { return txStatus("provider-a", earlyRefund) == "PROCESSED" })
	if got := balanceMinor(t, pendingWallet); got != "100.00" {
		t.Fatalf("bet and refund must net to the opening balance: %s", got)
	}
	reconcile(t, pendingWallet)
	replay := postWager(t, bases[2], "provider-a", replayCommand, "k-"+replayExternal)
	if replay.Status != 200 || replay.field("idempotentReplay") != true || replay.str("transactionId") != original.str("transactionId") || replay.str("balance", "amount") != "70.00" {
		t.Fatalf("a replay after a total restart must return the stored result: %d %s", replay.Status, replay.Body)
	}
	if got := balanceMinor(t, replayWallet); got != "70.00" {
		t.Fatalf("the replay must not debit again: %s", got)
	}
}

func TestDatabaseOutageIsReportedAsRetryableAndLeavesNothingBehind(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	bearer := token(t, "provider-a") // cached before the outage: Keycloak shares this database
	database := container("postgres")
	docker(t, "pause", database)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			docker(t, "unpause", database)
		}
	}
	t.Cleanup(func() { ensureInstancesReady(t) })
	t.Cleanup(unpause)

	external := uid("dbdown")
	command := w.command("provider-a", "BET", external, "1.00", "")
	r, err := wagerErr(bases[0], "provider-a", bearer, command, "k-"+external)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != 503 || r.field("error", "retryable") != true || r.str("error", "code") != "TEMPORARILY_UNAVAILABLE" || r.Header.Get("Retry-After") == "" {
		t.Fatalf("a database outage must be a retryable 503, not a business answer: %d %s", r.Status, r.Body)
	}
	if ready, err := callErr(bases[1], "GET", "/health/ready", "", nil, nil); err != nil || ready.Status != 503 {
		t.Fatalf("readiness must report the outage: %v %+v", err, ready.Status)
	}
	if live, err := callErr(bases[1], "GET", "/health/live", "", nil, nil); err != nil || live.Status != 200 {
		t.Fatalf("liveness must not depend on the database: %v %+v", err, live.Status)
	}
	unpause()
	ensureInstancesReady(t)
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1", external); n != 0 {
		t.Fatalf("the failed attempt left %d transactions", n)
	}
	retry := postWager(t, bases[2], "provider-a", command, "k-"+external)
	if retry.Status != 200 || retry.str("status") != "PROCESSED" || retry.field("idempotentReplay") != false || retry.str("balance", "amount") != "99.00" {
		t.Fatalf("retrying with the same identity must apply the bet once: %d %s", retry.Status, retry.Body)
	}
	reconcile(t, w)
}

func TestEventsSurviveABrokerOutage(t *testing.T) {
	ensureInstancesReady(t)
	broker := container("broker")
	docker(t, "pause", broker)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			docker(t, "unpause", broker)
		}
	}
	t.Cleanup(unpause)

	w := newWallet(t, "100.00") // database only: succeeds while the broker is down
	bearer := token(t, "provider-a")
	for i := 0; i < 10; i++ {
		external := uid("outage")
		r, err := wagerErr(bases[i%3], "provider-a", bearer, w.command("provider-a", "BET", external, "1.00", ""), "k-"+external)
		if err != nil || r.Status != 200 || r.str("status") != "PROCESSED" {
			t.Fatalf("a wager must still commit while the broker is down: %v %d %s", err, r.Status, r.Body)
		}
	}
	for i := range bases {
		i := i
		wait(t, 60*time.Second, instances[i]+" to report not ready", func() bool {
			r, err := callErr(bases[i], "GET", "/health/ready", "", nil, nil)
			return err == nil && r.Status == 503
		})
	}
	if n := queryInt(t, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND published_at IS NULL", w.ID); n != 22 {
		t.Fatalf("every event must wait in the outbox during the outage, got %d unpublished", n)
	}
	if got := balanceMinor(t, w); got != "90.00" {
		t.Fatalf("balance during the outage: %s", got)
	}
	// The backlog must be visible to operators while it builds up.
	wait(t, 60*time.Second, "the outbox backlog gauges to report the stuck events", func() bool {
		return metric(t, liveBase(t), "outbox_pending_events") >= 22 && metric(t, liveBase(t), "outbox_oldest_pending_age_seconds") > 0
	})
	unpause()
	assertEventsDelivered(t, w, 240*time.Second)
	ensureInstancesReady(t)
	wait(t, 90*time.Second, "the backlog gauge to drain after recovery", func() bool {
		return metric(t, liveBase(t), "outbox_pending_events") == 0
	})
	reconcile(t, w)
}

func TestSQSOriginatedEventsCarryTheMessageAsCorrelation(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	external := uid("corr")
	messageID := "msg-" + uuid.NewString()
	data := w.command("provider-a", "BET", external, "5.00", "")
	data["idempotencyKey"] = "provider-a:" + external
	publish(t, "provider-a", envelope(messageID, data), w.ID, publishOptions{})
	wait(t, 60*time.Second, "the message to be processed", func() bool { return txStatus("provider-a", external) == "PROCESSED" })
	var transaction string
	if err := pool.QueryRow(context.Background(), "SELECT id::text FROM wager_transactions WHERE provider_id='provider-a' AND external_transaction_id=$1", external).Scan(&transaction); err != nil {
		t.Fatal(err)
	}
	if n := queryInt(t, "SELECT count(*) FROM outbox_events WHERE transaction_id=$1 AND payload->>'correlationId'=$2 AND payload->>'causationId'=$2", transaction, messageID); n != 2 {
		t.Fatalf("both events must be correlated and caused by the message, found %d", n)
	}
	assertEventsDelivered(t, w, 90*time.Second)
}

func TestDLQDepthIsExportedAsAMetric(t *testing.T) {
	ensureInstancesReady(t)
	// The DLQ test above leaves the queue drained; here we only require that the gauge is wired and sane.
	wait(t, 30*time.Second, "the DLQ gauge to be exported", func() bool { return metric(t, liveBase(t), "sqs_dlq_messages") >= 0 })
}

func TestGracefulShutdownKeepsExactlyOnceSemantics(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "1000.00")
	bearer := token(t, "provider-a")
	const total = 40
	externals := make([]string, total)
	statuses := make([]int, total)
	for i := range externals {
		externals[i] = uid("drain")
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g; i < total; i += 4 {
				r, err := wagerErr(bases[2], "provider-a", bearer, w.command("provider-a", "BET", externals[i], "1.00", ""), "k-"+externals[i])
				if err == nil {
					statuses[i] = r.Status
				}
				time.Sleep(60 * time.Millisecond)
			}
		}()
	}
	time.Sleep(400 * time.Millisecond)
	docker(t, "stop", "-t", "45", container("app-3"))
	wg.Wait()
	if code := docker(t, "inspect", "-f", "{{.State.ExitCode}}", container("app-3")); code != "0" {
		t.Fatalf("the instance must exit cleanly on SIGTERM, exit code %s", code)
	}
	t.Cleanup(func() { ensureInstancesReady(t) })
	// Any request without a clean 200 is retried with the same identity on a surviving instance.
	for i, status := range statuses {
		if status == 200 {
			continue
		}
		r := postWager(t, bases[0], "provider-a", w.command("provider-a", "BET", externals[i], "1.00", ""), "k-"+externals[i])
		if r.Status != 200 || r.str("status") != "PROCESSED" {
			t.Fatalf("retry of %s: %d %s", externals[i], r.Status, r.Body)
		}
	}
	if got := balanceMinor(t, w); got != "960.00" {
		t.Fatalf("every bet must be applied exactly once, balance %s", got)
	}
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL'", w.ID); n != total {
		t.Fatalf("expected %d transactions, got %d", total, n)
	}
	reconcile(t, w)
	ensureInstancesReady(t)
}

// Holding the wallet row lock pins consumers inside their transactions (inbox row inserted, wallet
// not yet locked). Killing every instance at that moment is a crash between receive and commit.
func TestCrashInsideTheTransactionLeavesNoPartialStateAndLosesNothing(t *testing.T) {
	ensureInstancesReady(t)
	setInputVisibility(t, 5)
	w := newWallet(t, "100.00")
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = tx.Rollback(ctx)
		}
	}
	defer release()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE", w.ID); err != nil {
		t.Fatal(err)
	}
	const n = 5
	messageIDs := make([]string, n)
	externals := make([]string, n)
	for i := range messageIDs {
		messageIDs[i], externals[i] = uuid.NewString(), uid("inflight")
		data := w.command("provider-a", "BET", externals[i], "1.00", "")
		data["idempotencyKey"] = "provider-a:" + externals[i]
		publish(t, "provider-a", envelope(messageIDs[i], data), w.ID, publishOptions{})
	}
	waitForBlockedConsumer(t, w)
	for _, name := range instances {
		docker(t, "stop", "-t", "0", container(name))
	}
	t.Cleanup(func() { ensureInstancesReady(t) })
	release()
	wait(t, 30*time.Second, "the killed instances' transactions to be rolled back", func() bool {
		return queryInt(t, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()") == 0
	})
	if got := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL'", w.ID); got != 0 {
		t.Fatalf("a crash before commit must leave no transaction behind, found %d", got)
	}
	if got := queryInt(t, "SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1)", messageIDs); got != 0 {
		t.Fatalf("a crash before commit must leave no inbox rows behind, found %d", got)
	}
	ensureInstancesReady(t)
	assertExactlyOnce(t, []wallet{w}, n, externals)
	if got := queryInt(t, "SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1) AND disposition='ACK'", messageIDs); got != n {
		t.Fatalf("every redelivered message must end acknowledged in the inbox, got %d", got)
	}
}

// With the broker frozen, consumers can commit but never acknowledge. Killing them then is a crash
// between commit and ACK: the redelivered messages must be recognised by the inbox.
func TestCrashBetweenCommitAndAckCausesNoSecondEffect(t *testing.T) {
	ensureInstancesReady(t)
	setInputVisibility(t, 5)
	w := newWallet(t, "100.00")
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			_ = tx.Rollback(ctx)
		}
	}
	defer release()
	if _, err := tx.Exec(ctx, "SELECT 1 FROM wallets WHERE id=$1 FOR UPDATE", w.ID); err != nil {
		t.Fatal(err)
	}
	const n = 5
	messageIDs := make([]string, n)
	externals := make([]string, n)
	for i := range messageIDs {
		messageIDs[i], externals[i] = uuid.NewString(), uid("noack")
		data := w.command("provider-a", "BET", externals[i], "1.00", "")
		data["idempotencyKey"] = "provider-a:" + externals[i]
		publish(t, "provider-a", envelope(messageIDs[i], data), w.ID, publishOptions{})
	}
	waitForBlockedConsumer(t, w)
	broker := container("broker")
	docker(t, "pause", broker)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			docker(t, "unpause", broker)
		}
	}
	// Cleanups run last-in first-out: the broker must be back before instances can become ready.
	t.Cleanup(func() { ensureInstancesReady(t) })
	t.Cleanup(unpause)
	began := time.Now().Add(-time.Minute)
	release()
	if !tryWait(90*time.Second, func() bool {
		return queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL' AND status='PROCESSED'", w.ID) >= 1
	}) {
		dumpAppLogs(t, began)
		t.Fatalf("consumers did not commit while the broker was frozen (blocked backends: %d)",
			queryInt(t, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'"))
	}
	for _, name := range instances {
		docker(t, "stop", "-t", "0", container(name))
	}
	committed := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL'", w.ID)
	if committed < 1 || committed > n {
		t.Fatalf("expected at least one commit before the crash (FIFO holds the rest of the group), got %d", committed)
	}
	unpause()
	ensureInstancesReady(t)
	assertExactlyOnce(t, []wallet{w}, n, externals)
	if got := queryInt(t, "SELECT count(*) FROM inbox_messages WHERE message_id = ANY($1) AND disposition='ACK'", messageIDs); got != n {
		t.Fatalf("every message must end acknowledged in the inbox, got %d", got)
	}
	t.Logf("%d of %d messages were committed before the crash; all %d applied exactly once", committed, n, n)
}

// FAILED is a permanent failure recorded for audit. The one case the system can establish by itself is a wallet
// that can no longer advance its version counter; the test reaches it by editing the counter as the database
// owner and then drives the real stack: HTTP answers 500 with the recorded failure, a retry replays it on any
// instance, a queue delivery is dead-lettered by the broker, and nothing moves.
func TestPermanentFailureIsRecordedReturnedAndDeadLettered(t *testing.T) {
	ensureInstancesReady(t)
	owner := ownerPool(t)
	w := newWallet(t, "100.00")
	setWalletVersion(t, owner, w.ID, math.MaxInt64)
	t.Cleanup(func() { setWalletVersion(t, owner, w.ID, 1) })
	failedBefore := labeledMetric(t, "wager_transactions_total", `status="FAILED"`)

	bearer := token(t, "provider-a")
	external := uid("failed")
	command := w.command("provider-a", "BET", external, "10.00", "")
	key := "k-" + external
	first, err := wagerErr(bases[0], "provider-a", bearer, command, key)
	if err != nil || first.Status != 500 || first.str("status") != "FAILED" || first.str("failureCode") != "WALLET_VERSION_EXHAUSTED" {
		t.Fatalf("a movement the wallet cannot record must answer 500 FAILED: %v %d %s", err, first.Status, first.Body)
	}
	if first.str("balance", "amount") != "100.00" {
		t.Fatalf("the failure must report the untouched balance: %s", first.Body)
	}
	replay, err := wagerErr(bases[1], "provider-a", bearer, command, key)
	if err != nil || replay.Status != 500 || replay.str("status") != "FAILED" || replay.str("transactionId") != first.str("transactionId") || replay.field("idempotentReplay") != true {
		t.Fatalf("a retry on another instance must replay the recorded failure: %v %d %s", err, replay.Status, replay.Body)
	}
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE wallet_id=$1 AND origin='EXTERNAL' AND status='FAILED' AND failure_code='WALLET_VERSION_EXHAUSTED'", w.ID); n != 1 {
		t.Fatalf("exactly one audited FAILED transaction expected, got %d", n)
	}
	if n := queryInt(t, "SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id=$1 AND wallet_version > 1", w.ID); n != 0 {
		t.Fatalf("a failed transaction must not move money: %d entries", n)
	}
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("balance %s", got)
	}
	stored := call(t, bases[2], "GET", "/wagering/transactions/"+first.str("transactionId"), bearer, nil, nil)
	if stored.Status != 200 || stored.str("status") != "FAILED" {
		t.Fatalf("the recorded failure must be readable: %d %s", stored.Status, stored.Body)
	}

	// The same condition through the queue: the failure is recorded and the message is never acknowledged, so the
	// broker's redrive policy dead-letters it.
	external = uid("failed-sqs")
	messageID := "msg-" + uuid.NewString()
	data := w.command("provider-a", "WIN", external, "5.00", "")
	data["idempotencyKey"] = "provider-a:" + external
	publish(t, "provider-a", envelope(messageID, data), w.ID, publishOptions{})
	wait(t, 60*time.Second, "the delivery to be recorded as FAILED", func() bool { return txStatus("provider-a", external) == "FAILED" })
	found := false
	wait(t, 180*time.Second, "the failed message to reach the dead-letter queue", func() bool {
		out, err := operatorQ.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(inputDLQ), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), messageID) {
				found = true
			}
			_, _ = operatorQ.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(inputDLQ), ReceiptHandle: m.ReceiptHandle})
		}
		return found
	})
	var disposition string
	if err := pool.QueryRow(context.Background(), "SELECT disposition FROM inbox_messages WHERE message_id=$1", messageID).Scan(&disposition); err != nil || disposition != "DLQ" {
		t.Fatalf("the inbox must record that the message was dead-lettered: %q %v", disposition, err)
	}
	if n := queryInt(t, "SELECT count(*) FROM outbox_events WHERE wallet_id=$1 AND transaction_id IN (SELECT id FROM wager_transactions WHERE wallet_id=$1 AND status='FAILED')", w.ID); n != 0 {
		t.Fatalf("a failed transaction emits no integration event, got %d", n)
	}
	if got := labeledMetric(t, "wager_transactions_total", `status="FAILED"`); got < failedBefore+2 {
		t.Fatalf("the FAILED counter must reflect both failures: %v -> %v", failedBefore, got)
	}

	// Restore the counter and prove the wallet is healthy again and still consistent.
	setWalletVersion(t, owner, w.ID, 1)
	reconcile(t, w)
}

// keycloakAdmin returns an admin token of the realm master, used only to shorten the access-token lifespan.
func keycloakAdmin(t *testing.T) string {
	t.Helper()
	endpoint := strings.Replace(tokenURL, "/realms/jungle/", "/realms/master/", 1)
	resp, err := http.PostForm(endpoint, url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {"admin"}, "password": {"local-admin-password"}})
	if err != nil {
		t.Fatalf("admin token: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("admin token: status %d err %v", resp.StatusCode, err)
	}
	return body.AccessToken
}

// setAccessTokenLifespan changes how long the real identity provider's access tokens live and returns the
// previous value (seconds) so the caller can restore it.
func setAccessTokenLifespan(t *testing.T, seconds int) int {
	t.Helper()
	admin := keycloakAdmin(t)
	realm := strings.Replace(tokenURL, "/realms/jungle/protocol/openid-connect/token", "", 1)
	endpoint := realm + "/admin/realms/jungle"
	do := func(method string, body any) (int, []byte) {
		var reader io.Reader
		if body != nil {
			raw, _ := json.Marshal(body)
			reader = bytes.NewReader(raw)
		}
		req, _ := http.NewRequest(method, endpoint, reader)
		req.Header.Set("Authorization", "Bearer "+admin)
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("keycloak admin %s: %v", method, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	status, raw := do("GET", nil)
	var current struct {
		AccessTokenLifespan int `json:"accessTokenLifespan"`
	}
	if status != 200 || json.Unmarshal(raw, &current) != nil || current.AccessTokenLifespan <= 0 {
		t.Fatalf("read realm: %d %s", status, raw)
	}
	if status, raw := do("PUT", map[string]any{"accessTokenLifespan": seconds}); status != 204 {
		t.Fatalf("update realm: %d %s", status, raw)
	}
	return current.AccessTokenLifespan
}

// An expired credential from the real identity provider must be refused on both transports: the HTTP API answers
// 401, and a queue message carrying it has no effect and is dead-lettered. (Unit tests cover expiry with generated
// keys; this proves it with tokens Keycloak itself issued and let expire.)
func TestExpiredTokensFromTheRealIdentityProviderAreRefused(t *testing.T) {
	ensureInstancesReady(t)
	w := newWallet(t, "100.00")
	original := setAccessTokenLifespan(t, 3)
	t.Cleanup(func() {
		setAccessTokenLifespan(t, original)
		tokenMu.Lock()
		tokenCache = map[string]struct {
			value   string
			expires time.Time
		}{}
		tokenMu.Unlock()
	})
	resp, err := http.PostForm(tokenURL, url.Values{"grant_type": {"client_credentials"}, "client_id": {"provider-a"}, "client_secret": {secrets["provider-a"]}})
	if err != nil {
		t.Fatal(err)
	}
	var issued struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	err = json.NewDecoder(resp.Body).Decode(&issued)
	resp.Body.Close()
	if err != nil || issued.AccessToken == "" || issued.ExpiresIn > 5 {
		t.Fatalf("the identity provider must issue a short-lived token: %+v %v", issued, err)
	}
	path := "/providers/provider-a/wagering/transactions/" + uid("unknown")
	if r := call(t, bases[0], "GET", path, issued.AccessToken, nil, nil); r.Status != 404 {
		t.Fatalf("a fresh token must authenticate (404 for an unknown transaction), got %d %s", r.Status, r.Body)
	}
	time.Sleep(time.Duration(issued.ExpiresIn+2) * time.Second)
	for i, base := range bases {
		r := call(t, base, "GET", path, issued.AccessToken, nil, nil)
		if r.Status != 401 || r.str("error", "code") != "UNAUTHENTICATED" {
			t.Fatalf("instance %d must refuse the expired token with 401 UNAUTHENTICATED, got %d %s", i+1, r.Status, r.Body)
		}
	}

	// The same expired credential in a queue message.
	external := uid("expired")
	messageID := "msg-" + uuid.NewString()
	data := w.command("provider-a", "BET", external, "10.00", "")
	data["idempotencyKey"] = "provider-a:" + external
	publish(t, "provider-a", envelope(messageID, data), w.ID, publishOptions{Token: issued.AccessToken})
	found := false
	wait(t, 120*time.Second, "the message with the expired token to be dead-lettered", func() bool {
		out, err := operatorQ.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(inputDLQ), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			return false
		}
		for _, m := range out.Messages {
			if strings.Contains(aws.ToString(m.Body), messageID) {
				found = true
			}
			_, _ = operatorQ.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(inputDLQ), ReceiptHandle: m.ReceiptHandle})
		}
		return found
	})
	if got := balanceMinor(t, w); got != "100.00" {
		t.Fatalf("an expired credential moved money: %s", got)
	}
	if n := queryInt(t, "SELECT count(*) FROM wager_transactions WHERE external_transaction_id=$1", external); n != 0 {
		t.Fatalf("an expired credential created %d transactions", n)
	}
}
