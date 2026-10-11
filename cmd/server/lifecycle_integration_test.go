//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/httpapi"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// These tests start the real composition (every module of options) against a real PostgreSQL, with in-process
// substitutes for the two external systems that need a network of their own (the OIDC provider and the SQS
// broker), and prove that stopping the application releases everything it started.

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// fakeBroker answers like an idle SQS queue and records what the application sends and how many calls are in
// flight, which is how the test sees workers that have not released the broker.
type fakeBroker struct {
	mu       sync.Mutex
	sent     []string
	calls    atomic.Int64
	inFlight atomic.Int64
}

func (f *fakeBroker) enter() func() {
	f.calls.Add(1)
	f.inFlight.Add(1)
	return func() { f.inFlight.Add(-1) }
}

func (f *fakeBroker) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	defer f.enter()()
	// A long poll that finds nothing: shortened so the test is quick, but still a call that outlives a
	// cancelled context the way a real in-flight poll does.
	time.Sleep(200 * time.Millisecond)
	return &sqs.ReceiveMessageOutput{}, nil
}

func (f *fakeBroker) DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	defer f.enter()()
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeBroker) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	defer f.enter()()
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeBroker) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	defer f.enter()()
	f.mu.Lock()
	f.sent = append(f.sent, *in.MessageBody)
	f.mu.Unlock()
	return &sqs.SendMessageOutput{MessageId: new(string)}, nil
}

func (f *fakeBroker) GetQueueAttributes(context.Context, *sqs.GetQueueAttributesInput, ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error) {
	defer f.enter()()
	return &sqs.GetQueueAttributesOutput{Attributes: map[string]string{"ApproximateNumberOfMessages": "0"}}, nil
}

func (f *fakeBroker) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// tokenVerifier maps the bearer token to a principal, the way the OIDC verifier maps client ids.
type tokenVerifier struct{}

func (tokenVerifier) Verify(_ context.Context, header string) (auth.Principal, error) {
	switch strings.TrimPrefix(header, "Bearer ") {
	case "internal":
		return auth.Principal{ClientID: "wallet-service", Internal: true, Roles: map[string]bool{"wallet:write": true, "wallet:read": true}}, nil
	case "provider-a":
		return auth.Principal{ClientID: "provider-a", ProviderID: "provider-a", Roles: map[string]bool{"wager:write": true, "wager:read": true}}, nil
	}
	return auth.Principal{}, auth.ErrUnauthenticated
}

// freshDatabase creates a database of its own and applies the migrations, so the publisher and the reference
// worker of the started application never touch rows of other tests. It returns the runtime role's DSN.
func freshDatabase(t *testing.T) string {
	t.Helper()
	ownerURL := os.Getenv("TEST_ADMIN_DATABASE_URL")
	if ownerURL == "" {
		ownerURL = "postgres://jungle:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	runtimeURL := os.Getenv("TEST_DATABASE_URL")
	if runtimeURL == "" {
		runtimeURL = "postgres://jungle_app:localpassword@127.0.0.1:15432/jungle_test?sslmode=disable"
	}
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	name := "lifecycle_" + hex.EncodeToString(random[:])
	ctx := context.Background()
	owner, err := pgxpool.New(ctx, ownerURL)
	if err != nil {
		t.Fatalf("owner connection (set TEST_ADMIN_DATABASE_URL): %v", err)
	}
	t.Cleanup(owner.Close)
	if _, err := owner.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	// Only the database this test created is dropped, once the application under test has stopped.
	t.Cleanup(func() { _, _ = owner.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })

	withDatabase := func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		u.Path = "/" + name
		return u.String()
	}
	migrated, err := pgxpool.New(ctx, withDatabase(ownerURL))
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	script, err := os.ReadFile(filepath.Join("..", "..", "migrations", "000001_init.up.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrated.Exec(ctx, string(script)); err != nil {
		t.Fatalf("migrate %s: %v", name, err)
	}
	return withDatabase(runtimeURL)
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type harness struct {
	app    *fx.App
	pool   *pgxpool.Pool
	broker *fakeBroker
	logs   *syncBuffer
	addr   string
}

func newHarness(t *testing.T, addr string) *harness {
	t.Helper()
	h := &harness{broker: &fakeBroker{}, logs: &syncBuffer{}, addr: addr}
	cfg := config.Config{
		HTTPAddr: addr, DatabaseURL: freshDatabase(t), Issuer: "http://idp.invalid", Audience: "jungle-backend", Region: "us-east-1",
		InputQueue: "http://broker.invalid/input.fifo", OutputQueue: "http://broker.invalid/output.fifo", InputDLQ: "http://broker.invalid/input-dlq.fifo",
		InternalClient: "wallet-service", ProviderClients: map[string]string{"provider-a": "provider-a"},
		ProcessingTimeout: 5 * time.Second, ShutdownTimeout: 10 * time.Second, ReferenceTTL: time.Hour,
	}
	logger := slog.New(slog.NewJSONHandler(h.logs, nil))
	h.app = fx.New(
		settings(cfg, logger),
		// The two external systems are replaced; every other module is the production one.
		fx.Provide(func() httpapi.TokenVerifier { return tokenVerifier{} }, func() Broker { return h.broker }),
		serviceModules,
		fx.Populate(&h.pool),
	)
	if err := h.app.Err(); err != nil {
		t.Fatalf("the graph must build: %v", err)
	}
	return h
}

func (h *harness) do(t *testing.T, method, path, token string, body any, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, "http://"+h.addr+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func eventually(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// goroutinesIn lists the goroutines whose stack mentions any of the markers.
func goroutinesIn(markers ...string) []string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var found []string
	for _, stack := range strings.Split(string(buf), "\n\n") {
		for _, marker := range markers {
			if strings.Contains(stack, marker) {
				first, _, _ := strings.Cut(stack, "\n")
				found = append(found, first+" "+marker)
				break
			}
		}
	}
	return found
}

var workerMarkers = []string{
	"main.supervise", "internal/messaging.(*Consumer)", "internal/messaging.(*Publisher)",
	"internal/workers.(*References)", "internal/workers.(*Observer)", "net/http.(*Server).Serve",
}

func assertReleased(t *testing.T, h *harness, workers ...string) {
	t.Helper()
	if err := h.pool.Ping(context.Background()); err == nil {
		t.Error("the database pool must be closed")
	}
	if n := h.broker.inFlight.Load(); n != 0 {
		t.Errorf("%d broker calls are still in flight", n)
	}
	calls := h.broker.calls.Load()
	time.Sleep(1500 * time.Millisecond) // longer than the supervisor's restart delay
	if after := h.broker.calls.Load(); after != calls {
		t.Errorf("the application kept calling the broker after it stopped (%d -> %d calls)", calls, after)
	}
	if left := goroutinesIn(workerMarkers...); len(left) != 0 {
		t.Errorf("worker or server goroutines survived the stop:\n%s", strings.Join(left, "\n"))
	}
	logs := h.logs.String()
	for _, name := range workers {
		if !strings.Contains(logs, fmt.Sprintf(`"msg":"worker stopped","worker":%q`, name)) {
			t.Errorf("worker %q did not report that it stopped", name)
		}
	}
}

func TestApplicationStartsServesAndReleasesEverythingOnStop(t *testing.T) {
	h := newHarness(t, freePort(t))
	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelStart()
	if err := h.app.Start(startCtx); err != nil {
		t.Fatalf("start: %v\n%s", err, h.logs.String())
	}
	stopped := false
	defer func() {
		if !stopped {
			_ = h.app.Stop(context.Background())
		}
	}()

	if status, _ := h.do(t, "GET", "/health/ready", "", nil, nil); status != 200 {
		t.Fatalf("a started application must be ready: %d", status)
	}
	// A wallet and a wager through HTTP; the publisher worker must carry their events to the broker.
	status, wallet := h.do(t, "POST", "/wallets", "internal", map[string]any{"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "100.00", "currency": "BRL"}}, nil)
	if status != 201 {
		t.Fatalf("create wallet: %d %v", status, wallet)
	}
	wager := map[string]any{"providerId": "provider-a", "externalTransactionId": "lifecycle-bet", "playerId": wallet["playerId"], "walletId": wallet["id"],
		"roundId": "r1", "gameId": "g1", "kind": "BET", "money": map[string]string{"amount": "10.00", "currency": "BRL"}}
	if status, body := h.do(t, "POST", "/wagering/transactions", "provider-a", wager, map[string]string{"Idempotency-Key": "lifecycle-key"}); status != 200 || body["status"] != "PROCESSED" {
		t.Fatalf("wager: %d %v", status, body)
	}
	eventually(t, 20*time.Second, "the publisher to send the wallet events", func() bool { return h.broker.sentCount() >= 4 })
	eventually(t, 10*time.Second, "the consumer to poll the input queue", func() bool { return h.broker.calls.Load() > 4 })
	if status, _ := h.do(t, "GET", "/wallets/"+fmt.Sprint(wallet["id"]), "provider-a", nil, nil); status != 403 {
		t.Fatalf("the verifier's roles must be enforced by the composition: %d", status)
	}
	if got := goroutinesIn("internal/messaging.(*Consumer)", "internal/messaging.(*Publisher)", "internal/workers.(*References)", "internal/workers.(*Observer)"); len(got) < 4 {
		t.Fatalf("all four workers must be running while the application is up, found %d", len(got))
	}

	began := time.Now()
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelStop()
	if err := h.app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v\n%s", err, h.logs.String())
	}
	stopped = true
	if took := time.Since(began); took > 10*time.Second {
		t.Errorf("the stop took %s", took)
	}
	if conn, err := net.DialTimeout("tcp", h.addr, time.Second); err == nil {
		conn.Close()
		t.Error("the HTTP port must be closed after the stop")
	}
	assertReleased(t, h, "consumer", "publisher", "references", "observer")
}

// A start that fails halfway must undo what the earlier modules acquired: here the port is taken, so the HTTP
// module fails after the pool and every worker are already running.
func TestAFailedStartReleasesWhatWasAlreadyAcquired(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	h := newHarness(t, taken.Addr().String())

	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = h.app.Start(startCtx)
	if err == nil {
		_ = h.app.Stop(context.Background())
		t.Fatal("the start must fail when the port is already in use")
	}
	if !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("the failure must name the port problem: %v", err)
	}
	assertReleased(t, h, "consumer", "publisher", "references", "observer")
}
