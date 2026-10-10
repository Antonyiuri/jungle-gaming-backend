//go:build e2e

// Package e2e drives the running Compose stack: three independent application instances share
// PostgreSQL, Keycloak and the SQS broker. Nothing here mocks a dependency.
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

var (
	tokenURL    = env("E2E_TOKEN_URL", "http://127.0.0.1:8081/realms/jungle/protocol/openid-connect/token")
	sqsEndpoint = env("E2E_SQS_ENDPOINT", "http://127.0.0.1:4566")
	dbURL       = env("E2E_DATABASE_URL", "postgres://jungle_app:localpassword@127.0.0.1:15432/jungle?sslmode=disable")
	runtimeDir  = env("E2E_RUNTIME_DIR", filepath.Join("..", "..", ".local"))
	project     = env("COMPOSE_PROJECT_NAME", "jungle-gaming")
	bases       = []string{env("E2E_API_1", "http://127.0.0.1:8080"), env("E2E_API_2", "http://127.0.0.1:8082"), env("E2E_API_3", "http://127.0.0.1:8083")}
	instances   = []string{"app", "app-2", "app-3"}
)

// One SQS identity per role, exactly as provisioned: a broker that enforces policies would deny a test that
// borrowed another role's credentials.
var (
	pool      *pgxpool.Pool
	providerQ *sqs.Client // provider-a: may only publish wager requests
	workerQ   *sqs.Client // jungle-worker: the application's own identity
	consumerQ *sqs.Client // events-consumer: downstream consumer of wallet-events.fifo
	operatorQ *sqs.Client // dlq-operator: drains the dead-letter queues
	adminQ    *sqs.Client // the emulator's account root, for queue administration done by the tests themselves
	events    = &sink{seen: map[string]map[string]int{}, types: map[string]string{}, wallets: map[string]string{}}

	// Queue URLs come from the provisioning output, so the suite works with any broker's URL scheme.
	inputQueue, inputDLQ, outputQueue, outputDLQ string
)

func adminClient() *sqs.Client {
	return sqs.New(sqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(sqsEndpoint),
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")})
}

// loadQueueURLs reads the URLs written by the provisioning (they name the broker by its Compose hostname)
// and points them at the endpoint published on the host.
func loadQueueURLs() error {
	raw, err := os.ReadFile(filepath.Join(runtimeDir, "queues.json"))
	if err != nil {
		return err
	}
	var q struct {
		Input     string `json:"input"`
		Output    string `json:"output"`
		InputDLQ  string `json:"inputDlq"`
		OutputDLQ string `json:"outputDlq"`
	}
	if err := json.Unmarshal(raw, &q); err != nil {
		return err
	}
	endpoint, err := url.Parse(sqsEndpoint)
	if err != nil {
		return err
	}
	local := func(raw string) (string, error) {
		u, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		u.Scheme, u.Host = endpoint.Scheme, endpoint.Host
		return u.String(), nil
	}
	if inputQueue, err = local(q.Input); err != nil {
		return err
	}
	if outputQueue, err = local(q.Output); err != nil {
		return err
	}
	if inputDLQ, err = local(q.InputDLQ); err != nil {
		return err
	}
	outputDLQ, err = local(q.OutputDLQ)
	return err
}

func clientFor(file string) (*sqs.Client, error) {
	raw, err := os.ReadFile(filepath.Join(runtimeDir, file))
	if err != nil {
		return nil, err
	}
	var c struct{ AccessKeyId, SecretAccessKey string }
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return sqs.New(sqs.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(sqsEndpoint),
		Credentials: credentials.NewStaticCredentialsProvider(c.AccessKeyId, c.SecretAccessKey, ""),
	}), nil
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	var err error
	if pool, err = pgxpool.New(ctx, dbURL); err != nil {
		fmt.Fprintln(os.Stderr, "database:", err)
		os.Exit(2)
	}
	if providerQ, err = clientFor("provider-a.json"); err != nil {
		fmt.Fprintln(os.Stderr, "provider credentials (run the stack first):", err)
		os.Exit(2)
	}
	for name, target := range map[string]**sqs.Client{"jungle-worker": &workerQ, "events-consumer": &consumerQ, "dlq-operator": &operatorQ} {
		if *target, err = clientFor(name + ".json"); err != nil {
			fmt.Fprintf(os.Stderr, "%s credentials (run the stack first, and re-run the provisioning if it predates this suite): %v\n", name, err)
			os.Exit(2)
		}
	}
	adminQ = adminClient()
	if err := loadQueueURLs(); err != nil {
		fmt.Fprintln(os.Stderr, "queue URLs (run the stack first):", err)
		os.Exit(2)
	}
	done := make(chan struct{})
	go func() { defer close(done); events.run(ctx) }()
	code := m.Run()
	cancel()
	<-done
	pool.Close()
	os.Exit(code)
}

// sink plays the downstream consumer of wallet-events.fifo: it records every delivery and
// deduplicates by eventId, exactly what the architecture requires of real consumers.
type sink struct {
	mu       sync.Mutex
	seen     map[string]map[string]int // walletId -> eventId -> deliveries
	types    map[string]string         // eventId -> eventType
	wallets  map[string]string
	reported map[string]bool
}

func (s *sink) run(ctx context.Context) {
	// Several receivers and batched deletes: one sequential consumer would be the bottleneck of the
	// event assertions, not the publishers under test.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.poll(ctx) }()
	}
	wg.Wait()
}

func (s *sink) poll(ctx context.Context) {
	for ctx.Err() == nil {
		out, err := consumerQ.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(outputQueue), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			s.report("receive", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, len(out.Messages))
		for i, m := range out.Messages {
			var e struct {
				EventID     string `json:"eventId"`
				EventType   string `json:"eventType"`
				AggregateID string `json:"aggregateId"`
			}
			if json.Unmarshal([]byte(aws.ToString(m.Body)), &e) == nil && e.EventID != "" {
				s.mu.Lock()
				if s.seen[e.AggregateID] == nil {
					s.seen[e.AggregateID] = map[string]int{}
				}
				s.seen[e.AggregateID][e.EventID]++
				s.types[e.EventID] = e.EventType
				s.mu.Unlock()
			}
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: m.ReceiptHandle})
		}
		if len(entries) > 0 {
			resp, err := consumerQ.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(outputQueue), Entries: entries})
			if err != nil {
				s.report("delete", err)
			} else if len(resp.Failed) > 0 {
				s.report("delete", fmt.Errorf("%d of %d entries failed: %s", len(resp.Failed), len(entries), aws.ToString(resp.Failed[0].Message)))
			}
		}
	}
}

// report prints each distinct sink problem once. A silent sink turns a broker or policy problem into a
// mysterious "events never arrived" timeout, which is exactly what it hid the first time it was run against a
// broker that enforces policies.
func (s *sink) report(what string, err error) {
	if err == nil || strings.Contains(err.Error(), "context canceled") {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := what + ": " + err.Error()
	if s.reported == nil {
		s.reported = map[string]bool{}
	}
	if !s.reported[key] {
		s.reported[key] = true
		fmt.Fprintf(os.Stderr, "event sink %s\n", key)
	}
}

func (s *sink) forWallet(wallet string) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int{}
	for id, n := range s.seen[wallet] {
		out[id] = n
	}
	return out
}

func (s *sink) typeOf(eventID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.types[eventID]
}

// ---- identities -------------------------------------------------------------------------------

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]struct {
		value   string
		expires time.Time
	}{}
	secrets = map[string]string{"provider-a": "local-provider-a-secret", "provider-b": "local-provider-b-secret", "wallet-service": "local-wallet-service-secret"}
)

func token(t testing.TB, client string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[client]; ok && time.Now().Before(c.expires) {
		return c.value
	}
	resp, err := http.PostForm(tokenURL, url.Values{"grant_type": {"client_credentials"}, "client_id": {client}, "client_secret": {secrets[client]}})
	if err != nil {
		t.Fatalf("token request for %s: %v", client, err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("token for %s: status %d err %v", client, resp.StatusCode, err)
	}
	tokenCache[client] = struct {
		value   string
		expires time.Time
	}{body.AccessToken, time.Now().Add(3 * time.Minute)}
	return body.AccessToken
}

// ---- HTTP -------------------------------------------------------------------------------------

type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r response) field(path ...string) any {
	var node any
	if json.Unmarshal(r.Body, &node) != nil {
		return nil
	}
	for _, p := range path {
		m, ok := node.(map[string]any)
		if !ok {
			return nil
		}
		node = m[p]
	}
	return node
}

func (r response) str(path ...string) string { s, _ := r.field(path...).(string); return s }

var httpClient = &http.Client{Timeout: 40 * time.Second}

func call(t testing.TB, base, method, path, bearer string, body any, headers map[string]string) response {
	t.Helper()
	r, err := callErr(base, method, path, bearer, body, headers)
	if err != nil {
		t.Fatalf("%s %s%s: %v", method, base, path, err)
	}
	return r
}

func callErr(base, method, path, bearer string, body any, headers map[string]string) (response, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return response{}, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, base+path, reader)
	if err != nil {
		return response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	return response{resp.StatusCode, resp.Header, raw}, err
}

// ---- domain helpers ---------------------------------------------------------------------------

type wallet struct{ ID, PlayerID string }

func newWallet(t testing.TB, amount string) wallet {
	t.Helper()
	player := uuid.NewString()
	r := call(t, liveBase(t), "POST", "/wallets", token(t, "wallet-service"), map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": amount, "currency": "BRL"}}, nil)
	if r.Status != 201 {
		t.Fatalf("create wallet: %d %s", r.Status, r.Body)
	}
	return wallet{ID: r.str("id"), PlayerID: r.str("playerId")}
}

func (w wallet) command(provider, kind, external, amount, reference string) map[string]any {
	c := map[string]any{"providerId": provider, "externalTransactionId": external, "playerId": w.PlayerID, "walletId": w.ID,
		"roundId": "round-1", "gameId": "game-1", "kind": kind, "money": map[string]string{"amount": amount, "currency": "BRL"}}
	if reference != "" {
		c["referenceExternalTransactionId"] = reference
	}
	return c
}

func uid(prefix string) string { return prefix + "-" + uuid.NewString()[:12] }

func postWager(t testing.TB, base, provider string, command map[string]any, key string) response {
	t.Helper()
	return call(t, base, "POST", "/wagering/transactions", token(t, provider), command, map[string]string{"Idempotency-Key": key})
}

func balanceMinor(t testing.TB, w wallet) string {
	t.Helper()
	r := call(t, liveBase(t), "GET", "/wallets/"+w.ID, token(t, "wallet-service"), nil, nil)
	if r.Status != 200 {
		t.Fatalf("get wallet: %d %s", r.Status, r.Body)
	}
	return r.str("balance", "amount")
}

func reconcile(t testing.TB, w wallet) {
	t.Helper()
	r := call(t, liveBase(t), "POST", "/wallets/"+w.ID+"/reconciliation", token(t, "wallet-service"), nil, nil)
	if r.Status != 200 || r.field("consistent") != true {
		t.Fatalf("wallet %s is inconsistent: %d %s", w.ID, r.Status, r.Body)
	}
}

func wait(t testing.TB, timeout time.Duration, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

func queryInt(t testing.TB, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func txStatus(provider, external string) string {
	var s string
	if err := pool.QueryRow(context.Background(), "SELECT status FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2", provider, external).Scan(&s); err != nil {
		return ""
	}
	return s
}

// ---- SQS --------------------------------------------------------------------------------------

type publishOptions struct {
	Token, Group, Dedup string
	NoAuthorization     bool
}

func envelope(messageID string, data map[string]any) map[string]any {
	return map[string]any{"messageId": messageID, "type": "WagerTransactionRequested", "occurredAt": time.Now().UTC().Format(time.RFC3339), "data": data}
}

// newSendInput builds the producer-side request. The deduplication ID defaults to a random value:
// tests that need duplicates to reach the consumer must not be filtered by the FIFO queue.
func newSendInput(raw, group string, opts publishOptions) *sqs.SendMessageInput {
	if opts.Dedup == "" {
		sum := sha256.Sum256([]byte(uuid.NewString()))
		opts.Dedup = hex.EncodeToString(sum[:])
	}
	in := &sqs.SendMessageInput{QueueUrl: aws.String(inputQueue), MessageBody: aws.String(raw), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(opts.Dedup)}
	if !opts.NoAuthorization {
		in.MessageAttributes = map[string]sqstypes.MessageAttributeValue{"Authorization": {DataType: aws.String("String"), StringValue: aws.String("Bearer " + opts.Token)}}
	}
	return in
}

func publish(t testing.TB, provider string, body any, group string, opts publishOptions) {
	t.Helper()
	raw, ok := body.(string)
	if !ok {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		raw = string(b)
	}
	if opts.Token == "" && !opts.NoAuthorization {
		opts.Token = token(t, provider)
	}
	if _, err := providerQ.SendMessage(context.Background(), newSendInput(raw, group, opts)); err != nil {
		t.Fatalf("send: %v", err)
	}
}

// setInputVisibility shortens redelivery so recovery scenarios finish quickly, restoring 60s afterwards.
func setInputVisibility(t *testing.T, seconds int) {
	t.Helper()
	set := func(v int) {
		_, err := adminQ.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: aws.String(inputQueue), Attributes: map[string]string{"VisibilityTimeout": fmt.Sprint(v)}})
		if err != nil {
			t.Errorf("set visibility: %v", err)
		}
	}
	set(seconds)
	t.Cleanup(func() { set(60) })
}

// ---- Docker control ---------------------------------------------------------------------------

func container(name string) string { return project + "-" + name + "-1" }

func docker(t testing.TB, args ...string) string {
	t.Helper()
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// liveBase returns an instance whose process is up, whatever the state of its dependencies.
func liveBase(t testing.TB) string {
	t.Helper()
	for _, base := range bases {
		if r, err := callErr(base, "GET", "/health/live", "", nil, nil); err == nil && r.Status == 200 {
			return base
		}
	}
	t.Fatal("no application instance is alive")
	return ""
}

func ready(base string) bool {
	r, err := callErr(base, "GET", "/health/ready", "", nil, nil)
	return err == nil && r.Status == 200
}

func ensureInstancesReady(t testing.TB) {
	t.Helper()
	for i, name := range instances {
		if state := docker(t, "inspect", "-f", "{{.State.Running}}", container(name)); state != "true" {
			docker(t, "start", container(name))
		}
		i := i
		wait(t, 120*time.Second, instances[i]+" ready", func() bool { return ready(bases[i]) })
	}
}

// tryWait is wait without the fatal timeout, for callers that want to add diagnostics first.
func tryWait(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

// dumpAppLogs prints warnings and errors the instances logged since the given time.
func dumpAppLogs(t testing.TB, since time.Time) {
	t.Helper()
	for _, name := range instances {
		out, _ := exec.Command("docker", "logs", "--since", since.UTC().Format(time.RFC3339), container(name)).CombinedOutput()
		for _, line := range strings.Split(string(out), "\n") {
			if strings.Contains(line, `"level":"WARN"`) || strings.Contains(line, `"level":"ERROR"`) {
				if len(line) > 400 {
					line = line[:400]
				}
				t.Logf("%s: %s", name, line)
			}
		}
	}
}

// metricErr reads one unlabeled sample from an instance's protected /metrics endpoint without failing the test,
// so background samplers can use it.
func metricErr(base, name string) (float64, error) {
	r, err := callErr(base, "GET", "/metrics", metricsToken(), nil, nil)
	if err != nil {
		return 0, err
	}
	if r.Status != 200 {
		return 0, fmt.Errorf("metrics: %d %s", r.Status, r.Body)
	}
	for _, line := range strings.Split(string(r.Body), "\n") {
		if strings.HasPrefix(line, name+" ") {
			return strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
		}
	}
	return 0, fmt.Errorf("metric %s not exposed", name)
}

// metricsToken returns a cached internal token without touching *testing.T, for use from goroutines.
func metricsToken() string {
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache["wallet-service"]; ok {
		return c.value
	}
	return ""
}

// metric reads one unlabeled sample from an instance's protected /metrics endpoint.
func metric(t testing.TB, base, name string) float64 {
	t.Helper()
	token(t, "wallet-service") // make sure a fresh token is cached
	v, err := metricErr(base, name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// waitForBlockedConsumer waits until some consumer is stuck behind the wallet lock the test is holding,
// and logs how long the broker took to deliver. On failure it prints what the instances logged.
func waitForBlockedConsumer(t *testing.T, w wallet) {
	t.Helper()
	published := time.Now()
	blocked := func() bool {
		return queryInt(t, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND pid<>pg_backend_pid()") > 0
	}
	if !tryWait(150*time.Second, blocked) {
		dumpAppLogs(t, published.Add(-time.Minute))
		t.Fatalf("no consumer blocked on wallet %s within %s of publishing", w.ID, time.Since(published).Round(time.Second))
	}
	t.Logf("a consumer blocked on the locked wallet %s after publishing", time.Since(published).Round(100*time.Millisecond))
}

// ---- owner access (fault injection only) -----------------------------------------------------

// ownerPool connects as the database owner, which the application never does. Tests use it only to put a
// wallet into a state no workload reaches in practice (an exhausted version counter) without faking anything.
func ownerPool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), env("E2E_ADMIN_DATABASE_URL", "postgres://jungle:localpassword@127.0.0.1:15432/jungle?sslmode=disable"))
	if err != nil {
		t.Fatalf("owner connection (E2E_ADMIN_DATABASE_URL): %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// setWalletVersion rewrites a wallet's version as the owner. The movement trigger protects the version even
// from the owner, so it is switched off for the edit.
func setWalletVersion(t testing.TB, owner *pgxpool.Pool, walletID string, version int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, statement := range []string{
		"ALTER TABLE wallets DISABLE TRIGGER wallet_movement_consistency",
		fmt.Sprintf("UPDATE wallets SET version = %d WHERE id = '%s'", version, walletID),
		"ALTER TABLE wallets ENABLE TRIGGER wallet_movement_consistency",
	} {
		if _, err := tx.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// labeledMetric sums the samples of a labeled series (matching every given label fragment) across instances.
func labeledMetric(t testing.TB, name string, fragments ...string) float64 {
	t.Helper()
	token(t, "wallet-service")
	var total float64
	for _, base := range bases {
		r, err := callErr(base, "GET", "/metrics", metricsToken(), nil, nil)
		if err != nil || r.Status != 200 {
			continue
		}
	lines:
		for _, line := range strings.Split(string(r.Body), "\n") {
			if !strings.HasPrefix(line, name+"{") {
				continue
			}
			for _, fragment := range fragments {
				if !strings.Contains(line, fragment) {
					continue lines
				}
			}
			if v, err := strconv.ParseFloat(strings.TrimSpace(line[strings.LastIndex(line, " ")+1:]), 64); err == nil {
				total += v
			}
		}
	}
	return total
}
