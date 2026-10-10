package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
)

// fakeVerifier maps bearer values to principals; everything else is unauthenticated.
type fakeVerifier map[string]auth.Principal

func (f fakeVerifier) Verify(_ context.Context, header string) (auth.Principal, error) {
	token := strings.TrimPrefix(header, "Bearer ")
	if p, ok := f[token]; ok {
		return p, nil
	}
	if token == "stranger" {
		return auth.Principal{}, auth.ErrForbidden
	}
	return auth.Principal{}, auth.ErrUnauthenticated
}

func newTestServer(ready func(context.Context) error) *Server {
	verifier := fakeVerifier{
		"provider-a": {ClientID: "provider-a", ProviderID: "provider-a", Roles: map[string]bool{"wager:write": true, "wager:read": true}},
		"internal":   {ClientID: "wallet-service", Internal: true, Roles: map[string]bool{"wallet:read": true, "wallet:write": true, "wallet:reconcile": true}},
	}
	if ready == nil {
		ready = func(context.Context) error { return nil }
	}
	// The service has no store: every case below must be decided before persistence is reached.
	return New(config.Config{ProcessingTimeout: 5 * time.Second}, application.New(nil), verifier, ready, slog.New(slog.NewTextHandler(io.Discard, nil)), observability.New())
}

func call(s *Server, method, path, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	s.HTTP.Handler.ServeHTTP(w, r)
	return w
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code      string `json:"code"`
			Retryable bool   `json:"retryable"`
		} `json:"error"`
		CorrelationID string `json:"correlationId"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not the standard error envelope: %q", w.Body.String())
	}
	if body.CorrelationID == "" || w.Header().Get("X-Correlation-ID") != body.CorrelationID {
		t.Errorf("correlation id missing or inconsistent: %q vs %q", body.CorrelationID, w.Header().Get("X-Correlation-ID"))
	}
	return body.Error.Code
}

const betBody = `{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"b734f231-163d-4bed-8d91-23d81a31821e","walletId":"05f3842e-55de-4e81-85a9-b547a85b0ef8","roundId":"r","gameId":"g","kind":"BET","money":{"amount":"10.00","currency":"BRL"}}`

func TestProtectedRoutesRequireAuthentication(t *testing.T) {
	s := newTestServer(nil)
	for _, route := range [][2]string{{"POST", "/wallets"}, {"GET", "/wallets/x"}, {"GET", "/wallets/x/ledger"}, {"POST", "/wallets/x/reconciliation"},
		{"POST", "/wagering/transactions"}, {"GET", "/wagering/transactions/x"}, {"GET", "/providers/provider-a/wagering/transactions/x"}, {"GET", "/metrics"}} {
		if w := call(s, route[0], route[1], "", "", nil); w.Code != 401 || errorCode(t, w) != "UNAUTHENTICATED" {
			t.Errorf("%v without a token: %d %s", route, w.Code, w.Body.String())
		}
		if w := call(s, route[0], route[1], "forged", "", nil); w.Code != 401 {
			t.Errorf("%v with an invalid token: %d", route, w.Code)
		}
		if w := call(s, route[0], route[1], "stranger", "", nil); w.Code != 403 {
			t.Errorf("%v with an unmapped client: %d", route, w.Code)
		}
	}
}

func TestHealthEndpointsAreOpenAndReadinessReflectsDependencies(t *testing.T) {
	s := newTestServer(nil)
	if w := call(s, "GET", "/health/live", "", "", nil); w.Code != 200 {
		t.Errorf("liveness: %d", w.Code)
	}
	if w := call(s, "GET", "/health/ready", "", "", nil); w.Code != 200 {
		t.Errorf("readiness: %d", w.Code)
	}
	down := newTestServer(func(context.Context) error { return errors.New("db down") })
	w := call(down, "GET", "/health/ready", "", "", nil)
	if w.Code != 503 || strings.Contains(w.Body.String(), "db down") {
		t.Errorf("readiness must be 503 without leaking details: %d %s", w.Code, w.Body.String())
	}
	if w := call(down, "GET", "/health/live", "", "", nil); w.Code != 200 {
		t.Errorf("liveness must not depend on dependencies: %d", w.Code)
	}
	s.Drain()
	if w := call(s, "GET", "/health/ready", "", "", nil); w.Code != 503 {
		t.Errorf("a draining server is not ready: %d", w.Code)
	}
	if w := call(s, "POST", "/wagering/transactions", "provider-a", betBody, map[string]string{"Idempotency-Key": "k"}); w.Code != 503 || w.Header().Get("Retry-After") == "" {
		t.Errorf("a draining server refuses new work with Retry-After: %d", w.Code)
	}
}

func TestWagerRequestValidation(t *testing.T) {
	s := newTestServer(nil)
	cases := []struct {
		name    string
		body    string
		headers map[string]string
		status  int
		code    string
	}{
		{"missing idempotency key", betBody, nil, 400, "IDEMPOTENCY_KEY_REQUIRED"},
		{"key in the body", strings.Replace(betBody, `"kind"`, `"idempotencyKey":"k","kind"`, 1), map[string]string{"Idempotency-Key": "k"}, 400, "INVALID_REQUEST"},
		{"unknown field", strings.Replace(betBody, `"kind"`, `"extra":1,"kind"`, 1), map[string]string{"Idempotency-Key": "k"}, 400, "INVALID_REQUEST"},
		{"float amount", strings.Replace(betBody, `"10.00"`, `10.00`, 1), map[string]string{"Idempotency-Key": "k"}, 400, "INVALID_REQUEST"},
		{"duplicate key in json", strings.Replace(betBody, `"kind":"BET"`, `"kind":"BET","kind":"WIN"`, 1), map[string]string{"Idempotency-Key": "k"}, 400, "INVALID_REQUEST"},
		{"malformed json", `{`, map[string]string{"Idempotency-Key": "k"}, 400, "INVALID_REQUEST"},
		{"another providers command", strings.Replace(betBody, "provider-a", "provider-b", 1), map[string]string{"Idempotency-Key": "k"}, 403, "FORBIDDEN"},
	}
	for _, tc := range cases {
		w := call(s, "POST", "/wagering/transactions", "provider-a", tc.body, tc.headers)
		if w.Code != tc.status || errorCode(t, w) != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
		}
	}
	r := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(betBody))
	r.Header.Set("Authorization", "Bearer provider-a")
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("Idempotency-Key", "k")
	w := httptest.NewRecorder()
	s.HTTP.Handler.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Errorf("non JSON content type: %d", w.Code)
	}
	big := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(strings.Repeat("a", 70<<10)))
	big.Header.Set("Authorization", "Bearer provider-a")
	big.Header.Set("Content-Type", "application/json")
	big.Header.Set("Idempotency-Key", "k")
	w = httptest.NewRecorder()
	s.HTTP.Handler.ServeHTTP(w, big)
	if w.Code != 400 {
		t.Errorf("oversized body: %d", w.Code)
	}
}

func TestRoleSeparationBetweenProvidersAndInternalClients(t *testing.T) {
	s := newTestServer(nil)
	if w := call(s, "POST", "/wallets", "provider-a", `{"playerId":"b734f231-163d-4bed-8d91-23d81a31821e","initialBalance":{"amount":"1.00","currency":"BRL"}}`, nil); w.Code != 403 {
		t.Errorf("providers cannot create wallets: %d", w.Code)
	}
	if w := call(s, "GET", "/wallets/05f3842e-55de-4e81-85a9-b547a85b0ef8", "provider-a", "", nil); w.Code != 403 {
		t.Errorf("providers cannot read wallets: %d", w.Code)
	}
	if w := call(s, "GET", "/metrics", "provider-a", "", nil); w.Code != 403 {
		t.Errorf("providers cannot read metrics: %d", w.Code)
	}
	// Histograms are exported even before the first observation, so the operational metrics are all visible.
	if w := call(s, "GET", "/metrics", "internal", "", nil); w.Code != 200 || !strings.Contains(w.Body.String(), "wallet_lock_wait_seconds_count") || !strings.Contains(w.Body.String(), "wager_processing_duration_seconds_count") || !strings.Contains(w.Body.String(), "concurrency_conflicts_total") || !strings.Contains(w.Body.String(), "outbox_pending_events") {
		t.Errorf("internal metrics: %d", w.Code)
	}
	if w := call(s, "POST", "/wagering/transactions", "internal", betBody, map[string]string{"Idempotency-Key": "k"}); w.Code != 403 {
		t.Errorf("the internal client cannot post provider wagers: %d", w.Code)
	}
	if w := call(s, "GET", "/providers/provider-b/wagering/transactions/x", "provider-a", "", nil); w.Code != 403 {
		t.Errorf("a provider cannot read another provider's namespace: %d", w.Code)
	}
}

func TestWalletRequestValidation(t *testing.T) {
	s := newTestServer(nil)
	cases := map[string]string{
		"bad player":  `{"playerId":"nope","initialBalance":{"amount":"1.00","currency":"BRL"}}`,
		"usd wallet":  `{"playerId":"b734f231-163d-4bed-8d91-23d81a31821e","initialBalance":{"amount":"1.00","currency":"USD"}}`,
		"bad amount":  `{"playerId":"b734f231-163d-4bed-8d91-23d81a31821e","initialBalance":{"amount":"1","currency":"BRL"}}`,
		"extra field": `{"playerId":"b734f231-163d-4bed-8d91-23d81a31821e","initialBalance":{"amount":"1.00","currency":"BRL"},"x":1}`,
	}
	for name, body := range cases {
		if w := call(s, "POST", "/wallets", "internal", body, nil); w.Code != 400 {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
	for _, limit := range []string{"0", "101", "-1", "abc"} {
		if w := call(s, "GET", "/wallets/05f3842e-55de-4e81-85a9-b547a85b0ef8/ledger?limit="+limit, "internal", "", nil); w.Code != 400 {
			t.Errorf("limit %q: %d", limit, w.Code)
		}
	}
}

func TestCorrelationIDIsPropagatedOrReplaced(t *testing.T) {
	s := newTestServer(nil)
	w := call(s, "GET", "/health/live", "", "", map[string]string{"X-Correlation-ID": "trace-123"})
	if w.Header().Get("X-Correlation-ID") != "trace-123" {
		t.Errorf("a valid id is propagated: %q", w.Header().Get("X-Correlation-ID"))
	}
	w = call(s, "GET", "/health/live", "", "", map[string]string{"X-Correlation-ID": strings.Repeat("x", 200)})
	if got := w.Header().Get("X-Correlation-ID"); got == "" || len(got) > 128 {
		t.Errorf("an oversized id is replaced: %q", got)
	}
	// Anything that could break an event payload or a log line is replaced, not propagated.
	for _, unsafe := range []string{"has space", "tab\there", "semi;colon", "quote\"", "ünïcode"} {
		w = call(s, "GET", "/health/live", "", "", map[string]string{"X-Correlation-ID": unsafe})
		if got := w.Header().Get("X-Correlation-ID"); got == unsafe || got == "" {
			t.Errorf("unsafe id %q must be replaced, got %q", unsafe, got)
		}
	}
	r := httptest.NewRequest("GET", "/health/live", nil)
	if w := httptest.NewRecorder(); func() bool { s.HTTP.Handler.ServeHTTP(w, r); return w.Header().Get("X-Correlation-ID") == "" }() {
		t.Error("an id is generated when absent")
	}
	_ = http.StatusOK
}

// Every failure the service can report maps to one documented status, and only the transient ones invite a retry.
func TestFailureMappingSeparatesPermanentFromRetryableErrors(t *testing.T) {
	s := newTestServer(nil)
	cases := []struct {
		name      string
		err       error
		status    int
		code      string
		retryable bool
	}{
		{"permanent processing failure", &domain.Error{Code: "PROCESSING_FAILED", Message: "x"}, 500, "PROCESSING_FAILED", false},
		{"not found", &domain.Error{Code: "NOT_FOUND"}, 404, "NOT_FOUND", false},
		{"forbidden", &domain.Error{Code: "FORBIDDEN"}, 403, "FORBIDDEN", false},
		{"idempotency conflict", &domain.Error{Code: "IDEMPOTENCY_CONFLICT"}, 409, "IDEMPOTENCY_CONFLICT", false},
		{"external transaction conflict", &domain.Error{Code: "EXTERNAL_TRANSACTION_CONFLICT"}, 409, "EXTERNAL_TRANSACTION_CONFLICT", false},
		{"invalid request", &domain.Error{Code: "INVALID_REQUEST"}, 400, "INVALID_REQUEST", false},
		{"unknown dependency failure", errors.New("connection reset by peer"), 503, "TEMPORARILY_UNAVAILABLE", true},
		{"deadline exceeded is transient", context.DeadlineExceeded, 503, "TEMPORARILY_UNAVAILABLE", true},
	}
	for _, c := range cases {
		w := httptest.NewRecorder()
		s.correlate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.failure(w, r, c.err) })).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
		if w.Code != c.status || errorCode(t, w) != c.code {
			t.Errorf("%s: got %d %s, want %d %s", c.name, w.Code, w.Body.String(), c.status, c.code)
		}
		if retryAfter := w.Header().Get("Retry-After"); (retryAfter != "") != c.retryable {
			t.Errorf("%s: Retry-After %q does not match retryable=%v", c.name, retryAfter, c.retryable)
		}
	}
}
