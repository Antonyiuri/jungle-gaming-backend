package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/junglegaming/backend-challenge-go/internal/application"
	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/config"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/jsonutil"
	"github.com/junglegaming/backend-challenge-go/internal/observability"
	"github.com/junglegaming/backend-challenge-go/internal/trace"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type TokenVerifier interface {
	Verify(context.Context, string) (auth.Principal, error)
}
type Server struct {
	HTTP     *http.Server
	draining atomic.Bool
	service  *application.Service
	verifier TokenVerifier
	ready    func(context.Context) error
	cfg      config.Config
	log      *slog.Logger
	metrics  *observability.Metrics
}
type principalKey struct{}
type correlationKey struct{}

func New(c config.Config, service *application.Service, verifier TokenVerifier, ready func(context.Context) error, log *slog.Logger, metrics *observability.Metrics) *Server {
	s := &Server{service: service, verifier: verifier, ready: ready, cfg: c, log: log, metrics: metrics}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /health/ready", s.readiness)
	mux.Handle("GET /metrics", s.protected(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := principal(r)
		if !p.Internal || !p.Has("wallet:read") {
			s.fail(w, r, 403, "FORBIDDEN", "Access is forbidden.", false)
			return
		}
		promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}).ServeHTTP(w, r)
	})))
	mux.Handle("POST /wallets", s.protected(http.HandlerFunc(s.createWallet)))
	mux.Handle("GET /wallets/{walletID}", s.protected(http.HandlerFunc(s.getWallet)))
	mux.Handle("GET /wallets/{walletID}/ledger", s.protected(http.HandlerFunc(s.ledger)))
	mux.Handle("POST /wallets/{walletID}/reconciliation", s.protected(http.HandlerFunc(s.reconcile)))
	mux.Handle("POST /wagering/transactions", s.protected(http.HandlerFunc(s.process)))
	mux.Handle("GET /wagering/transactions/{transactionID}", s.protected(http.HandlerFunc(s.transaction)))
	mux.Handle("GET /providers/{providerID}/wagering/transactions/{externalID}", s.protected(http.HandlerFunc(s.external)))
	s.HTTP = &http.Server{Addr: c.HTTPAddr, Handler: s.correlate(mux), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	return s
}
func (s *Server) Drain() { s.draining.Store(true) }
func (s *Server) correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The identifier ends up in logs, headers and event payloads, so only a safe shape is propagated.
		id := r.Header.Get("X-Correlation-ID")
		if !trace.Valid(id) {
			id = uuid.NewString()
		}
		w.Header().Set("X-Correlation-ID", id)
		ctx := trace.WithCorrelation(context.WithValue(r.Context(), correlationKey{}, id), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
func (s *Server) protected(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.draining.Load() {
			s.fail(w, r, 503, "TEMPORARILY_UNAVAILABLE", "The service is shutting down.", true)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ProcessingTimeout)
		defer cancel()
		p, err := s.verifier.Verify(ctx, r.Header.Get("Authorization"))
		if err != nil {
			status := 401
			code := "UNAUTHENTICATED"
			if errors.Is(err, auth.ErrForbidden) {
				status = 403
				code = "FORBIDDEN"
			}
			s.fail(w, r, status, code, "Access is not authorized.", false)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, principalKey{}, p)))
	})
}
func principal(r *http.Request) auth.Principal {
	p, _ := r.Context().Value(principalKey{}).(auth.Principal)
	return p
}
func (s *Server) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.draining.Load() || s.ready(ctx) != nil {
		write(w, 503, map[string]string{"status": "not_ready"})
		return
	}
	write(w, 200, map[string]string{"status": "ready"})
}
func decode(w http.ResponseWriter, r *http.Request, out any) error {
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		return errors.New("Content-Type must be application/json")
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64<<10))
	if err != nil {
		return err
	}
	return jsonutil.Decode(data, out)
}
func (s *Server) createWallet(w http.ResponseWriter, r *http.Request) {
	var request struct {
		PlayerID       string       `json:"playerId"`
		InitialBalance domain.Money `json:"initialBalance"`
	}
	if err := decode(w, r, &request); err != nil {
		s.fail(w, r, 400, "INVALID_REQUEST", "The request body is invalid.", false)
		return
	}
	if _, err := uuid.Parse(request.PlayerID); err != nil {
		s.fail(w, r, 400, "INVALID_REQUEST", "The player ID is invalid.", false)
		return
	}
	if request.InitialBalance.Currency() != "BRL" {
		s.fail(w, r, 400, "UNSUPPORTED_CURRENCY", "Only BRL wallets are supported.", false)
		return
	}
	result, err := s.service.CreateWallet(r.Context(), principal(r), request.PlayerID, request.InitialBalance)
	if err != nil {
		s.failure(w, r, err)
		return
	}
	write(w, 201, result)
}
func (s *Server) getWallet(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.Wallet(r.Context(), principal(r), r.PathValue("walletID"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	write(w, 200, result)
}
func (s *Server) ledger(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 100 {
			s.fail(w, r, 400, "INVALID_REQUEST", "The limit must be between 1 and 100.", false)
			return
		}
		limit = n
	}
	result, err := s.service.Ledger(r.Context(), principal(r), r.PathValue("walletID"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.failure(w, r, err)
		return
	}
	write(w, 200, result)
}
func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.Reconcile(r.Context(), principal(r), r.PathValue("walletID"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	if !result.Consistent {
		s.metrics.ReconciliationMismatch.Inc()
		s.log.ErrorContext(r.Context(), "wallet reconciliation mismatch", "walletId", r.PathValue("walletID"), "correlationId", r.Context().Value(correlationKey{}))
	}
	write(w, 200, result)
}
func (s *Server) process(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() { s.metrics.Duration.WithLabelValues("http").Observe(time.Since(start).Seconds()) }()
	var command domain.Command
	if err := decode(w, r, &command); err != nil {
		s.fail(w, r, 400, "INVALID_REQUEST", "The request body is invalid.", false)
		return
	}
	if command.IdempotencyKey != "" {
		s.fail(w, r, 400, "INVALID_REQUEST", "HTTP idempotency must be provided only in the header.", false)
		return
	}
	command.IdempotencyKey = r.Header.Get("Idempotency-Key")
	if command.IdempotencyKey == "" {
		s.fail(w, r, 400, "IDEMPOTENCY_KEY_REQUIRED", "The Idempotency-Key header is required.", false)
		return
	}
	result, err := s.service.Process(r.Context(), principal(r), command, nil)
	if err != nil {
		s.failure(w, r, err)
		return
	}
	s.metrics.Results.WithLabelValues(result.Status, command.Kind).Inc()
	if result.IdempotentReplay {
		s.metrics.Replays.Inc()
	}
	status := 200
	switch result.Status {
	case "PENDING_REFERENCE":
		status = 202
		w.Header().Set("Location", "/wagering/transactions/"+result.TransactionID)
	case "REJECTED":
		status = 422
	case "FAILED":
		status = 500
	}
	s.log.InfoContext(r.Context(), "wager request completed", "correlationId", r.Context().Value(correlationKey{}), "transactionId", result.TransactionID, "walletId", command.WalletID, "providerId", principal(r).ProviderID, "status", result.Status)
	write(w, status, result)
}
func (s *Server) transaction(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.Transaction(r.Context(), principal(r), r.PathValue("transactionID"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	write(w, 200, result)
}
func (s *Server) external(w http.ResponseWriter, r *http.Request) {
	result, err := s.service.ExternalTransaction(r.Context(), principal(r), r.PathValue("providerID"), r.PathValue("externalID"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	write(w, 200, result)
}
func (s *Server) failure(w http.ResponseWriter, r *http.Request, err error) {
	code := domain.Code(err)
	status := 400
	retry := false
	message := "The request is invalid."
	switch code {
	case "FORBIDDEN":
		status = 403
		message = "Access is forbidden."
	case "NOT_FOUND":
		status = 404
		message = "The resource was not found."
	case "IDEMPOTENCY_CONFLICT", "EXTERNAL_TRANSACTION_CONFLICT", "WALLET_ALREADY_EXISTS":
		status = 409
		message = "The request conflicts with a stored operation."
		s.metrics.Conflicts.Inc()
	case "PROCESSING_FAILED":
		// A permanent server-side failure that retrying cannot fix (for example a ledger that does not add up).
		status = 500
		message = "The operation failed permanently."
	case "":
		status = 503
		code = "TEMPORARILY_UNAVAILABLE"
		message = "The service is temporarily unavailable. Retry with the same identity."
		retry = true
		s.log.ErrorContext(r.Context(), "request dependency failure", "correlationId", r.Context().Value(correlationKey{}), "error", err)
	}
	s.fail(w, r, status, code, message, retry)
}
func (s *Server) fail(w http.ResponseWriter, r *http.Request, status int, code, message string, retry bool) {
	if retry {
		w.Header().Set("Retry-After", "1")
	}
	write(w, status, map[string]any{"error": map[string]any{"code": code, "message": message, "retryable": retry}, "correlationId": r.Context().Value(correlationKey{})})
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
