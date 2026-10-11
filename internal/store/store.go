// Package store persists financial operations and their integration records atomically.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/trace"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool         *pgxpool.Pool
	referenceTTL time.Duration
	lockWait     func(time.Duration)
	onConflict   func()
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool, referenceTTL: 24 * time.Hour} }

// SetReferenceTTL configures reference expiry before workers are started.
func (s *Store) SetReferenceTTL(ttl time.Duration) {
	if ttl > 0 {
		s.referenceTTL = ttl
	}
}

// OnLockWait registers a callback that receives how long each wallet lock acquisition waited. It must be
// set before the store is used concurrently.
func (s *Store) OnLockWait(observe func(time.Duration)) { s.lockWait = observe }

// OnConflict registers a callback invoked each time a transaction is repeated because PostgreSQL chose it
// as a deadlock victim or failed it for serialization. The uniform lock order should make this rare, so
// any sustained rate is worth an alert. It must be set before the store is used concurrently.
func (s *Store) OnConflict(observe func()) { s.onConflict = observe }

// lockWallet takes the wallet's row lock and reports the time spent waiting for it, including when the
// wait ends in a timeout: that is exactly when contention matters most.
func (s *Store) lockWallet(ctx context.Context, tx pgx.Tx, walletID string) (domain.WalletSnapshot, error) {
	started := time.Now()
	snapshot, err := scanWallet(tx.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallets WHERE id=$1 FOR UPDATE", walletID))
	if s.lockWait != nil {
		s.lockWait(time.Since(started))
	}
	return snapshot, err
}

type Delivery struct{ MessageID, ConsumerName, Hash, SenderID string }

func failure(code, message string) error { return &domain.Error{Code: code, Message: message} }

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func normalizeID(value string) (string, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return "", failure("INVALID_REQUEST", "identifier must be a nonzero UUID")
	}
	return id.String(), nil
}

type scanner interface{ Scan(...any) error }

func scanWallet(row scanner) (domain.WalletSnapshot, error) {
	var snapshot domain.WalletSnapshot
	var minor int64
	var currency string
	if err := row.Scan(&snapshot.ID, &snapshot.PlayerID, &minor, &currency, &snapshot.Version, &snapshot.CreatedAt, &snapshot.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return snapshot, failure("NOT_FOUND", "wallet was not found")
		}
		return snapshot, err
	}
	var err error
	snapshot.Balance, err = domain.NewMoney(minor, currency)
	return snapshot, err
}

const walletColumns = "id::text, player_id::text, balance_minor, currency, version, created_at, updated_at"

func (s *Store) Wallet(ctx context.Context, id string) (domain.WalletSnapshot, error) {
	normalized, err := normalizeID(id)
	if err != nil {
		return domain.WalletSnapshot{}, err
	}
	return scanWallet(s.pool.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallets WHERE id=$1", normalized))
}

func (s *Store) CreateWallet(ctx context.Context, player string, initial domain.Money) (domain.WalletSnapshot, error) {
	player, err := normalizeID(player)
	if err != nil {
		return domain.WalletSnapshot{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.WalletSnapshot{}, err
	}
	defer rollback(tx)
	// Timestamps come from the database clock, the same source every later update uses, so they stay
	// comparable even when application hosts are skewed from it.
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return domain.WalletSnapshot{}, err
	}
	wallet, err := domain.NewWallet(uuid.NewString(), player, initial, now.UTC())
	if err != nil {
		return domain.WalletSnapshot{}, err
	}
	snapshot := wallet.Snapshot()
	_, err = tx.Exec(ctx, `INSERT INTO wallets(id,player_id,currency,balance_minor,version,created_at,updated_at) VALUES($1,$2,$3,$4,1,$5,$5)`, snapshot.ID, player, initial.Currency(), initial.MinorUnits(), snapshot.CreatedAt)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "23505" {
			return domain.WalletSnapshot{}, failure("WALLET_ALREADY_EXISTS", "the player already has a wallet in this currency")
		}
		return domain.WalletSnapshot{}, err
	}
	if initial.MinorUnits() > 0 {
		// The opening credit is an internal transaction whose metadata the domain defines; it shares the wallet's id.
		opening, err := domain.NewOpeningTransaction(snapshot.ID, snapshot.ID, player, initial, now.UTC())
		if err != nil {
			return domain.WalletSnapshot{}, err
		}
		o := opening.Snapshot()
		encoded, err := json.Marshal(domain.Result{TransactionID: o.ID, Status: o.Status, Balance: o.Balance, WalletVersion: o.WalletVersion})
		if err != nil {
			return domain.WalletSnapshot{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO wager_transactions(id,origin,wallet_id,player_id,kind,amount_minor,currency,status,result) VALUES($1,$2,$1,$3,$4,$5,$6,$7,$8)`, o.ID, o.Origin, o.PlayerID, o.Kind, o.Money.MinorUnits(), o.Money.Currency(), o.Status, encoded)
		if err != nil {
			return domain.WalletSnapshot{}, err
		}
		zero, _ := domain.Zero(initial.Currency())
		outcome := domain.Outcome{Status: "PROCESSED", Direction: "CREDIT", Before: zero, After: initial, Changed: true}
		command := domain.Command{WalletID: snapshot.ID, PlayerID: player, Kind: "OPENING", Money: initial}
		if err := appendLedger(ctx, tx, snapshot.ID, command, outcome, 1); err != nil {
			return domain.WalletSnapshot{}, err
		}
		if err := appendEvents(ctx, tx, snapshot.ID, command, outcome, 1, "", time.Time{}); err != nil {
			return domain.WalletSnapshot{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.WalletSnapshot{}, err
	}
	return snapshot, nil
}

func (s *Store) Process(ctx context.Context, command domain.Command, delivery *Delivery) (domain.Result, error) {
	if err := command.Validate(); err != nil {
		return domain.Result{}, err
	}
	command.WalletID = strings.ToLower(command.WalletID)
	command.PlayerID = strings.ToLower(command.PlayerID)
	hash, err := command.Hash()
	if err != nil {
		return domain.Result{}, err
	}
	if delivery != nil && (delivery.MessageID == "" || delivery.ConsumerName == "" || delivery.Hash == "" || delivery.SenderID == "") {
		return domain.Result{}, failure("INVALID_MESSAGE", "delivery identity is required")
	}
	for attempt := 0; attempt < 3; attempt++ {
		result, err := s.processOnce(ctx, command, hash, delivery)
		var pgerr *pgconn.PgError
		if err == nil || !errors.As(err, &pgerr) || (pgerr.Code != "40P01" && pgerr.Code != "40001") {
			return result, err
		}
		// A deadlock victim or serialization failure rolled this attempt back entirely; repeating it is safe.
		if s.onConflict != nil {
			s.onConflict()
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 15 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return domain.Result{}, ctx.Err()
		case <-timer.C:
		}
	}
	return domain.Result{}, failure("TEMPORARILY_UNAVAILABLE", "concurrent operation must be retried")
}

func (s *Store) processOnce(ctx context.Context, command domain.Command, hash string, delivery *Delivery) (domain.Result, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Result{}, err
	}
	defer rollback(tx)
	if delivery != nil {
		existing, err := reserveInbox(ctx, tx, delivery, command.ProviderID)
		if err != nil {
			return domain.Result{}, err
		}
		if existing != nil {
			if err := tx.Commit(ctx); err != nil {
				return domain.Result{}, err
			}
			return *existing, nil
		}
	}
	if existing, err := findIdentity(ctx, tx, command, hash); err != nil {
		return domain.Result{}, err
	} else if existing != nil {
		if err := completeInbox(ctx, tx, delivery, *existing); err != nil {
			return domain.Result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Result{}, err
		}
		return *existing, nil
	}
	snapshot, err := s.lockWallet(ctx, tx, command.WalletID)
	if err != nil {
		return domain.Result{}, err
	}
	if existing, err := findIdentity(ctx, tx, command, hash); err != nil {
		return domain.Result{}, err
	} else if existing != nil {
		if err := completeInbox(ctx, tx, delivery, *existing); err != nil {
			return domain.Result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Result{}, err
		}
		return *existing, nil
	}
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, `INSERT INTO wager_transactions(id,origin,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount_minor,currency,reference_external_id,status)
        VALUES($1,'EXTERNAL',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NULLIF($13,''),'PENDING') ON CONFLICT DO NOTHING`, id, command.ProviderID, command.ExternalTransactionID, command.IdempotencyKey, hash, command.WalletID, command.PlayerID, command.RoundID, command.GameID, command.Kind, command.Money.MinorUnits(), command.Money.Currency(), command.ReferenceExternalTransactionID)
	if err != nil {
		return domain.Result{}, err
	}
	if tag.RowsAffected() == 0 {
		existing, err := findIdentity(ctx, tx, command, hash)
		if err != nil {
			return domain.Result{}, err
		}
		if existing == nil {
			return domain.Result{}, failure("TEMPORARILY_UNAVAILABLE", "operation identity is being resolved")
		}
		if err := completeInbox(ctx, tx, delivery, *existing); err != nil {
			return domain.Result{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.Result{}, err
		}
		return *existing, nil
	}
	result, err := s.evaluateAndSave(ctx, tx, id, command, snapshot, delivery, time.Time{}, false)
	if err != nil {
		return domain.Result{}, err
	}
	if err := completeInbox(ctx, tx, delivery, result); err != nil {
		return domain.Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Result{}, err
	}
	return result, nil
}

func findIdentity(ctx context.Context, tx pgx.Tx, command domain.Command, hash string) (*domain.Result, error) {
	rows, err := tx.Query(ctx, `SELECT idempotency_key,external_transaction_id,payload_hash,result FROM wager_transactions WHERE provider_id=$1 AND (idempotency_key=$2 OR external_transaction_id=$3)`, command.ProviderID, command.IdempotencyKey, command.ExternalTransactionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type match struct {
		key, external, hash string
		result              []byte
	}
	var matches []match
	for rows.Next() {
		var row match
		if err := rows.Scan(&row.key, &row.external, &row.hash, &row.result); err != nil {
			return nil, err
		}
		matches = append(matches, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, nil
	}
	if len(matches) > 1 {
		return nil, failure("IDEMPOTENCY_CONFLICT", "the supplied identities belong to different transactions")
	}
	row := matches[0]
	if row.key != command.IdempotencyKey {
		return nil, failure("EXTERNAL_TRANSACTION_CONFLICT", "the external transaction is bound to another idempotency key")
	}
	if row.hash != hash {
		return nil, failure("IDEMPOTENCY_CONFLICT", "the idempotency key was already used with another payload")
	}
	var result domain.Result
	if err := json.Unmarshal(row.result, &result); err != nil {
		return nil, fmt.Errorf("decode persisted result: %w", err)
	}
	result.IdempotentReplay = true
	return &result, nil
}

func reserveInbox(ctx context.Context, tx pgx.Tx, delivery *Delivery, provider string) (*domain.Result, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash,sender_id) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, delivery.ConsumerName, delivery.MessageID, delivery.Hash, delivery.SenderID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var hash, sender string
	var disposition *string
	var result []byte
	var storedProvider *string
	err = tx.QueryRow(ctx, `SELECT i.payload_hash,i.sender_id,i.disposition,t.result,t.provider_id FROM inbox_messages i LEFT JOIN wager_transactions t ON t.id=i.transaction_id WHERE i.consumer_name=$1 AND i.message_id=$2`, delivery.ConsumerName, delivery.MessageID).Scan(&hash, &sender, &disposition, &result, &storedProvider)
	if err != nil {
		return nil, err
	}
	if hash != delivery.Hash || sender != delivery.SenderID || storedProvider == nil || *storedProvider != provider {
		return nil, failure("INBOX_CONFLICT", "the message identity was already used by another envelope or sender")
	}
	if disposition == nil {
		return nil, failure("TEMPORARILY_UNAVAILABLE", "message completion is unavailable")
	}
	var decoded domain.Result
	if err := json.Unmarshal(result, &decoded); err != nil {
		return nil, err
	}
	decoded.IdempotentReplay = true
	if *disposition == "DLQ" && decoded.Status != "FAILED" {
		return nil, failure("MESSAGE_REQUIRES_DLQ", "the message requires dead-letter handling")
	}
	return &decoded, nil
}

func completeInbox(ctx context.Context, tx pgx.Tx, delivery *Delivery, result domain.Result) error {
	if delivery == nil {
		return nil
	}
	disposition := "ACK"
	if result.Status == "FAILED" {
		disposition = "DLQ"
	}
	_, err := tx.Exec(ctx, `UPDATE inbox_messages SET transaction_id=$3,completed_at=clock_timestamp(),disposition=$4 WHERE consumer_name=$1 AND message_id=$2 AND completed_at IS NULL`, delivery.ConsumerName, delivery.MessageID, result.TransactionID, disposition)
	return err
}

func resolveReference(ctx context.Context, tx pgx.Tx, command domain.Command) (*domain.Reference, bool, error) {
	if command.ReferenceExternalTransactionID == "" {
		return nil, false, nil
	}
	var reference domain.Reference
	var minor int64
	var currency string
	err := tx.QueryRow(ctx, `SELECT t.id::text,t.provider_id,t.external_transaction_id,t.player_id::text,t.wallet_id::text,t.round_id,t.game_id,t.kind,t.status,t.amount_minor,t.currency,COALESCE(l.direction,'') FROM wager_transactions t LEFT JOIN wallet_ledger_entries l ON l.transaction_id=t.id AND l.wallet_id=t.wallet_id WHERE t.provider_id=$1 AND t.external_transaction_id=$2`, command.ProviderID, command.ReferenceExternalTransactionID).Scan(&reference.ID, &reference.ProviderID, &reference.ExternalTransactionID, &reference.PlayerID, &reference.WalletID, &reference.RoundID, &reference.GameID, &reference.Kind, &reference.Status, &minor, &currency, &reference.Direction)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	reference.Money, err = domain.NewMoney(minor, currency)
	if err != nil {
		return nil, false, err
	}
	var reversed bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM wager_transactions WHERE reference_transaction_id=$1 AND kind IN ('REFUND','ROLLBACK') AND status='PROCESSED')`, reference.ID).Scan(&reversed)
	return &reference, reversed, err
}

func (s *Store) evaluateAndSave(ctx context.Context, tx pgx.Tx, id string, command domain.Command, snapshot domain.WalletSnapshot, delivery *Delivery, expiresAt time.Time, retrying bool) (domain.Result, error) {
	wallet, err := domain.RestoreWallet(snapshot)
	if err != nil {
		return domain.Result{}, err
	}
	reference, reversed, err := resolveReference(ctx, tx, command)
	if err != nil {
		return domain.Result{}, err
	}
	outcome, err := domain.Evaluate(command, wallet, reference, reversed)
	if err != nil {
		return domain.Result{}, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return domain.Result{}, err
	}
	if outcome.Status == "PENDING_REFERENCE" && !expiresAt.IsZero() && !now.Before(expiresAt) {
		outcome.Status = "REJECTED"
		outcome.FailureCode = "REFERENCE_NOT_FOUND"
		if reference != nil {
			outcome.FailureCode = "REFERENCE_NOT_READY"
		}
	}
	if outcome.Status == "PENDING_REFERENCE" && expiresAt.IsZero() {
		expiresAt = now.Add(s.referenceTTL)
	}
	// Evaluate turns an exhausted wallet version into a FAILED outcome, so advancing it cannot overflow.
	version := snapshot.Version
	if outcome.Changed {
		version++
	}
	// The aggregate approves the transition and the shape of the resulting state before anything is written.
	if err := approveTransition(id, command, retrying, outcome, version, now); err != nil {
		return domain.Result{}, err
	}
	if outcome.Changed {
		tag, err := tx.Exec(ctx, `UPDATE wallets SET balance_minor=$2,version=$3,updated_at=clock_timestamp() WHERE id=$1 AND version=$4 AND $2::BIGINT>=0`, command.WalletID, outcome.After.MinorUnits(), version, snapshot.Version)
		if err != nil {
			return domain.Result{}, err
		}
		if tag.RowsAffected() != 1 {
			return domain.Result{}, failure("TEMPORARILY_UNAVAILABLE", "wallet version changed")
		}
		if err := appendLedger(ctx, tx, id, command, outcome, version); err != nil {
			return domain.Result{}, err
		}
	}
	result := domain.Result{TransactionID: id, Status: outcome.Status, FailureCode: outcome.FailureCode, ReferenceExternalTransactionID: command.ReferenceExternalTransactionID}
	if outcome.Status != "PENDING_REFERENCE" {
		balance := snapshot.Balance
		if outcome.Changed {
			balance = outcome.After
		}
		result.Balance = &balance
		result.WalletVersion = version
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return domain.Result{}, err
	}
	var referenceID any
	if reference != nil {
		referenceID = reference.ID
	}
	var expiry any
	if !expiresAt.IsZero() {
		expiry = expiresAt
	}
	_, err = tx.Exec(ctx, `UPDATE wager_transactions SET status=$2,failure_code=NULLIF($3,''),result=$4,reference_transaction_id=$5,
        first_pending_at=CASE WHEN $2='PENDING_REFERENCE' THEN COALESCE(first_pending_at,clock_timestamp()) ELSE first_pending_at END,
        expires_at=COALESCE(expires_at,$6),
        next_attempt_at=CASE WHEN $2='PENDING_REFERENCE' THEN clock_timestamp()+ make_interval(secs => LEAST(300, power(2, LEAST(attempt_count,9))) * (0.5 + random()*0.5)) ELSE NULL END,
        lease_token=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1`, id, outcome.Status, outcome.FailureCode, encoded, referenceID, expiry)
	if err != nil {
		return domain.Result{}, err
	}
	cause := ""
	if delivery != nil {
		cause = delivery.MessageID
	}
	if !retrying || outcome.Status != "PENDING_REFERENCE" {
		if err := appendEvents(ctx, tx, id, command, outcome, version, cause, expiresAt); err != nil {
			return domain.Result{}, err
		}
	}
	return result, nil
}

// approveTransition has the domain aggregate validate the decision as a state transition: from PENDING for a
// new transaction, from PENDING_REFERENCE for a retry of the reference worker. Nothing is persisted yet, so
// a refusal leaves the database untouched.
func approveTransition(id string, command domain.Command, retrying bool, outcome domain.Outcome, version int64, now time.Time) error {
	now = now.UTC()
	aggregate, err := domain.NewWagerTransaction(id, command, now)
	if err != nil {
		return err
	}
	if retrying {
		snapshot := aggregate.Snapshot()
		snapshot.Status = domain.StatusPendingReference
		if aggregate, err = domain.RestoreWagerTransaction(snapshot); err != nil {
			return err
		}
	}
	return aggregate.Apply(outcome, version, now)
}

// appendLedger records one movement. The domain entry is built first so the balance equation is verified
// in the application as well as by the database constraints.
func appendLedger(ctx context.Context, tx pgx.Tx, id string, command domain.Command, outcome domain.Outcome, version int64) error {
	entry, err := domain.NewWalletLedgerEntry(domain.LedgerSnapshot{ID: uuid.NewString(), WalletID: command.WalletID, TransactionID: id, Direction: outcome.Direction,
		Money: command.Money, BalanceBefore: outcome.Before, BalanceAfter: outcome.After, CreatedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	e := entry.Snapshot()
	_, err = tx.Exec(ctx, `INSERT INTO wallet_ledger_entries(id,wallet_id,transaction_id,direction,amount_minor,currency,balance_before,balance_after,wallet_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, e.ID, e.WalletID, e.TransactionID, e.Direction, e.Money.MinorUnits(), e.Money.Currency(), e.BalanceBefore.MinorUnits(), e.BalanceAfter.MinorUnits(), version)
	return err
}

func appendEvents(ctx context.Context, tx pgx.Tx, id string, command domain.Command, outcome domain.Outcome, version int64, cause string, expires time.Time) error {
	// The request or message correlation travels with the events; background work falls back to the transaction.
	correlation := trace.Correlation(ctx)
	if correlation == "" {
		correlation = id
	}
	events, err := domain.BuildEvents(id, command, outcome, version, domain.EventMetadata{CorrelationID: correlation, CausationID: cause, OccurredAt: time.Now().UTC(), ExpiresAt: expires})
	if err != nil {
		return err
	}
	for _, event := range events {
		payload, err := event.JSON()
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events(id,wallet_id,transaction_id,event_type,version,payload,occurred_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, event.ID(), command.WalletID, id, event.EventType(), event.Version(), payload, event.OccurredAt())
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Transaction(ctx context.Context, provider, id string) (domain.Result, error) {
	normalized, err := normalizeID(id)
	if err != nil {
		return domain.Result{}, err
	}
	return scanResult(s.pool.QueryRow(ctx, `SELECT result FROM wager_transactions WHERE provider_id=$1 AND id=$2`, provider, normalized))
}

func (s *Store) ExternalTransaction(ctx context.Context, provider, external string) (domain.Result, error) {
	return scanResult(s.pool.QueryRow(ctx, `SELECT result FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`, provider, external))
}

func scanResult(row scanner) (domain.Result, error) {
	var result domain.Result
	var encoded []byte
	if err := row.Scan(&encoded); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, failure("NOT_FOUND", "transaction was not found")
		}
		return result, err
	}
	err := json.Unmarshal(encoded, &result)
	return result, err
}
