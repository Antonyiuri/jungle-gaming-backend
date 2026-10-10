package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type OutboxRecord struct {
	ID, WalletID, LeaseToken string
	Payload                  []byte
	Attempts                 int
}

func boundedBatch(limit int) int {
	if limit < 1 {
		return 10
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func (s *Store) ClaimOutbox(ctx context.Context, limit int, lease time.Duration) ([]OutboxRecord, error) {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `WITH candidates AS (
        SELECT id FROM outbox_events WHERE published_at IS NULL AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
        ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT $1)
        UPDATE outbox_events e SET lease_token=$2,lease_until=clock_timestamp()+make_interval(secs=>$3),attempts=e.attempts+1
        FROM candidates c WHERE e.id=c.id RETURNING e.id::text,e.wallet_id::text,e.lease_token::text,e.payload,e.attempts`, boundedBatch(limit), uuid.NewString(), lease.Seconds())
	if err != nil {
		return nil, err
	}
	records := make([]OutboxRecord, 0)
	for rows.Next() {
		var row OutboxRecord
		if err := rows.Scan(&row.ID, &row.WalletID, &row.LeaseToken, &row.Payload, &row.Attempts); err != nil {
			rows.Close()
			return nil, err
		}
		records = append(records, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *Store) Published(ctx context.Context, id, token string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET published_at=clock_timestamp(),lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2 AND published_at IS NULL`, id, token)
	return err
}

func (s *Store) RetryOutbox(ctx context.Context, id, token string) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events SET next_attempt_at=clock_timestamp()+make_interval(secs=>LEAST(300,power(2,LEAST(attempts,9)))*(0.5+random()*0.5)),lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2 AND published_at IS NULL`, id, token)
	return err
}

type referenceJob struct{ id, walletID, token string }

func (s *Store) ProcessReferences(ctx context.Context, limit int) error {
	jobs, err := s.claimReferences(ctx, limit)
	if err != nil {
		return err
	}
	var failures []error
	for _, job := range jobs {
		if ctx.Err() != nil {
			return errors.Join(append(failures, ctx.Err())...)
		}
		taskCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := s.processReference(taskCtx, job)
		cancel()
		if err != nil {
			failures = append(failures, err)
			s.deferReference(job)
		}
	}
	return errors.Join(failures...)
}

// deferReference postpones the next attempt of a job whose processing failed. Without it a persistent error
// would be retried every time the 30 s lease expires, forever and at that rate. It follows the schedule used
// for a reference that is simply not there yet: exponential in the attempts so far, jittered, capped at five
// minutes. The error itself never ends the transaction: only the transaction's own facts can do that.
// It runs on its own short budget because the job's context is usually the thing that just expired.
func (s *Store) deferReference(job referenceJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.pool.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at=clock_timestamp()+make_interval(secs=>LEAST(300,power(2,LEAST(attempt_count,9)))*(0.5+random()*0.5)),lease_token=NULL,lease_until=NULL WHERE id=$1 AND lease_token=$2 AND status='PENDING_REFERENCE'`, job.id, job.token)
}

func (s *Store) claimReferences(ctx context.Context, limit int) ([]referenceJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `WITH candidates AS (
        SELECT id FROM wager_transactions WHERE status='PENDING_REFERENCE' AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
        ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT $1)
        UPDATE wager_transactions t SET lease_token=$2,lease_until=clock_timestamp()+interval '30 seconds',attempt_count=t.attempt_count+1
        FROM candidates c WHERE t.id=c.id RETURNING t.id::text,t.wallet_id::text,t.lease_token::text`, boundedBatch(limit), uuid.NewString())
	if err != nil {
		return nil, err
	}
	var jobs []referenceJob
	for rows.Next() {
		var job referenceJob
		if err := rows.Scan(&job.id, &job.walletID, &job.token); err != nil {
			rows.Close()
			return nil, err
		}
		jobs = append(jobs, job)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (s *Store) processReference(ctx context.Context, job referenceJob) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	snapshot, err := s.lockWallet(ctx, tx, job.walletID)
	if err != nil {
		return err
	}
	var command domain.Command
	var minor int64
	var currency string
	var expires time.Time
	err = tx.QueryRow(ctx, `SELECT provider_id,external_transaction_id,idempotency_key,player_id::text,wallet_id::text,round_id,game_id,kind,amount_minor,currency,reference_external_id,expires_at
        FROM wager_transactions WHERE id=$1 AND status='PENDING_REFERENCE' AND lease_token=$2 AND lease_until>clock_timestamp() FOR UPDATE`, job.id, job.token).Scan(&command.ProviderID, &command.ExternalTransactionID, &command.IdempotencyKey, &command.PlayerID, &command.WalletID, &command.RoundID, &command.GameID, &command.Kind, &minor, &currency, &command.ReferenceExternalTransactionID, &expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	command.Money, err = domain.NewMoney(minor, currency)
	if err != nil {
		return err
	}
	if _, err := s.evaluateAndSave(ctx, tx, job.id, command, snapshot, nil, expires, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
