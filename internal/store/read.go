package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

type LedgerEntry struct {
	ID            string       `json:"id"`
	WalletID      string       `json:"walletId"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Money         domain.Money `json:"money"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	WalletVersion int64        `json:"walletVersion"`
	CreatedAt     time.Time    `json:"createdAt"`
}

type LedgerPage struct {
	Entries    []LedgerEntry `json:"entries"`
	NextCursor string        `json:"nextCursor,omitempty"`
}

type ledgerCursor struct {
	Format   int    `json:"v"`
	WalletID string `json:"w"`
	After    int64  `json:"a"`
	Upper    int64  `json:"u"`
}

func (s *Store) Ledger(ctx context.Context, wallet, cursor string, limit int) (LedgerPage, error) {
	normalized, err := normalizeID(wallet)
	if err != nil {
		return LedgerPage{}, err
	}
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 100 {
		return LedgerPage{}, failure("INVALID_REQUEST", "ledger limit must be between one and one hundred")
	}
	snapshot, err := s.Wallet(ctx, normalized)
	if err != nil {
		return LedgerPage{}, err
	}
	position := ledgerCursor{Format: 1, WalletID: normalized, Upper: snapshot.Version}
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(decoded) > 512 {
			return LedgerPage{}, failure("INVALID_CURSOR", "ledger cursor is invalid")
		}
		decoder := json.NewDecoder(strings.NewReader(string(decoded)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&position); err != nil || position.Format != 1 || position.WalletID != normalized || position.After < 0 || position.Upper < position.After || position.Upper > snapshot.Version {
			return LedgerPage{}, failure("INVALID_CURSOR", "ledger cursor does not match this wallet")
		}
	}
	rows, err := s.pool.Query(ctx, `SELECT id::text,transaction_id::text,direction,amount_minor,currency,balance_before,balance_after,wallet_version,created_at FROM wallet_ledger_entries WHERE wallet_id=$1 AND wallet_version>$2 AND wallet_version<=$3 ORDER BY wallet_version,id LIMIT $4`, normalized, position.After, position.Upper, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	defer rows.Close()
	page := LedgerPage{Entries: make([]LedgerEntry, 0)}
	for rows.Next() {
		var entry LedgerEntry
		var minor, before, after int64
		var currency string
		if err := rows.Scan(&entry.ID, &entry.TransactionID, &entry.Direction, &minor, &currency, &before, &after, &entry.WalletVersion, &entry.CreatedAt); err != nil {
			return LedgerPage{}, err
		}
		entry.WalletID = normalized
		entry.Money, err = domain.NewMoney(minor, currency)
		if err != nil {
			return LedgerPage{}, err
		}
		entry.BalanceBefore, err = domain.NewMoney(before, currency)
		if err != nil {
			return LedgerPage{}, err
		}
		entry.BalanceAfter, err = domain.NewMoney(after, currency)
		if err != nil {
			return LedgerPage{}, err
		}
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return LedgerPage{}, err
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		position.After = page.Entries[len(page.Entries)-1].WalletVersion
		encoded, err := json.Marshal(position)
		if err != nil {
			return LedgerPage{}, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, nil
}

// ExactAmount can represent reconciliation discrepancies outside operational int64 bounds.
type ExactAmount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type Reconciliation struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance ExactAmount  `json:"calculatedBalance"`
	Difference        ExactAmount  `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

func (s *Store) Reconcile(ctx context.Context, wallet string) (Reconciliation, error) {
	normalized, err := normalizeID(wallet)
	if err != nil {
		return Reconciliation{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Reconciliation{}, err
	}
	defer rollback(tx)
	snapshot, err := scanWallet(tx.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallets WHERE id=$1", normalized))
	if err != nil {
		return Reconciliation{}, err
	}
	var calculatedText string
	var count int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor::NUMERIC ELSE -amount_minor::NUMERIC END),0)::TEXT,COUNT(*) FROM wallet_ledger_entries WHERE wallet_id=$1`, normalized).Scan(&calculatedText, &count)
	if err != nil {
		return Reconciliation{}, err
	}
	calculated, ok := new(big.Int).SetString(calculatedText, 10)
	if !ok {
		return Reconciliation{}, failure("PROCESSING_FAILED", "ledger sum is not an exact integer")
	}
	difference := new(big.Int).Sub(big.NewInt(snapshot.Balance.MinorUnits()), calculated)
	result := Reconciliation{WalletID: normalized, StoredBalance: snapshot.Balance, CalculatedBalance: ExactAmount{Amount: formatMinor(calculated), Currency: snapshot.Balance.Currency()}, Difference: ExactAmount{Amount: formatMinor(difference), Currency: snapshot.Balance.Currency()}, Consistent: difference.Sign() == 0, CheckedEntries: count}
	if err := tx.Commit(ctx); err != nil {
		return Reconciliation{}, err
	}
	return result, nil
}

func formatMinor(value *big.Int) string {
	magnitude := new(big.Int).Abs(value).String()
	for len(magnitude) < 3 {
		magnitude = "0" + magnitude
	}
	result := magnitude[:len(magnitude)-2] + "." + magnitude[len(magnitude)-2:]
	if value.Sign() < 0 {
		return "-" + result
	}
	return result
}
