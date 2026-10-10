package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	KindOpening            = "OPENING"
	KindBet                = "BET"
	KindWin                = "WIN"
	KindLoss               = "LOSS"
	KindRefund             = "REFUND"
	KindRollback           = "ROLLBACK"
	StatusPending          = "PENDING"
	StatusPendingReference = "PENDING_REFERENCE"
	StatusProcessed        = "PROCESSED"
	StatusRejected         = "REJECTED"
	StatusFailed           = "FAILED"
	DirectionDebit         = "DEBIT"
	DirectionCredit        = "CREDIT"
)

type Command struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
	Money                          Money  `json:"money"`
}

type Result struct {
	TransactionID                  string `json:"transactionId"`
	Status                         string `json:"status"`
	Balance                        *Money `json:"balance,omitempty"`
	WalletVersion                  int64  `json:"walletVersion,omitempty"`
	FailureCode                    string `json:"failureCode,omitempty"`
	IdempotentReplay               bool   `json:"idempotentReplay"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func validUUID(value string) bool {
	return uuidPattern.MatchString(value) && value != "00000000-0000-0000-0000-000000000000"
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 255 || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r < 0x20 {
			return false
		}
	}
	return true
}

func (c Command) Validate() error {
	if c.IdempotencyKey == "" {
		return fail("IDEMPOTENCY_KEY_REQUIRED", "idempotency key is required")
	}
	for _, value := range []string{c.ProviderID, c.ExternalTransactionID, c.IdempotencyKey, c.RoundID, c.GameID} {
		if !validIdentifier(value) {
			return fail("INVALID_REQUEST", "business identifiers must be valid nonempty strings")
		}
	}
	if !validUUID(c.PlayerID) || !validUUID(c.WalletID) {
		return fail("INVALID_REQUEST", "player and wallet identifiers must be UUIDs")
	}
	if err := c.Money.Validate(); err != nil {
		return err
	}
	if c.Money.MinorUnits() < 0 {
		return fail("INVALID_MONEY", "external amounts cannot be negative")
	}
	switch c.Kind {
	case KindBet, KindWin, KindRefund, KindRollback:
		if c.Money.MinorUnits() == 0 {
			return fail("INVALID_MONEY", "this operation requires a positive amount")
		}
	case KindLoss:
		if c.Money.MinorUnits() != 0 {
			return fail("INVALID_MONEY", "loss requires a zero amount")
		}
	default:
		return fail("INVALID_REQUEST", "unsupported external operation kind")
	}
	hasReference := c.ReferenceExternalTransactionID != ""
	if (c.Kind == KindRefund || c.Kind == KindRollback) && !hasReference {
		return fail("INVALID_REFERENCE", "reversal requires an external reference")
	}
	if hasReference && (c.Kind == KindBet || c.Kind == KindLoss) {
		return fail("INVALID_REFERENCE", "this operation cannot have a reference")
	}
	if hasReference && (!validIdentifier(c.ReferenceExternalTransactionID) || c.ReferenceExternalTransactionID == c.ExternalTransactionID) {
		return fail("INVALID_REFERENCE", "reference must be valid and cannot point to the operation itself")
	}
	return nil
}

// CanonicalJSON implements JCS for this fixed schema of strings and objects.
// Keys are emitted in UTF-16 lexical order, which is ASCII order for this schema.
func (c Command) CanonicalJSON() ([]byte, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	out.WriteByte('{')
	writePair := func(key, value string) {
		appendJSONString(&out, key)
		out.WriteByte(':')
		appendJSONString(&out, value)
	}
	writePair("externalTransactionId", c.ExternalTransactionID)
	out.WriteByte(',')
	writePair("gameId", c.GameID)
	out.WriteByte(',')
	writePair("kind", c.Kind)
	out.WriteString(",\"money\":{")
	writePair("amount", c.Money.Amount())
	out.WriteByte(',')
	writePair("currency", c.Money.Currency())
	out.WriteByte('}')
	out.WriteByte(',')
	writePair("playerId", strings.ToLower(c.PlayerID))
	out.WriteByte(',')
	writePair("providerId", c.ProviderID)
	if c.ReferenceExternalTransactionID != "" {
		out.WriteByte(',')
		writePair("referenceExternalTransactionId", c.ReferenceExternalTransactionID)
	}
	out.WriteByte(',')
	writePair("roundId", c.RoundID)
	out.WriteByte(',')
	writePair("walletId", strings.ToLower(c.WalletID))
	out.WriteByte('}')
	return out.Bytes(), nil
}

func (c Command) Hash() (string, error) {
	canonical, err := c.CanonicalJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func appendJSONString(out *bytes.Buffer, value string) {
	const digits = "0123456789abcdef"
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if r < 0x20 {
				out.WriteString(`\u00`)
				out.WriteByte(digits[r>>4])
				out.WriteByte(digits[r&15])
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
}
