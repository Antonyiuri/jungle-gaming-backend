// Package messaging adapts SQS deliveries and outbox publication to the application use cases.
package messaging

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/junglegaming/backend-challenge-go/internal/domain"
	"github.com/junglegaming/backend-challenge-go/internal/jsonutil"
	"github.com/junglegaming/backend-challenge-go/internal/trace"
)

const (
	ConsumerName = "wager-processor"
	EnvelopeType = "WagerTransactionRequested"
)

// Envelope is the strict wire contract for wager requests received from SQS.
type Envelope struct {
	MessageID  string         `json:"messageId"`
	Type       string         `json:"type"`
	OccurredAt time.Time      `json:"occurredAt"`
	Data       domain.Command `json:"data"`
}

var errInvalidEnvelope = errors.New("invalid envelope")

// ParseEnvelope strictly decodes a message body; any violation is a permanent defect.
func ParseEnvelope(body []byte) (Envelope, error) {
	var envelope Envelope
	if err := jsonutil.Decode(body, &envelope); err != nil {
		return envelope, errInvalidEnvelope
	}
	// The message ID becomes the correlation ID of the resulting events, so it must be a safe identifier.
	if envelope.Type != EnvelopeType || envelope.OccurredAt.IsZero() || !trace.Valid(envelope.MessageID) {
		return envelope, errInvalidEnvelope
	}
	if err := envelope.Data.Validate(); err != nil {
		return envelope, err
	}
	return envelope, nil
}

// Hash covers the whole canonical envelope and the authenticated sender.
func (e Envelope) Hash(sender string) (string, error) {
	canonical, err := e.Data.CanonicalJSON()
	if err != nil {
		return "", err
	}
	view, err := json.Marshal(struct {
		MessageID      string          `json:"messageId"`
		Type           string          `json:"type"`
		OccurredAt     string          `json:"occurredAt"`
		IdempotencyKey string          `json:"idempotencyKey"`
		Sender         string          `json:"sender"`
		Data           json.RawMessage `json:"data"`
	}{e.MessageID, e.Type, e.OccurredAt.UTC().Format(time.RFC3339Nano), e.Data.IdempotencyKey, sender, canonical})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(view)
	return hex.EncodeToString(digest[:]), nil
}
