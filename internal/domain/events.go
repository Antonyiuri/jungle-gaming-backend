package domain

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
)

type EventMetadata struct {
	CorrelationID, CausationID string
	OccurredAt, ExpiresAt      time.Time
}

type Event interface {
	ID() string
	EventType() string
	AggregateID() string
	TransactionID() string
	Version() int
	OccurredAt() time.Time
	JSON() ([]byte, error)
}

type EventContext struct {
	TransactionID         string `json:"transactionId"`
	WalletID              string `json:"walletId"`
	PlayerID              string `json:"playerId"`
	Origin                string `json:"origin"`
	ProviderID            string `json:"providerId,omitempty"`
	ExternalTransactionID string `json:"externalTransactionId,omitempty"`
	RoundID               string `json:"roundId,omitempty"`
	GameID                string `json:"gameId,omitempty"`
	Kind                  string `json:"kind"`
	Money                 Money  `json:"money"`
}

type ProcessedEventData struct {
	EventContext
	Balance       Money `json:"balance"`
	WalletVersion int64 `json:"walletVersion"`
}
type RejectedEventData struct {
	EventContext
	FailureCode string    `json:"failureCode"`
	RejectedAt  time.Time `json:"rejectedAt"`
}
type PendingReferenceEventData struct {
	EventContext
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
	ExpiresAt                      time.Time `json:"expiresAt"`
}
type BalanceChangedEventData struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore Money  `json:"balanceBefore"`
	BalanceAfter  Money  `json:"balanceAfter"`
	WalletVersion int64  `json:"walletVersion"`
}

type eventRecord struct {
	id, eventType, aggregateID, transactionID string
	occurredAt                                time.Time
	payload                                   []byte
}

func (e eventRecord) ID() string            { return e.id }
func (e eventRecord) EventType() string     { return e.eventType }
func (e eventRecord) AggregateID() string   { return e.aggregateID }
func (e eventRecord) TransactionID() string { return e.transactionID }
func (e eventRecord) Version() int          { return 1 }
func (e eventRecord) OccurredAt() time.Time { return e.occurredAt }
func (e eventRecord) JSON() ([]byte, error) {
	if len(e.payload) == 0 {
		return nil, fail("INVALID_EVENT", "event is uninitialized")
	}
	return append([]byte(nil), e.payload...), nil
}
func (e eventRecord) MarshalJSON() ([]byte, error) { return e.JSON() }

type WagerTransactionProcessed struct{ eventRecord }
type WagerTransactionRejected struct{ eventRecord }
type WagerTransactionPendingReference struct{ eventRecord }
type WalletBalanceChanged struct{ eventRecord }

func newEventRecord(eventType, transactionID, walletID string, metadata EventMetadata, data any) (eventRecord, error) {
	if !validUUID(transactionID) || !validUUID(walletID) || !validIdentifier(metadata.CorrelationID) || metadata.OccurredAt.IsZero() {
		return eventRecord{}, fail("INVALID_EVENT", "event metadata is invalid")
	}
	if metadata.CausationID != "" && !validIdentifier(metadata.CausationID) {
		return eventRecord{}, fail("INVALID_EVENT", "event causation identifier is invalid")
	}
	id, err := newEventID()
	if err != nil {
		return eventRecord{}, err
	}
	envelope := struct {
		EventID       string    `json:"eventId"`
		EventType     string    `json:"eventType"`
		AggregateID   string    `json:"aggregateId"`
		CorrelationID string    `json:"correlationId"`
		CausationID   string    `json:"causationId,omitempty"`
		OccurredAt    time.Time `json:"occurredAt"`
		Version       int       `json:"version"`
		Data          any       `json:"data"`
	}{id, eventType, walletID, metadata.CorrelationID, metadata.CausationID, metadata.OccurredAt.UTC(), 1, data}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return eventRecord{}, err
	}
	return eventRecord{id: id, eventType: eventType, aggregateID: walletID, transactionID: transactionID, occurredAt: metadata.OccurredAt.UTC(), payload: payload}, nil
}

func newEventID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func NewWagerTransactionProcessed(metadata EventMetadata, data ProcessedEventData) (WagerTransactionProcessed, error) {
	if err := validateEventContext(data.EventContext); err != nil {
		return WagerTransactionProcessed{}, err
	}
	if err := data.Balance.Validate(); err != nil {
		return WagerTransactionProcessed{}, err
	}
	if data.Balance.MinorUnits() < 0 || data.WalletVersion < 1 {
		return WagerTransactionProcessed{}, fail("INVALID_EVENT", "processed result is invalid")
	}
	record, err := newEventRecord(EventWagerTransactionProcessed, data.TransactionID, data.WalletID, metadata, data)
	return WagerTransactionProcessed{record}, err
}

func NewWagerTransactionRejected(metadata EventMetadata, data RejectedEventData) (WagerTransactionRejected, error) {
	if err := validateEventContext(data.EventContext); err != nil {
		return WagerTransactionRejected{}, err
	}
	if !validIdentifier(data.FailureCode) || data.RejectedAt.IsZero() {
		return WagerTransactionRejected{}, fail("INVALID_EVENT", "rejection details are invalid")
	}
	data.RejectedAt = data.RejectedAt.UTC()
	record, err := newEventRecord(EventWagerTransactionRejected, data.TransactionID, data.WalletID, metadata, data)
	return WagerTransactionRejected{record}, err
}

func NewWagerTransactionPendingReference(metadata EventMetadata, data PendingReferenceEventData) (WagerTransactionPendingReference, error) {
	if err := validateEventContext(data.EventContext); err != nil {
		return WagerTransactionPendingReference{}, err
	}
	if !validIdentifier(data.ReferenceExternalTransactionID) || !data.ExpiresAt.After(metadata.OccurredAt) {
		return WagerTransactionPendingReference{}, fail("INVALID_EVENT", "pending reference details are invalid")
	}
	data.ExpiresAt = data.ExpiresAt.UTC()
	record, err := newEventRecord(EventWagerTransactionPendingReference, data.TransactionID, data.WalletID, metadata, data)
	return WagerTransactionPendingReference{record}, err
}

func NewWalletBalanceChanged(metadata EventMetadata, data BalanceChangedEventData) (WalletBalanceChanged, error) {
	if data.WalletVersion < 1 {
		return WalletBalanceChanged{}, fail("INVALID_EVENT", "wallet version must be positive")
	}
	if _, err := NewWalletLedgerEntry(LedgerSnapshot{ID: data.TransactionID, WalletID: data.WalletID, TransactionID: data.TransactionID, Direction: data.Direction, Money: data.Money, BalanceBefore: data.BalanceBefore, BalanceAfter: data.BalanceAfter, CreatedAt: metadata.OccurredAt}); err != nil {
		return WalletBalanceChanged{}, err
	}
	record, err := newEventRecord(EventWalletBalanceChanged, data.TransactionID, data.WalletID, metadata, data)
	return WalletBalanceChanged{record}, err
}

func validateEventContext(data EventContext) error {
	if !validUUID(data.TransactionID) || !validUUID(data.WalletID) || !validUUID(data.PlayerID) {
		return fail("INVALID_EVENT", "event identity is invalid")
	}
	if err := data.Money.Validate(); err != nil {
		return err
	}
	if data.Money.MinorUnits() < 0 {
		return fail("INVALID_EVENT", "event amount cannot be negative")
	}
	if data.Origin == "INTERNAL" {
		if data.Kind != KindOpening || data.Money.MinorUnits() <= 0 || data.ProviderID != "" || data.ExternalTransactionID != "" || data.RoundID != "" || data.GameID != "" {
			return fail("INVALID_EVENT", "internal event contains invalid metadata")
		}
	} else if data.Origin == "EXTERNAL" {
		for _, value := range []string{data.ProviderID, data.ExternalTransactionID, data.RoundID, data.GameID} {
			if !validIdentifier(value) {
				return fail("INVALID_EVENT", "external event metadata is incomplete")
			}
		}
		switch data.Kind {
		case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		default:
			return fail("INVALID_EVENT", "external event kind is invalid")
		}
	} else {
		return fail("INVALID_EVENT", "event origin is invalid")
	}
	return nil
}

// BuildEvents must only be called for a new persisted transition, never a replay.
func BuildEvents(transactionID string, command Command, outcome Outcome, walletVersion int64, metadata EventMetadata) ([]Event, error) {
	origin := "EXTERNAL"
	if command.Kind == KindOpening {
		origin = "INTERNAL"
	}
	context := EventContext{TransactionID: transactionID, WalletID: command.WalletID, PlayerID: command.PlayerID, Origin: origin, ProviderID: command.ProviderID, ExternalTransactionID: command.ExternalTransactionID, RoundID: command.RoundID, GameID: command.GameID, Kind: command.Kind, Money: command.Money}
	var events []Event
	switch outcome.Status {
	case StatusProcessed:
		event, err := NewWagerTransactionProcessed(metadata, ProcessedEventData{EventContext: context, Balance: outcome.After, WalletVersion: walletVersion})
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		if outcome.Changed {
			changed, err := NewWalletBalanceChanged(metadata, BalanceChangedEventData{WalletID: command.WalletID, TransactionID: transactionID, Direction: outcome.Direction, Money: command.Money, BalanceBefore: outcome.Before, BalanceAfter: outcome.After, WalletVersion: walletVersion})
			if err != nil {
				return nil, err
			}
			events = append(events, changed)
		}
	case StatusRejected:
		event, err := NewWagerTransactionRejected(metadata, RejectedEventData{EventContext: context, FailureCode: outcome.FailureCode, RejectedAt: metadata.OccurredAt})
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	case StatusPendingReference:
		event, err := NewWagerTransactionPendingReference(metadata, PendingReferenceEventData{EventContext: context, ReferenceExternalTransactionID: command.ReferenceExternalTransactionID, ExpiresAt: metadata.ExpiresAt})
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	case StatusFailed:
		return []Event{}, nil
	default:
		return nil, fail("INVALID_EVENT", "this status cannot emit an integration event")
	}
	return events, nil
}
