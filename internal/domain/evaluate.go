package domain

import (
	"math"
	"strings"
)

type Reference struct {
	ID, ProviderID, ExternalTransactionID, PlayerID, WalletID, RoundID, GameID, Kind, Status string
	Money                                                                                    Money
	Direction                                                                                string
}

// FailureWalletVersionExhausted is the failure code of a transaction that needs a wallet movement the wallet
// can no longer record. It is the only permanent failure the domain can establish by itself: every other
// failure (database errors, timeouts, publisher problems) is transient or unproven and never becomes FAILED.
const FailureWalletVersionExhausted = "WALLET_VERSION_EXHAUSTED"

type Outcome struct {
	Status, FailureCode, Direction string
	Before, After                  Money
	Changed                        bool
	ReferenceID                    string
}

// Evaluate computes a decision without mutating the wallet or reference.
// Persistence must hold the wallet lock while evaluating and saving the result.
func Evaluate(command Command, wallet *Wallet, reference *Reference, alreadyReversed bool) (Outcome, error) {
	if err := wallet.Validate(); err != nil {
		return Outcome{}, err
	}
	before := wallet.Balance()
	result := Outcome{Status: StatusProcessed, Before: before, After: before}
	reject := func(code string) (Outcome, error) {
		result.Status = StatusRejected
		result.FailureCode = code
		result.Direction = ""
		result.Changed = false
		result.After = before
		return result, nil
	}
	if err := command.Validate(); err != nil {
		return reject(Code(err))
	}
	if !strings.EqualFold(command.WalletID, wallet.ID()) || !strings.EqualFold(command.PlayerID, wallet.PlayerID()) || command.Money.Currency() != before.Currency() {
		return reject("WALLET_CONTEXT_MISMATCH")
	}
	if command.ReferenceExternalTransactionID != "" {
		if reference == nil {
			result.Status = StatusPendingReference
			return result, nil
		}
		result.ReferenceID = reference.ID
		if reference.ProviderID != command.ProviderID || reference.ExternalTransactionID != command.ReferenceExternalTransactionID || !strings.EqualFold(reference.PlayerID, command.PlayerID) || !strings.EqualFold(reference.WalletID, command.WalletID) || reference.RoundID != command.RoundID || reference.GameID != command.GameID || reference.Money.Currency() != command.Money.Currency() {
			return reject("REFERENCE_CONTEXT_MISMATCH")
		}
		if (command.Kind == KindRefund || command.Kind == KindWin) && reference.Kind != KindBet {
			return reject("REFERENCE_KIND_NOT_ALLOWED")
		}
		if command.Kind == KindRollback && reference.Kind != KindBet && reference.Kind != KindWin && reference.Kind != KindRefund {
			return reject("REFERENCE_KIND_NOT_ALLOWED")
		}
		if command.Kind == KindRefund || command.Kind == KindRollback {
			comparison, err := command.Money.Compare(reference.Money)
			if err != nil || comparison != 0 {
				return reject("REFERENCE_AMOUNT_MISMATCH")
			}
		}
		switch reference.Status {
		case StatusPending, StatusPendingReference:
			result.Status = StatusPendingReference
			return result, nil
		case StatusProcessed:
		default:
			return reject("REFERENCE_NOT_PROCESSED")
		}
		if (command.Kind == KindRefund || command.Kind == KindRollback) && alreadyReversed {
			return reject("REFERENCE_ALREADY_REVERSED")
		}
	}
	switch command.Kind {
	case KindLoss:
		return result, nil
	case KindBet:
		result.Direction = DirectionDebit
	case KindWin, KindRefund:
		result.Direction = DirectionCredit
	case KindRollback:
		if reference.Kind == KindBet {
			result.Direction = DirectionCredit
		} else {
			result.Direction = DirectionDebit
		}
	}
	var err error
	if result.Direction == DirectionDebit {
		result.After, err = before.Subtract(command.Money)
	} else {
		result.After, err = before.Add(command.Money)
	}
	if err != nil {
		return reject(Code(err))
	}
	if result.After.MinorUnits() < 0 {
		if command.Kind == KindBet {
			return reject("INSUFFICIENT_FUNDS")
		}
		return reject("REVERSAL_INSUFFICIENT_FUNDS")
	}
	if wallet.Version() == math.MaxInt64 {
		// A movement must advance the wallet version, and this wallet cannot advance it. The version never
		// decreases, so no retry can ever succeed: this is a permanent failure of the aggregate, not a business
		// rejection. Only decisions that need a movement fail this way; losses and rejections still work.
		return Outcome{Status: StatusFailed, FailureCode: FailureWalletVersionExhausted, Before: before, After: before}, nil
	}
	result.Changed = true
	return result, nil
}
