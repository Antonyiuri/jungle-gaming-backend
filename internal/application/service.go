package application

import (
	"context"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/auth"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/domain"
	"github.com/Antonyiuri/jungle-gaming-backend/internal/store"
)

type Service struct{ store *store.Store }

func New(s *store.Store) *Service { return &Service{store: s} }
func forbidden() error            { return &domain.Error{Code: "FORBIDDEN", Message: "Access is forbidden."} }

func (s *Service) Process(ctx context.Context, p auth.Principal, c domain.Command, delivery *store.Delivery) (domain.Result, error) {
	if p.ProviderID == "" || !p.Has("wager:write") || p.ProviderID != c.ProviderID {
		return domain.Result{}, forbidden()
	}
	return s.store.Process(ctx, c, delivery)
}
func (s *Service) CreateWallet(ctx context.Context, p auth.Principal, player string, m domain.Money) (domain.WalletSnapshot, error) {
	if !p.Internal || !p.Has("wallet:write") {
		return domain.WalletSnapshot{}, forbidden()
	}
	return s.store.CreateWallet(ctx, player, m)
}
func (s *Service) Wallet(ctx context.Context, p auth.Principal, id string) (domain.WalletSnapshot, error) {
	if !p.Internal || !p.Has("wallet:read") {
		return domain.WalletSnapshot{}, forbidden()
	}
	return s.store.Wallet(ctx, id)
}
func (s *Service) Ledger(ctx context.Context, p auth.Principal, id, cursor string, limit int) (store.LedgerPage, error) {
	if !p.Internal || !p.Has("wallet:read") {
		return store.LedgerPage{}, forbidden()
	}
	return s.store.Ledger(ctx, id, cursor, limit)
}
func (s *Service) Reconcile(ctx context.Context, p auth.Principal, id string) (store.Reconciliation, error) {
	if !p.Internal || !p.Has("wallet:reconcile") {
		return store.Reconciliation{}, forbidden()
	}
	return s.store.Reconcile(ctx, id)
}
func (s *Service) Transaction(ctx context.Context, p auth.Principal, id string) (domain.Result, error) {
	if p.ProviderID == "" || !p.Has("wager:read") {
		return domain.Result{}, forbidden()
	}
	return s.store.Transaction(ctx, p.ProviderID, id)
}
func (s *Service) ExternalTransaction(ctx context.Context, p auth.Principal, provider, external string) (domain.Result, error) {
	if p.ProviderID == "" || provider != p.ProviderID || !p.Has("wager:read") {
		return domain.Result{}, forbidden()
	}
	return s.store.ExternalTransaction(ctx, p.ProviderID, external)
}
