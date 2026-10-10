package application

import (
	"context"
	"testing"

	"github.com/junglegaming/backend-challenge-go/internal/auth"
	"github.com/junglegaming/backend-challenge-go/internal/domain"
)

// Authorization runs before any persistence access, so these checks use a service without a store:
// reaching the store would panic and fail the test.
func principal(provider string, internal bool, roles ...string) auth.Principal {
	p := auth.Principal{ClientID: "client", ProviderID: provider, Internal: internal, Roles: map[string]bool{}}
	for _, role := range roles {
		p.Roles[role] = true
	}
	return p
}

func forbiddenCode(t *testing.T, name string, err error) {
	t.Helper()
	if domain.Code(err) != "FORBIDDEN" {
		t.Errorf("%s: want FORBIDDEN, got %v", name, err)
	}
}

func TestProcessRequiresMatchingProviderAndRole(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	c := domain.Command{ProviderID: "provider-a"}
	_, err := s.Process(ctx, principal("provider-b", false, "wager:write"), c, nil)
	forbiddenCode(t, "another provider", err)
	_, err = s.Process(ctx, principal("provider-a", false, "wager:read"), c, nil)
	forbiddenCode(t, "read-only role", err)
	_, err = s.Process(ctx, principal("", true, "wager:write"), c, nil)
	forbiddenCode(t, "internal client without a provider", err)
	_, err = s.Process(ctx, principal("provider-a", false), c, nil)
	forbiddenCode(t, "no roles", err)
}

func TestInternalOperationsAreNotAvailableToProviders(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	provider := principal("provider-a", false, "wallet:read", "wallet:write", "wallet:reconcile", "wager:read", "wager:write")
	money, _ := domain.NewMoney(0, "BRL")
	_, err := s.CreateWallet(ctx, provider, "p", money)
	forbiddenCode(t, "create wallet", err)
	_, err = s.Wallet(ctx, provider, "w")
	forbiddenCode(t, "read wallet", err)
	_, err = s.Ledger(ctx, provider, "w", "", 10)
	forbiddenCode(t, "read ledger", err)
	_, err = s.Reconcile(ctx, provider, "w")
	forbiddenCode(t, "reconcile", err)
}

func TestInternalClientNeedsTheSpecificRole(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	internal := principal("", true, "wallet:read")
	money, _ := domain.NewMoney(0, "BRL")
	_, err := s.CreateWallet(ctx, internal, "p", money)
	forbiddenCode(t, "write without wallet:write", err)
	_, err = s.Reconcile(ctx, internal, "w")
	forbiddenCode(t, "reconcile without wallet:reconcile", err)
	_, err = s.Transaction(ctx, internal, "t")
	forbiddenCode(t, "internal client reading provider transactions", err)
}

func TestTransactionReadsAreScopedToTheAuthenticatedProvider(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	_, err := s.ExternalTransaction(ctx, principal("provider-a", false, "wager:read"), "provider-b", "x")
	forbiddenCode(t, "path provider differs from the token", err)
	_, err = s.ExternalTransaction(ctx, principal("provider-a", false), "provider-a", "x")
	forbiddenCode(t, "missing wager:read", err)
	_, err = s.Transaction(ctx, principal("provider-a", false), "t")
	forbiddenCode(t, "missing wager:read on lookup by id", err)
}
