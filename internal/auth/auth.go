package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"github.com/coreos/go-oidc/v3/oidc"
)

var ErrUnauthenticated = errors.New("authentication is required")
var ErrForbidden = errors.New("access is forbidden")

type Principal struct {
	ClientID, ProviderID string
	Internal             bool
	Roles                map[string]bool
}

func (p Principal) Has(role string) bool { return p.Roles[role] }

type Verifier struct {
	verifier *oidc.IDTokenVerifier
	clients  map[string]string
	internal string
}

func New(ctx context.Context, c config.Config) (*Verifier, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	ctx = oidc.ClientContext(ctx, client)
	provider, err := oidc.NewProvider(ctx, c.Issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery: %w", err)
	}
	return &Verifier{verifier: provider.Verifier(&oidc.Config{ClientID: c.Audience, SupportedSigningAlgs: []string{"RS256"}}), clients: c.ProviderClients, internal: c.InternalClient}, nil
}

func (v *Verifier) Verify(ctx context.Context, authorization string) (Principal, error) {
	p := Principal{Roles: map[string]bool{}}
	parts := strings.Fields(authorization)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return p, ErrUnauthenticated
	}
	token, err := v.verifier.Verify(ctx, parts[1])
	if err != nil {
		return p, ErrUnauthenticated
	}
	var claims struct {
		ClientID    string `json:"azp"`
		NotBefore   int64  `json:"nbf"`
		RealmAccess struct {
			Roles []string `json:"roles"`
		} `json:"realm_access"`
	}
	if err := token.Claims(&claims); err != nil || claims.ClientID == "" || claims.NotBefore > time.Now().Unix() {
		return p, ErrUnauthenticated
	}
	p.ClientID = claims.ClientID
	p.ProviderID = v.clients[p.ClientID]
	p.Internal = p.ClientID == v.internal
	if p.ProviderID == "" && !p.Internal {
		return p, ErrForbidden
	}
	for _, role := range claims.RealmAccess.Roles {
		p.Roles[role] = true
	}
	return p, nil
}
