package auth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Antonyiuri/jungle-gaming-backend/internal/config"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

// idp is an in-process OpenID provider that publishes its keys and signs whatever claims a test needs.
type idp struct {
	server *httptest.Server
	key    *rsa.PrivateKey
}

func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &idp{key: key}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": p.server.URL, "jwks_uri": p.server.URL + "/keys",
			"authorization_endpoint": p.server.URL + "/auth", "token_endpoint": p.server.URL + "/token",
			"id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "k1", Algorithm: "RS256", Use: "sig"}}})
	})
	p.server = httptest.NewServer(mux)
	t.Cleanup(p.server.Close)
	return p
}

func (p *idp) verifier(t *testing.T) *Verifier {
	t.Helper()
	v, err := New(context.Background(), config.Config{
		Issuer: p.server.URL, Audience: "jungle-backend",
		ProviderClients: map[string]string{"provider-a": "provider-a", "provider-b": "provider-b"}, InternalClient: "wallet-service",
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

type claims map[string]any

func (p *idp) baseClaims(client string, roles ...string) claims {
	now := time.Now()
	c := claims{
		"iss": p.server.URL, "aud": []string{"jungle-backend"}, "sub": "service-account-" + client, "azp": client,
		"iat": now.Unix(), "nbf": now.Add(-time.Minute).Unix(), "exp": now.Add(5 * time.Minute).Unix(),
	}
	if roles != nil {
		c["realm_access"] = map[string]any{"roles": roles}
	}
	return c
}

func (p *idp) sign(t *testing.T, key *rsa.PrivateKey, c claims) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "k1"))
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.Signed(signer).Claims(map[string]any(c)).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestVerifyAcceptsAValidProviderToken(t *testing.T) {
	p := newIDP(t)
	principal, err := p.verifier(t).Verify(context.Background(), "Bearer "+p.sign(t, p.key, p.baseClaims("provider-a", "wager:write", "wager:read")))
	if err != nil {
		t.Fatal(err)
	}
	if principal.ClientID != "provider-a" || principal.ProviderID != "provider-a" || principal.Internal || !principal.Has("wager:write") || !principal.Has("wager:read") || principal.Has("wallet:write") {
		t.Fatalf("unexpected principal %+v", principal)
	}
}

func TestVerifyMapsTheInternalClientWithoutAProvider(t *testing.T) {
	p := newIDP(t)
	principal, err := p.verifier(t).Verify(context.Background(), "bearer "+p.sign(t, p.key, p.baseClaims("wallet-service", "wallet:read")))
	if err != nil {
		t.Fatal(err)
	}
	if !principal.Internal || principal.ProviderID != "" || !principal.Has("wallet:read") {
		t.Fatalf("unexpected principal %+v", principal)
	}
}

func TestVerifyRefusesTokensThatAreNotTrustworthy(t *testing.T) {
	p := newIDP(t)
	v := p.verifier(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(claims)) string {
		c := p.baseClaims("provider-a", "wager:write")
		f(c)
		return p.sign(t, p.key, c)
	}
	cases := map[string]string{
		"wrong audience":           mutate(func(c claims) { c["aud"] = []string{"another-service"} }),
		"missing audience":         mutate(func(c claims) { delete(c, "aud") }),
		"wrong issuer":             mutate(func(c claims) { c["iss"] = "https://evil.example/realms/jungle" }),
		"expired":                  mutate(func(c claims) { c["exp"] = time.Now().Add(-time.Hour).Unix() }),
		"not yet valid":            mutate(func(c claims) { c["nbf"] = time.Now().Add(time.Hour).Unix() }),
		"missing client claim":     mutate(func(c claims) { delete(c, "azp") }),
		"signed by an unknown key": p.sign(t, otherKey, p.baseClaims("provider-a", "wager:write")),
		"not a jwt":                "abc.def.ghi",
		"empty":                    "",
	}
	for name, token := range cases {
		if _, err := v.Verify(context.Background(), "Bearer "+token); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: want ErrUnauthenticated, got %v", name, err)
		}
	}
	if _, err := v.Verify(context.Background(), p.sign(t, p.key, p.baseClaims("provider-a"))); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("a token without the Bearer scheme must be refused: %v", err)
	}
	if _, err := v.Verify(context.Background(), ""); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("a missing header must be refused: %v", err)
	}
}

func TestVerifyRefusesAlgorithmConfusion(t *testing.T) {
	p := newIDP(t)
	v := p.verifier(t)
	segment := func(value any) string {
		raw, _ := json.Marshal(value)
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	payload := segment(p.baseClaims("provider-a", "wager:write"))

	unsigned := segment(map[string]string{"alg": "none", "typ": "JWT", "kid": "k1"}) + "." + payload + "."
	if _, err := v.Verify(context.Background(), "Bearer "+unsigned); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("alg=none must be refused: %v", err)
	}

	header := segment(map[string]string{"alg": "HS256", "typ": "JWT", "kid": "k1"})
	mac := hmac.New(sha256.New, []byte("any shared secret"))
	mac.Write([]byte(header + "." + payload))
	forged := header + "." + payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, err := v.Verify(context.Background(), "Bearer "+forged); !errors.Is(err, ErrUnauthenticated) {
		t.Errorf("HS256 must be refused: %v", err)
	}
}

func TestVerifyForbidsAuthenticatedButUnmappedClients(t *testing.T) {
	p := newIDP(t)
	_, err := p.verifier(t).Verify(context.Background(), "Bearer "+p.sign(t, p.key, p.baseClaims("some-other-client", "wager:write")))
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("an unmapped client must be forbidden, got %v", err)
	}
}

func TestNewFailsWhenTheIssuerIsUnreachableOrMisconfigured(t *testing.T) {
	if _, err := New(context.Background(), config.Config{Issuer: "http://127.0.0.1:1/realms/none", Audience: "x"}); err == nil {
		t.Fatal("discovery against an unreachable issuer must fail")
	}
	p := newIDP(t)
	// The discovery document names p.server.URL as issuer; asking for a different issuer URL must fail.
	mismatch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"issuer": "https://someone-else.example", "jwks_uri": p.server.URL + "/keys"})
	}))
	defer mismatch.Close()
	if _, err := New(context.Background(), config.Config{Issuer: mismatch.URL, Audience: "x"}); err == nil {
		t.Fatal("an issuer that does not match its own discovery document must be refused")
	}
}
