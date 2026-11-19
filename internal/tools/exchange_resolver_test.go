package tools

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExchangeResolverBoundedResponseAndNoFallback(t *testing.T) {
	for _, mode := range []string{"valid", "scope", "type", "ttl", "duplicate", "redirect", "revoked", "unreachable"} {
		t.Run(mode, func(t *testing.T) {
			calls, authority, fallback := 0, 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				_ = r.ParseForm()
				if r.Form.Get("subject_token") != "separate-assertion" || r.Form.Get("audience") != "mcp" || r.Form.Get("resource") != "urn:mcp:trusted" || r.Form.Get("subject_token_type") != "urn:ietf:params:oauth:token-type:jwt" {
					t.Error("unbounded exchange request")
				}
				if mode == "redirect" {
					http.Redirect(w, r, "/token", 307)
					return
				}
				if mode == "duplicate" {
					w.Write([]byte(`{"access_token":"secret","access_token":"other"}`))
					return
				}
				out := ExchangeResponse{AccessToken: "bounded-token", IssuedTokenType: AccessTokenTokenType, TokenType: "Bearer", ExpiresIn: 60, Scope: "read"}
				if mode == "scope" {
					out.Scope = "read write"
				}
				if mode == "type" {
					out.IssuedTokenType = "refresh_token"
				}
				if mode == "ttl" {
					out.ExpiresIn = 86400
				}
				json.NewEncoder(w).Encode(out)
			}))
			defer server.Close()
			if mode == "unreachable" {
				server.Close()
			}
			cfg := ExchangeRegistration{TokenURL: "http://oauth.fixture/token", Audience: "mcp", Resource: "urn:mcp:trusted", Scope: "read", Policy: netpolicy.Policy{PrivateHosts: []string{"oauth.fixture"}, AllowedMethods: []string{"POST"}, AllowedPathPrefixes: []string{"/token"}}}
			r, err := NewExchangeResolver(map[string]ExchangeRegistration{"trusted": cfg}, func(context.Context, contracts.Scope, string, ExchangeRegistration) (string, error) {
				return "separate-assertion", nil
			}, func(context.Context, contracts.Scope, string) error {
				authority++
				if mode == "revoked" && authority > 1 {
					return contracts.NewError(contracts.ErrForbidden, "revoked")
				}
				return nil
			}, exchangeFallback(func() { fallback++ }), netpolicy.WithResolver(oauthFixtureResolver{}), netpolicy.WithDialer(oauthFixtureDialer{strings.TrimPrefix(server.URL, "http://")}))
			if err != nil {
				t.Fatal(err)
			}
			c, err := r.Resolve(context.Background(), contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "alice"}, "trusted")
			if mode == "valid" {
				if err != nil || c.Bearer != "bounded-token" {
					t.Fatalf("valid exchange refused: %v", err)
				}
			} else if err == nil || c.Bearer != "" {
				t.Fatal("invalid exchange returned credential")
			}
			if fallback != 0 {
				t.Fatal("exchange refusal fell back to static credential")
			}
			if calls > 1 {
				t.Fatal("assertion replayed on redirect")
			}
			if err != nil && (strings.Contains(err.Error(), "bounded-token") || strings.Contains(err.Error(), "separate-assertion")) {
				t.Fatal("credential leaked in error")
			}
		})
	}
}

type exchangeFallback func()

func (f exchangeFallback) Resolve(context.Context, contracts.Scope, string) (Credential, error) {
	f()
	return Credential{Bearer: "fallback"}, nil
}

func TestExchangeAudienceCannotBeWidenedBeforeHTTP(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; http.Error(w, "unexpected", 500) }))
	defer srv.Close()
	a, err := NewOAuthAdapter(ProviderBoundary{TokenURL: "http://oauth.fixture/token", Audience: "trusted", Policy: netpolicy.Policy{PrivateHosts: []string{"oauth.fixture"}, AllowedMethods: []string{"POST"}, AllowedPathPrefixes: []string{"/token"}}}, netpolicy.WithResolver(oauthFixtureResolver{}), netpolicy.WithDialer(oauthFixtureDialer{strings.TrimPrefix(srv.URL, "http://")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Exchange(context.Background(), ExchangeRequest{SubjectToken: "never-transmit", Audience: "foreign"}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("cross-audience exchange accepted")
	}
	if called {
		t.Fatal("cross-audience assertion left ToolGW")
	}
}
