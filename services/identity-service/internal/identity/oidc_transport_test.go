package identity

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOIDCSyntheticTLSIssuerBoundary(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	var server *httptest.Server
	mode := "normal"
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/discovery":
			if mode == "redirect" {
				http.Redirect(w, r, "https://foreign.example/metadata", 302)
				return
			}
			uri := server.URL + "/jwks"
			if mode == "foreign-jwks" {
				uri = "https://foreign.example/jwks"
			}
			json.NewEncoder(w).Encode(map[string]any{"issuer": server.URL, "jwks_uri": uri})
		case "/jwks":
			json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "OKP", "crv": "Ed25519", "kid": "test-key", "use": "sig", "alg": "EdDSA", "x": base64.RawURLEncoding.EncodeToString(pub)}}})
			if mode == "oversized" {
				w.Write([]byte(strings.Repeat(" ", oidcMaxJWSKBody+1)))
			}
		}
	}))
	defer server.Close()
	token := func(changes map[string]any) string {
		claims := map[string]any{"iss": server.URL, "aud": "ahp", "exp": time.Now().Add(time.Minute).Unix(), "token_use": "access", "sub": "alice", "tenant": "tenant"}
		for k, v := range changes {
			claims[k] = v
		}
		h, _ := json.Marshal(map[string]string{"alg": "EdDSA", "kid": "test-key", "typ": "at+jwt"})
		p, _ := json.Marshal(claims)
		data := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
		return data + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(data)))
	}
	for _, tc := range []struct {
		name, mode string
		claims     map[string]any
		ok         bool
	}{
		{"valid", "normal", nil, true},
		{"wrong-issuer", "normal", map[string]any{"iss": "https://foreign.example"}, false},
		{"wrong-audience", "normal", map[string]any{"aud": "foreign"}, false},
		{"refresh", "normal", map[string]any{"token_use": "refresh"}, false},
		{"task", "normal", map[string]any{"token_kind": "task", "task": "task"}, false},
		{"expired", "normal", map[string]any{"exp": time.Now().Add(-time.Minute).Unix()}, false},
		{"foreign-jwks", "foreign-jwks", nil, false},
		{"redirect", "redirect", nil, false},
		{"oversized-jwks", "oversized", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode = tc.mode
			a, err := NewOIDCAuthenticator(OIDCConfig{Issuer: server.URL, DiscoveryURL: server.URL + "/discovery", ClientID: "ahp", HTTPClient: server.Client(), Synthetic: true})
			if err != nil {
				t.Fatal(err)
			}
			if a.Profile() != "synthetic-oidc" {
				t.Errorf("synthetic mode not labelled")
			}
			u, err := a.Login(context.Background(), LoginInput{Assertion: token(tc.claims)})
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v want=%v", err == nil, tc.ok)
			}
			if tc.ok && (u.Subject != "alice" || u.Tenant != "tenant") {
				t.Fatalf("bad authoritative identity")
			}
		})
	}
}
