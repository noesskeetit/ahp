package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"net/http"
	"sync"
	"time"
)

type authority struct {
	cfg      config
	key      ed25519.PrivateKey
	keys     map[string]ed25519.PublicKey
	ids      *identity.Store
	mu       sync.Mutex
	redeemed map[string]int64
}

func newAuthority(cfg config, key ed25519.PrivateKey, keys map[string]ed25519.PublicKey, ids *identity.Store) http.Handler {
	a := &authority{cfg: cfg, key: key, keys: keys, ids: ids, redeemed: map[string]int64{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		jsonReply(w, map[string]string{"issuer": cfg.Issuer, "jwks_uri": cfg.Issuer + "/jwks", "token_endpoint": cfg.Issuer + "/token", "ahp_profile": "synthetic"})
	})
	mux.HandleFunc("GET /jwks", func(w http.ResponseWriter, r *http.Request) {
		jsonReply(w, map[string]any{"keys": []any{map[string]string{"kty": "OKP", "crv": "Ed25519", "kid": "synthetic-authority", "use": "sig", "alg": "EdDSA", "x": base64.RawURLEncoding.EncodeToString(key.Public().(ed25519.PublicKey))}}})
	})
	mux.HandleFunc("POST /token", a.exchange)
	return mux
}
func jsonReply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}
func (a *authority) exchange(w http.ResponseWriter, r *http.Request) {
	deny := func() {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusForbidden)
		jsonReply(w, map[string]string{"error": "invalid_request"})
	}
	if r.TLS == nil || r.Header.Get("Authorization") != "" || r.URL.RawQuery != "" {
		deny()
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	if r.ParseForm() != nil {
		deny()
		return
	}
	allowed := map[string]bool{"grant_type": true, "subject_token": true, "subject_token_type": true, "audience": true, "scope": true, "resource": true}
	if len(r.PostForm) != len(allowed) {
		deny()
		return
	}
	for name, values := range r.PostForm {
		if !allowed[name] || len(values) != 1 {
			deny()
			return
		}
	}
	if r.PostForm.Get("grant_type") != tools.TokenExchangeGrantType || r.PostForm.Get("subject_token_type") != "urn:ietf:params:oauth:token-type:jwt" {
		deny()
		return
	}
	c, err := auth.VerifyExchangeAssertion(r.PostForm.Get("subject_token"), a.cfg.AHPBearerIssuer, a.cfg.Issuer+"/token", a.keys)
	if err != nil {
		deny()
		return
	}
	d, ok := a.cfg.Connections[c.Connection]
	if !ok || c.TargetAudience != d.Audience || c.Resource != d.Resource || c.Scope != d.Scope || r.PostForm.Get("audience") != d.Audience || r.PostForm.Get("resource") != d.Resource || r.PostForm.Get("scope") != d.Scope {
		deny()
		return
	}
	deadline, err := a.ids.ValidateConnectionAssertion(r.Context(), c)
	if err != nil {
		deny()
		return
	}
	a.mu.Lock()
	now := time.Now().Unix()
	for j, exp := range a.redeemed {
		if exp <= now {
			delete(a.redeemed, j)
		}
	}
	_, used := a.redeemed[c.JTI]
	if !used && len(a.redeemed) < 10000 {
		a.redeemed[c.JTI] = c.ExpiresAt
	} else {
		used = true
	}
	a.mu.Unlock()
	if used {
		deny()
		return
	}
	ttl := min(int64(60), c.ExpiresAt-now, deadline.Unix()-now)
	if ttl <= 0 {
		deny()
		return
	}
	// This downstream credential has its own purpose and exact MCP resource.
	// It is never a human/session or AHP task access token.
	h, _ := json.Marshal(map[string]string{"alg": "EdDSA", "kid": "synthetic-authority", "typ": "at+jwt"})
	p, _ := json.Marshal(map[string]any{"iss": a.cfg.Issuer, "aud": d.Audience, "sub": c.Subject, "tenant": c.Tenant, "connection": c.Connection, "resource": d.Resource, "scope": d.Scope, "token_use": "mcp_access", "iat": now, "exp": now + ttl})
	input := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	token := input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(a.key, []byte(input)))
	jsonReply(w, tools.ExchangeResponse{AccessToken: token, IssuedTokenType: tools.AccessTokenTokenType, TokenType: "Bearer", ExpiresIn: ttl, Scope: d.Scope})
}
