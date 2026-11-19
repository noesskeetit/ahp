package tools

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSyntheticLoopbackIsExplicitTLSExactBoundary(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/token", 307)
			return
		}
		json.NewEncoder(w).Encode(ExchangeResponse{AccessToken: "synthetic-only", IssuedTokenType: AccessTokenTokenType, TokenType: "Bearer", Scope: "read", ExpiresIn: 60})
	}))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign.pem")
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(700), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(foreign, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	issue := func(context.Context, contracts.Scope, string, ExchangeRegistration) (string, error) {
		return "owner-assertion", nil
	}
	check := func(context.Context, contracts.Scope, string) error { return nil }
	base := ExchangeRegistration{TokenURL: server.URL + "/token", Audience: "mcp", Scope: "read", Resource: "urn:mcp:trusted", SyntheticLoopbackCAFile: ca, Policy: netpolicy.Policy{AllowedPathPrefixes: []string{"/token"}}}
	for _, tc := range []struct {
		name, profile, url, ca string
		ok                     bool
	}{{"valid", "synthetic", base.TokenURL, ca, true}, {"no-profile", "local", base.TokenURL, ca, false}, {"http", "synthetic", strings.Replace(base.TokenURL, "https:", "http:", 1), ca, false}, {"dns-localhost", "synthetic", strings.Replace(base.TokenURL, "127.0.0.1", "localhost", 1), ca, false}, {"foreign-ca", "synthetic", base.TokenURL, foreign, false}, {"redirect", "synthetic", server.URL + "/redirect", ca, false}} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.TokenURL = tc.url
			cfg.SyntheticLoopbackCAFile = tc.ca
			before := calls.Load()
			r, err := NewSyntheticExchangeResolver(tc.profile, map[string]ExchangeRegistration{"trusted": cfg}, issue, check, nil)
			if err == nil {
				_, err = r.Resolve(context.Background(), contracts.Scope{Tenant: "t", Deployment: "d", Subject: "s"}, "trusted")
			}
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%v wanted=%v", err == nil, tc.ok)
			}
			if calls.Load()-before > 1 {
				t.Fatal("assertion followed redirect")
			}
		})
	}
	if _, err := NewExchangeResolver(map[string]ExchangeRegistration{"trusted": base}, issue, check, nil); err == nil {
		t.Fatal("regular connector accepted synthetic exception")
	}
}
