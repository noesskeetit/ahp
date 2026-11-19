package sdk

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// TLS VerifiedChains here is an explicit middleware-boundary fixture. Actual
// TLS handshakes, A2A and PostgreSQL are exercised in the real-State test.
func TestNativeIngressBindsVerifiedRequestCredential(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("native-ingress-test", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: "native-unit", Deployment: "agent", Subject: "alice", Task: "task", Context: "conversation", Root: "root", Grant: "grant"}
	token, err := signer.Sign(claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"valid", "missing-tls", "unverified-tls", "wrong-peer", "ambiguous-peer", "missing-jwt", "duplicate-jwt", "tampered", "wrong-workload", "wrong-audience", "human", "revoked", "authorization-unavailable"} {
		t.Run(name, func(t *testing.T) {
			calls, authorizations := 0, 0
			cfg := NativeIngressConfig{Tenant: "native-unit", Deployment: "agent", GatewayDeployment: "gateway", Audience: "ahp", Verifier: auth.NewVerifier("native-ingress-test", map[string]ed25519.PublicKey{"key": pub}), Authorize: func(_ context.Context, actual auth.Claims) error {
				authorizations++
				if actual.Scope() != claims.Scope() || actual.Task != "task" {
					t.Error("authorization received a different task")
				}
				if name == "revoked" {
					return contracts.NewError(contracts.ErrForbidden, "sensitive authorization diagnostic")
				}
				if name == "authorization-unavailable" {
					return contracts.NewError(contracts.ErrUnavailable, "sensitive authorization diagnostic")
				}
				return nil
			}}
			handler, err := NewNativeIngress(cfg, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				actual, err := NativeTaskTokenSource(r.Context())
				principal, ok := transport.FromContext(r.Context())
				if err != nil || actual != token || !ok || principal.Scope != claims.Scope() || principal.Claims.Task != "task" {
					t.Error("verified request credential/principal lost")
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "https://agent/a2a", nil)
			peer, _ := url.Parse("spiffe://ahp/tenant/native-unit/deployment/gateway")
			leaf := &x509.Certificate{URIs: []*url.URL{peer}}
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{leaf}}}
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("X-Workload-Id", peer.String()) // never identity authority
			switch name {
			case "missing-tls":
				r.TLS = nil
			case "unverified-tls":
				r.TLS.VerifiedChains = nil
			case "wrong-peer":
				other, _ := url.Parse("spiffe://ahp/tenant/native-unit/deployment/other")
				leaf.URIs = []*url.URL{other}
			case "ambiguous-peer":
				leaf.URIs = append(leaf.URIs, peer)
			case "missing-jwt":
				r.Header.Del("Authorization")
			case "duplicate-jwt":
				r.Header.Add("Authorization", "Bearer "+token)
			case "tampered":
				r.Header.Set("Authorization", "Bearer "+token+"x")
			case "wrong-workload", "wrong-audience", "human":
				foreign := claims
				if name == "wrong-workload" {
					foreign.Deployment = "other"
				}
				if name == "wrong-audience" {
					foreign.Audience = "other"
				}
				if name == "human" {
					foreign = auth.Claims{Audience: "ahp", Kind: auth.KindHuman, Tenant: "native-unit", Subject: "alice"}
				}
				other, err := signer.Sign(foreign, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("Authorization", "Bearer "+other)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			want := http.StatusForbidden
			if name == "valid" {
				want = http.StatusNoContent
			}
			if name == "authorization-unavailable" {
				want = http.StatusServiceUnavailable
			}
			if w.Code != want || name != "valid" && calls != 0 || name == "valid" && calls != 1 {
				t.Fatalf("ingress %s: status=%d calls=%d", name, w.Code, calls)
			}
			if strings.Contains(w.Body.String(), token) || strings.Contains(w.Body.String(), "sensitive") {
				t.Fatal("ingress leaked credential or owner diagnostics")
			}
			if name == "valid" || name == "revoked" || name == "authorization-unavailable" {
				if authorizations != 1 {
					t.Fatalf("active authorization checks=%d", authorizations)
				}
			} else if authorizations != 0 {
				t.Fatal("unverified request reached active authorization")
			}
		})
	}
	if _, err := NativeTaskTokenSource(context.Background()); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("token source accepted context outside verified ingress")
	}
}

func TestNativeIngressRejectsIncompleteConfiguration(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"tenant", "deployment", "gateway", "audience", "verifier", "authorizer", "handler"} {
		t.Run(name, func(t *testing.T) {
			cfg := NativeIngressConfig{Tenant: "tenant", Deployment: "agent", GatewayDeployment: "gateway", Audience: "ahp", Verifier: auth.NewVerifier("issuer", map[string]ed25519.PublicKey{"key": pub}), Authorize: func(context.Context, auth.Claims) error { return nil }}
			var next http.Handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("invalid ingress executed") })
			switch name {
			case "tenant":
				cfg.Tenant = "../tenant"
			case "deployment":
				cfg.Deployment = ""
			case "gateway":
				cfg.GatewayDeployment = ""
			case "audience":
				cfg.Audience = ""
			case "verifier":
				cfg.Verifier = nil
			case "authorizer":
				cfg.Authorize = nil
			case "handler":
				next = nil
			}
			if _, err := NewNativeIngress(cfg, next); err == nil {
				t.Fatal("incomplete native ingress accepted")
			}
		})
	}
}
