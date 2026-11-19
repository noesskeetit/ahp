package registry

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type boundaryOwnerReader struct{}

func (boundaryOwnerReader) ObserveOwned(context.Context, contracts.Scope, EffectiveConfig) (json.RawMessage, error) {
	return nil, nil
}

// This isolates the HTTP credential boundary; owner/session and State isolation
// are covered by the real PostgreSQL composition test in cmd/ahp.
func TestOnlyInvokedHistoryAcceptsHumanCredential(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "test", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	handler, err := NewHTTPHandler(&Store{}, verifier, authorize)
	if err != nil {
		t.Fatal(err)
	}
	if err := AttachOwnerObserver(handler, boundaryOwnerReader{}, verifier, "ahp-browser", authorize); err != nil {
		t.Fatal(err)
	}
	h := handler.(*HTTPHandler)
	for name := range h.routes {
		h.routes[name] = func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }
	}
	token, err := signer.Sign(auth.Claims{Audience: "ahp-browser", Kind: auth.KindHuman, Tenant: "tenant", Subject: "owner"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse("spiffe://ahp/tenant/tenant/deployment/space")
	for name := range h.routes {
		r := httptest.NewRequest(http.MethodPost, "/v1/registry/"+name, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		want := http.StatusForbidden
		if name == "GetInvokedTask" {
			want = http.StatusNoContent
		}
		if w.Code != want {
			t.Fatalf("%s human status=%d want=%d", name, w.Code, want)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/registry/GetInvokedTask", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("history without workload certificate: %d", w.Code)
	}
}
