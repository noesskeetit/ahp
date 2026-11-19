package ahpbridge

import (
	"bytes"
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

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// Recorder tests the production JWT/mTLS-metadata guard and browser RPC path.
// It does not claim a real TLS handshake or certificate-chain validation test.
func TestBrowserRPCUsesVerifiedTransportAndReturnsQueuedOperation(t *testing.T) {
	f := fixture(t)
	f.claims.CtxScope = auth.CtxScopeAuthority
	f.claims.Audience = "ahp"
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp-test", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Sign(f.claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	f.access.d.Source = func(context.Context, auth.Claims, string, string) error { return nil }
	service := memory.New(memory.NewMemoryStore(), nil, f.access)
	handler, err := NewHandler(service, auth.NewVerifier("ahp-test", map[string]ed25519.PublicKey{"key": pub}), f.access.d.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse(auth.WorkloadID(f.claims.Scope()))
	call := func(path, body string, certificate bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "https://memory.internal/v1/memory/"+path, bytes.NewBufferString(body))
		r.Header.Set("Authorization", "Bearer "+token)
		if certificate {
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := call("Settings", `{}`, false); w.Code != 403 {
		t.Fatal("missing mTLS metadata accepted", w.Code)
	}
	if w := call("Settings", `{}`, true); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w := call("Remember", `{"target":"personal","text":"I prefer concise answers","op_key":"remember-1"}`, true)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var op memory.Operation
	if err := json.Unmarshal(w.Body.Bytes(), &op); err != nil || op.ID == "" || op.State != "queued" {
		t.Fatal(op, err)
	}
	w = call("Remember", `{"target":"personal","text":"I prefer concise answers","op_key":"remember-1"}`, true)
	var again memory.Operation
	_ = json.Unmarshal(w.Body.Bytes(), &again)
	if w.Code != 202 || again.ID != op.ID {
		t.Fatal("browser retry created duplicate", w.Code, again)
	}
	w = call("Operation", `{"id":"`+op.ID+`","scope":"personal"}`, true)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call("UpdateSettings", `{"personal_enabled":false,"op_key":"off"}`, true)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	f.revoked = true
	if w := call("Settings", `{}`, true); w.Code != 403 {
		t.Fatal("revoked JWT accepted", w.Code)
	}
}
