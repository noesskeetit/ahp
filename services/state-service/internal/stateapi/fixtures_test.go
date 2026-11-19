package stateapi

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	testAudience = "ahp"
	testIssuer   = "https://identity.test"
	testKID      = "k1"
	// testSubject is the human subject every test token carries. It is used
	// consistently everywhere; task tokens derive their isolation subject from
	// the task claim, never from a colon-separated human id.
	testSubject = "user-ada"
)

// ---------------------------------------------------------------------------
// Fresh test CA and SPIFFE client certificates (test-only material).
// ---------------------------------------------------------------------------

type testCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &testCA{cert: cert, key: key,
		pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *testCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad test uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-test-client"},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func workloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

// ---------------------------------------------------------------------------
// Real-PostgreSQL environment: fresh tenant per test, real mTLS server,
// Ed25519 signer-backed task tokens and a callback issuer that mints real
// signed task tokens for claimed runs.
// ---------------------------------------------------------------------------

type env struct {
	handler    http.Handler
	srv        *httptest.Server
	store      *state.Store
	ctx        context.Context
	tenant     string
	deployment string
	// ca is kept on the env so a test can mint a SECOND workload identity -
	// another tenant on the same server and the same database - which is what
	// the cross-tenant data-plane check needs.
	ca       *testCA
	signer   *auth.Signer
	verifier *auth.Verifier
	revoked  *atomic.Bool
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run stateapi integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := state.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	signerPub, signerPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(testIssuer, testKID, signerPriv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: signerPub})

	var revoked atomic.Bool
	authorize := func(_ context.Context, _ auth.Claims, _ string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "grant revoked")
		}
		return nil
	}
	issue := func(_ context.Context, scope contracts.Scope, taskID string) (string, error) {
		if revoked.Load() {
			return "", contracts.NewError(contracts.ErrForbidden, "grant revoked")
		}
		return signer.Sign(auth.Claims{
			Audience: testAudience, Kind: auth.KindTask,
			Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, Task: taskID, Root: "root-" + taskID,
			Context: "ctx-" + taskID, Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}

	handler, err := NewWithAdmin(store, verifier, authorize, issue, AdminConfig{
		Audience: testAudience, Authorize: func(_ context.Context, claims auth.Claims, capability string) error {
			if claims.Subject != "state-admin" || capability != AdminCapability {
				return contracts.NewError(contracts.ErrForbidden, "not an administrator")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ca := newCA(t)
	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	client := ca.clientCert(t, workloadURI(tenant, deployment))

	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("test CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	// The test-server certificate is trusted through the httptest CA already
	// installed in srv.Client(); only the client identity is added here.
	clientTransport := srv.Client().Transport.(*http.Transport).Clone()
	clientTransport.TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	clientTransport.TLSClientConfig.Certificates = []tls.Certificate{client}
	srv.Client().Transport = clientTransport
	t.Cleanup(srv.Close)
	return &env{handler: handler, srv: srv, store: store, ctx: ctx,
		tenant: tenant, deployment: deployment, ca: ca, signer: signer, verifier: verifier,
		revoked: &revoked}
}

// signTask mints a real Ed25519 task token bound to the env workload.
func (e *env) signTask(t *testing.T, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: testAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: testSubject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// call posts body to /v1/state/{op} with an optional bearer token.
func (e *env) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/state/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	dec := json.NewDecoder(resp.Body)
	if dec.Decode(&out) != nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func mustStatus(t *testing.T, got int, want int, body map[string]any) {
	t.Helper()
	if got != want {
		t.Fatalf("status = %d, want %d (body %v)", got, want, body)
	}
}

// ---------------------------------------------------------------------------
// Unit-level checks that need no database.
// ---------------------------------------------------------------------------

func TestNewRejectsNilDependencies(t *testing.T) {
	store := state.New(nil)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(context.Context, contracts.Scope, string) (string, error) { return "", nil }
	nop := func(h http.Handler) http.Handler { return h }
	_ = nop
	if _, err := New(nil, verifier, authorize, issue); err == nil {
		t.Error("nil store accepted")
	}
	if _, err := New(store, nil, authorize, issue); err == nil {
		t.Error("nil verifier accepted")
	}
	if _, err := New(store, verifier, nil, issue); err == nil {
		t.Error("nil authorize accepted")
	}
	if _, err := New(store, verifier, authorize, nil); err == nil {
		t.Error("nil issuer accepted")
	}
	if _, err := New(store, verifier, authorize, issue); err != nil {
		t.Fatalf("valid dependencies rejected: %v", err)
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	store := state.New(nil)
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(context.Context, contracts.Scope, string) (string, error) { return "", nil }
	h, err := New(store, verifier, authorize, issue)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/v1/state/NoSuchOp"},
		{http.MethodPost, "/v1/state"},
		{http.MethodPost, "/v1/state/CreateThread/extra"},
		{http.MethodGet, "/v1/state/CreateThread"},
		{http.MethodGet, "/v1/state/ClaimRun"},
		{http.MethodDelete, "/v1/state/Whatever"},
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, bytes.NewReader([]byte(`{}`))))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s %s: status %d, want 404", tc.method, tc.path, rec.Code)
		}
	}
}

func TestTTLConversion(t *testing.T) {
	if d, err := ttl(0); err != nil || d != 30*time.Second {
		t.Fatalf("default ttl: %v, %v", d, err)
	}
	if d, err := ttl(1500); err != nil || d != 1500*time.Millisecond {
		t.Fatalf("explicit ttl: %v, %v", d, err)
	}
	if _, err := ttl(-1); err == nil {
		t.Fatal("negative ttl accepted")
	}
}
