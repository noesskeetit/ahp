// Package compute_test is the black-box acceptance suite for the Compute plane
// (class S/C/X computers, disposable incarnations, volume fencing, activity
// leases, the durable command ledger, snapshots, environment & project
// copy-on-write revisions, artifact exports, and the HTTP facade).
//
// Two verification levels are kept strictly separate, mirroring the block's
// contract:
//
//   - VERIFIED here: the store state machine, security policy, the host
//     backend (a real process executed on this machine), and the HTTP wire
//     paths over real mTLS + Ed25519 task tokens. These run against the
//     isolated PostgreSQL schema ahp_hive_compute.
//
//   - NOT VERIFIED here: the microVM (Firecracker/Kata) isolation itself. This
//     Mac has no /dev/kvm, so class X refuses to run (fail-closed) rather than
//     pretend. Those paths are exercised only as a fake runtime (to prove the
//     supervisor state machine, not VM isolation) and as a reproducible
//     Linux/KVM acceptance runner under infra/compute. See docs/compute.md.
package compute_test

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

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	testAudience = "ahp"
	testIssuer   = "https://identity.test"
	testKID      = "k1"
	testSubject  = "user-ada"
)

// dsn resolves the isolated test database. Tests that need PostgreSQL skip
// (rather than fail) when no DSN is configured, so the suite stays green in
// environments without a database.
func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("AHP_TEST_DATABASE_URL")
	if v == "" {
		v = os.Getenv("DATABASE_URL")
	}
	if v == "" {
		t.Skip("set DATABASE_URL (or AHP_TEST_DATABASE_URL) to run Compute integration tests")
	}
	return v
}

// ctx returns a context bounded to a generous deadline for a whole test.
func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// newStore returns a migrated Store against the isolated schema.
func newStore(t *testing.T) *compute.Store {
	t.Helper()
	pool, err := pgxpool.New(ctx(t), dsn(t))
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	st := compute.New(pool)
	if err := st.Migrate(ctx(t)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return st
}

// scope builds a fresh, isolated scope. Each test uses unique tenant/deployment
// values so concurrent runs on the shared schema never collide; the values are
// SPIFFE-path-safe (letters/digits/dash) so they can also serve as workload
// identities in the HTTP tests.
func scope() contracts.Scope {
	return contracts.Scope{
		Tenant:     "t-" + contracts.NewID(),
		Deployment: "d-" + contracts.NewID(),
		Subject:    testSubject,
	}
}

func mustCreate(t *testing.T, st *compute.Store, sc contracts.Scope, class compute.Class, profile compute.Profile, workspaceID string) compute.Computer {
	t.Helper()
	c, err := st.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: class, Profile: profile, WorkspaceID: workspaceID,
	}, "key-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer(%s/%s): %v", class, profile, err)
	}
	return c
}

// ---------------------------------------------------------------------------
// mTLS fixtures for the HTTP facade (mirrors the accepted stateapi pattern).
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

// apiEnv is a live mTLS server fronting computeapi.New with a real store, an
// Ed25519 signer and an optional execution runner.
type apiEnv struct {
	srv        *httptest.Server
	store      *compute.Store
	ctx        context.Context
	tenant     string
	deployment string
	signer     *auth.Signer
	revoked    *atomic.Bool
}

func newAPIEnv(t *testing.T, runner *compute.Runner) *apiEnv {
	t.Helper()
	pool, err := pgxpool.New(ctx(t), dsn(t))
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := compute.New(pool)
	if err := store.Migrate(ctx(t)); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})

	var revoked atomic.Bool
	authorize := func(_ context.Context, _ auth.Claims, _ string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "grant revoked")
		}
		return nil
	}
	handler, err := computeapi.New(store, verifier, authorize, runner)
	if err != nil {
		t.Fatalf("computeapi.New: %v", err)
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
	transport := srv.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.Certificates = []tls.Certificate{client}
	srv.Client().Transport = transport
	t.Cleanup(srv.Close)
	return &apiEnv{srv: srv, store: store, ctx: ctx(t),
		tenant: tenant, deployment: deployment, signer: signer, revoked: &revoked}
}

func (e *apiEnv) signTask(t *testing.T, task string) string {
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

func (e *apiEnv) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/compute/"+op, strings.NewReader(body))
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
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func mustStatus(t *testing.T, got, want int, body map[string]any) {
	t.Helper()
	if got != want {
		t.Fatalf("status = %d, want %d (body %v)", got, want, body)
	}
}

// jsonBody is a tiny helper for building request bodies from a map literal.
func jsonBody(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// keep bytes importable in files that use only some helpers.
var _ = bytes.NewReader
