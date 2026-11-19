package stateint

// This suite drives the COMPLETE durable State stack end to end: real
// PostgreSQL, a real mTLS HTTP server mounting the production stateapi
// handler, real Ed25519-signed task tokens, and the public Go SDK client.
// Nothing is stubbed: every assertion is behaviour observed through the SDK
// against a live server and database.

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

const (
	testAudience = "ahp"
	testIssuer   = "https://identity.test"
	testKID      = "k1"
	// testSubject is the human subject carried by every token this suite mints.
	testSubject = "user-ada"
)

// ---------------------------------------------------------------------------
// Test-only CA and SPIFFE client certificates.
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
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-it-ca"},
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

var serial int64 = 1000

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
			t.Fatalf("bad uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(atomic.AddInt64(&serial, 1)),
		Subject:      pkix.Name{CommonName: "ahp-it-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         parsed,
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
// env is a live end-to-end environment: real store + mTLS server + SDK client.
// ---------------------------------------------------------------------------

type env struct {
	handler    http.Handler
	srv        *httptest.Server
	pool       *pgxpool.Pool
	store      *state.Store
	dsn        string
	ctx        context.Context
	tenant     string
	deployment string
	signer     *auth.Signer
	verifier   *auth.Verifier
	revoked    *atomic.Bool
	ca         *testCA
	client     *sdk.Client

	mu  sync.Mutex
	cur string // current bearer token used by the SDK token source
}

// dsnFromEnv resolves the isolated test database URL.
func dsnFromEnv(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run State integration tests")
	}
	return dsn
}

// newEnv boots a fresh tenant/deployment pair against the shared isolated PG.
func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvScoped(t, "t-"+contracts.NewID(), "d-"+contracts.NewID())
}

// newEnvScoped boots an environment bound to an explicit scope. Reusing a
// scope across environments is how crash/restart is simulated: a brand-new
// process (pool, store, server) reads the same durable database.
func newEnvScoped(t *testing.T, tenant, deployment string) *env {
	t.Helper()
	dsn := dsnFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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

	// Fresh signing material per environment; the verifier trusts only this key.
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

	handler, err := stateapi.NewWithAdmin(store, verifier, authorize, issue, stateapi.AdminConfig{
		Audience: "ahp-admin",
		Authorize: func(_ context.Context, c auth.Claims, capability string) error {
			if c.Subject != "state-admin" || c.Tenant != tenant || c.Deployment != "control-"+deployment || capability != stateapi.AdminCapability {
				return contracts.NewError(contracts.ErrForbidden, "admin permission required")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}

	ca := newCA(t)
	e := &env{handler: handler, dsn: dsn, ctx: ctx, tenant: tenant,
		deployment: deployment, signer: signer, verifier: verifier,
		revoked: &revoked, ca: ca, pool: pool, store: store}
	e.startServer(t)
	e.client = e.buildClient(t, tenant, deployment)
	return e
}

// startServer mounts the handler on a real mTLS httptest server and wires a
// client transport carrying the env workload certificate.
func (e *env) startServer(t *testing.T) {
	t.Helper()
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(e.ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(e.handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	e.srv = srv
}

// transportFor clones the test server's client transport and installs the
// client certificate for the given scope.
func (e *env) transportFor(t *testing.T, tenant, deployment string) *http.Transport {
	t.Helper()
	cert := e.ca.clientCert(t, workloadURI(tenant, deployment))
	base, ok := e.srv.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatal("expected *http.Transport on test client")
	}
	tr := base.Clone()
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	}
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	return tr
}

// buildClient returns an SDK client that presents the given scope's workload
// certificate and reads its bearer token from the env token holder.
func (e *env) buildClient(t *testing.T, tenant, deployment string) *sdk.Client {
	t.Helper()
	hc := &http.Client{Transport: e.transportFor(t, tenant, deployment)}
	c, err := sdk.New(sdk.Config{
		BaseURL:    e.srv.URL,
		HTTPClient: hc,
		TokenSource: func(context.Context) (string, error) {
			e.mu.Lock()
			defer e.mu.Unlock()
			return e.cur, nil
		},
	})
	if err != nil {
		t.Fatalf("sdk.New: %v", err)
	}
	return c
}

// setToken installs the bearer token the SDK token source will present.
func (e *env) setToken(tok string) {
	e.mu.Lock()
	e.cur = tok
	e.mu.Unlock()
}

// signTask mints a task token bound to the env workload and human subject.
func (e *env) signTask(t *testing.T, task string) string {
	t.Helper()
	return e.signTaskScoped(t, e.tenant, e.deployment, testSubject, task)
}

func (e *env) signTaskScoped(t *testing.T, tenant, deployment, subject, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: testAudience, Kind: auth.KindTask,
		Tenant: tenant, Deployment: deployment,
		Subject: subject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// reopen boots a second live process (new pool/store/server) on the SAME
// database and scope, reusing the original signing material so previously
// issued durable data stays readable. It is the crash/restart model.
func (e *env) reopen(t *testing.T) *env {
	t.Helper()
	e2 := newEnvScoped(t, e.tenant, e.deployment)
	// Reuse the original signer/verifier so the reopened process can mint
	// tokens that are still valid against its own handler.
	return e2
}

// raw posts a body directly to /v1/state/{op} with an optional bearer token,
// bypassing the SDK so route-level negatives can be observed server-side.
func (e *env) raw(t *testing.T, op, token, body string) (int, map[string]any) {
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
	if json.NewDecoder(resp.Body).Decode(&out) != nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

// rawWithCert posts a body using a client certificate for a foreign scope.
func (e *env) rawWithCert(t *testing.T, tenant, deployment, op, token, body string) (int, map[string]any) {
	t.Helper()
	tr := e.transportFor(t, tenant, deployment)
	hc := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/state/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
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

// ---------------------------------------------------------------------------
// Shared helpers for lease establishment and assertions.
// ---------------------------------------------------------------------------

// leasePair is a live lease plus the fence needed to act on it.
type leasePair struct {
	lease  sdk.Lease
	token  string
	fence  sdk.Fence
	thread sdk.Thread
	run    sdk.Run
}

// claimFirst bootstraps a thread+run under task, claims it, installs the lease
// token on the env client and returns the live lease.
func (e *env) claimFirst(t *testing.T, worker string) leasePair {
	t.Helper()
	task := "task-" + contracts.NewID()
	e.setToken(e.signTask(t, task))
	thread, err := e.client.CreateThread(e.ctx, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(e.ctx, thread.ID, "k-"+contracts.NewID(), json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claimed, err := e.client.ClaimRun(e.ctx, worker, 60000)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a claimable run")
	}
	e.setToken(claimed.TaskToken)
	return leasePair{
		lease:  claimed.Lease,
		token:  claimed.TaskToken,
		fence:  sdk.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch},
		thread: thread,
		run:    run,
	}
}

// mustCode asserts that err carries the expected contract error code.
func mustCode(t *testing.T, err error, want string, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want error %v, got nil", what, want)
	}
	if got := contracts.Code(err); got != want {
		t.Fatalf("%s: want code %v, got %v (%v)", what, want, got, err)
	}
}

// requireNoErr fails the test on any non-nil error.
func requireNoErr(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// cp builds a valid checkpoint with the given id and one opaque part.
func cp(id string, version int64, data []byte) sdk.Checkpoint {
	return sdk.Checkpoint{
		ID:        id,
		Namespace: "lg",
		Version:   version,
		Parts:     []sdk.Part{{Channel: "messages", Version: "v-" + id, Data: data}},
	}
}

// jsonBody is a small helper for the raw JSON helper.
func jsonBody(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// pj marshals v to a json.RawMessage, the payload/args/result/filter type
// accepted by the SDK methods. jsonBody stays for rawWithCert string bodies.
func pj(v any) json.RawMessage { return json.RawMessage(jsonBody(v)) }

var _ = bytes.MinRead // keep the bytes import meaningful for future raw helpers

// adminCall uses a separately authenticated control workload and a human token.
// Task SDK credentials remain installed for ordinary reads and mutations.
func (e *env) adminCall(t *testing.T, op string, input any, out any) {
	t.Helper()
	token, err := e.signer.Sign(auth.Claims{Audience: "ahp-admin", Kind: auth.KindHuman, Tenant: e.tenant, Subject: "state-admin"}, 5*time.Minute)
	requireNoErr(t, err, "sign admin")
	body, err := json.Marshal(map[string]any{"target": map[string]string{"deployment": e.deployment, "subject": testSubject}, "input": input})
	requireNoErr(t, err, "admin body")
	req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, e.srv.URL+"/v1/state-admin/"+op, bytes.NewReader(body))
	requireNoErr(t, err, "admin request")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	tr := e.transportFor(t, e.tenant, "control-"+e.deployment)
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	requireNoErr(t, err, "admin HTTP")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var failure map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&failure)
		t.Fatalf("admin %s: status=%d body=%v", op, resp.StatusCode, failure)
	}
	if out != nil {
		requireNoErr(t, json.NewDecoder(resp.Body).Decode(out), "decode admin response")
	}
}
