//go:build e2ehttp

// Real-HTTP SDK integration test: the ACTUAL mac-sdk-ci Go SDK client, driven
// over real mTLS HTTP against the ACTUAL internal/stateapi handler backed by a
// real PostgreSQL. A missingDSN SKIP is NOT treated as a pass — this file only
// produces evidence when AHP_TEST_DATABASE_URL is set.
//
// Authoritative scope boundary (evidence, not claim): the real State HTTP API
// currently mounts 10 routes (CreateThread, GetThread, GetThreadByContext,
// CreateRun, GetRun, ListRuns, Heartbeat, Release, Suspend via task auth; and
// ClaimRun via workload cert). The SDK's KV/intent/checkpoint/control/inbox/
// stream operations have NO HTTP route yet (state-owner scope). This test drives
// the wired lifecycle end-to-end and asserts an unrouted op returns the server's
// unknown-route error rather than pretending the SDK path is HTTP-supported.
package sdk_test

import (
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
	"net/url"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

const httpAudience = "ahp"

type caBundle struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newHTTPCA(t *testing.T) *caBundle {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-sdk-e2ehttp-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	return &caBundle{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *caBundle) workloadCert(t *testing.T, tenant, deployment string) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u, _ := url.Parse("spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-sdk-e2ehttp-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: []*url.URL{u},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, _ := x509.ParseCertificate(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func startTLSServer(t *testing.T, h http.Handler, clientCAs *x509.CertPool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	return srv
}

func TestE2EHTTPSdkLifecycle(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("e2ehttp requires AHP_TEST_DATABASE_URL (SKIP is not an accepted result)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	store := state.New(pool)
	for _, m := range []func(context.Context) error{
		store.Migrate, store.MigrateCheckpoints, store.MigrateIntents, store.MigrateEvents, store.MigrateKV,
	} {
		if err := m(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}

	// Ed25519 signer/verifier + a real task issuer.
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := auth.NewSigner("https://identity.test", "k1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := auth.NewVerifier("https://identity.test", map[string]ed25519.PublicKey{"k1": pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, scope contracts.Scope, taskID string) (string, error) {
		return signer.Sign(auth.Claims{
			Audience: httpAudience, Kind: auth.KindTask, Tenant: scope.Tenant,
			Deployment: scope.Deployment, Subject: scope.Subject, Task: taskID,
			Root: "root-" + taskID, Context: "ctx-" + taskID, Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}
	handler, err := stateapi.New(store, verifier, authorize, issue)
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}

	tenant := "sdkhttp-" + contracts.NewID()
	deployment := "dep-" + contracts.NewID()
	ca := newHTTPCA(t)
	clientCert := ca.workloadCert(t, tenant, deployment)

	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	srv := startTLSServer(t, handler, serverCAs)
	defer srv.Close()

	// An http.Client carrying mTLS material that trusts the test server.
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(srv.Certificate())
	tlsClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: rootCAs, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12},
	}}

	taskID := "task-" + contracts.NewID()
	mint := func(subject, task string) string {
		tok, err := signer.Sign(auth.Claims{
			Audience: httpAudience, Kind: auth.KindTask, Tenant: tenant, Deployment: deployment,
			Subject: subject, Task: task, Root: "root-" + task, Context: "ctx-" + task, Grant: "g-" + task, Depth: 1,
		}, 5*time.Minute)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		return tok
	}
	currentToken := mint("user-sdk", taskID)
	client, err := sdkstate.New(sdkstate.Config{
		BaseURL: srv.URL, HTTPClient: tlsClient,
		TokenSource: func(context.Context) (string, error) { return currentToken, nil },
	})
	if err != nil {
		t.Fatalf("sdk New: %v", err)
	}

	// --- wired lifecycle over REAL HTTP + REAL PG ---
	thread, err := client.CreateThread(ctx, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread over real HTTP: %v", err)
	}
	if thread.Scope.Tenant != tenant || thread.Scope.Deployment != deployment {
		t.Fatalf("scope not derived from transport: %+v", thread.Scope)
	}
	run, err := client.CreateRun(ctx, thread.ID, "k-"+contracts.NewID(), json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("CreateRun over real HTTP: %v", err)
	}
	if run.TaskID != taskID {
		t.Errorf("run task_id = %q, want %q", run.TaskID, taskID)
	}
	if got, err := client.GetRun(ctx, run.ID); err != nil || got.ID != run.ID {
		t.Fatalf("GetRun over real HTTP: run=%+v err=%v", got, err)
	}
	runs, err := client.ListRuns(ctx, thread.ID)
	if err != nil || len(runs) != 1 {
		t.Fatalf("ListRuns over real HTTP: runs=%v err=%v", runs, err)
	}

	// ClaimRun is workload-cert-only (SDK sends no bearer); it returns a lease and
	// a freshly issued task token, which the SDK hands back for follow-up calls.
	claimed, err := client.ClaimRun(ctx, "w-"+contracts.NewID(), 60000)
	if err != nil {
		t.Fatalf("ClaimRun over real HTTP: %v", err)
	}
	if claimed == nil || claimed.Lease.Epoch < 1 || claimed.TaskToken == "" {
		t.Fatalf("bad claim: %+v", claimed)
	}
	currentToken = claimed.TaskToken // SDK pattern: caller owns/rotates the token.

	fence := sdkstate.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}
	if _, err := client.Heartbeat(ctx, fence, 60000); err != nil {
		t.Fatalf("Heartbeat over real HTTP: %v", err)
	}
	if err := client.Release(ctx, fence, "done"); err != nil {
		t.Fatalf("Release over real HTTP: %v", err)
	}

	// --- authoritative gap boundary: unrouted SDK ops are NOT HTTP-supported ---
	if _, err := client.PutKV(ctx, fence, []string{"sdk", "ns"}, "k", "op-1", 0, json.RawMessage(`"v"`)); err == nil {
		t.Fatal("PutKV unexpectedly succeeded — a KV route now exists; update this boundary test + gap report")
	} else if contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("PutKV over real HTTP code = %s (err=%v), want NOT_FOUND (unknown route): KV/intent/checkpoint HTTP routes are state-owner scope", contracts.Code(err), err)
	}
}
