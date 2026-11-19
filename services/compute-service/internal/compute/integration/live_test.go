// Package integration_test is the live-verification suite for the VM2
// compute-workspace lane (PDF 3.6 Computer B49-B53 + Workspace Store B152
// compute-side duties). It runs against the dedicated live PostgreSQL
// database and the real Redis instance; both are mandatory — the suite fails
// (never skips) when they are missing, so a green run is a live run.
//
// Substrate: command execution goes through the existing host backend on this
// machine (no VM-in-VM). Throwaway state (jails, environment prefixes, TLS
// material, transcripts) lives under the workdir only.
package integration_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	testIssuer   = "https://identity.vm2compute.test"
	testKID      = "k1"
	redisDefault = "127.0.0.1:16379"
	smokeListen  = "127.0.0.1:54203"
)

// repoRoot is the source root of this checkout: the directory holding go.mod,
// found by walking up from the test binary's working directory. A fixed number
// of "../" steps encodes one particular disk layout and nothing else, and that
// layout (a lane workdir with the repository inside it as src/) only exists on
// the VM nodes; a plain checkout or a git worktree elsewhere resolves to a
// directory that is not there. AHP_COMPUTE_TEST_REPO_ROOT overrides the walk.
func repoRoot(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("AHP_COMPUTE_TEST_REPO_ROOT"); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			t.Fatalf("AHP_COMPUTE_TEST_REPO_ROOT=%s: %v", v, err)
		}
		if _, err := os.Stat(filepath.Join(abs, "go.mod")); err != nil {
			t.Fatalf("AHP_COMPUTE_TEST_REPO_ROOT=%s has no go.mod: %v", v, err)
		}
		return abs
	}
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod above %s: cannot locate the repository root", dir)
		}
		dir = parent
	}
}

// workdirRoot is the lane workdir that holds throwaway state (jails,
// environment prefixes, TLS material). On the VM nodes the repository lives at
// <workdir>/src, so the workdir is the checkout's parent and stays exactly
// where it always was; outside that layout the workdir is the lane directory
// under the user's home (or the temp dir when there is no home), never
// somewhere inside the checkout. AHP_COMPUTE_TEST_WORKDIR overrides it.
func workdirRoot(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("AHP_COMPUTE_TEST_WORKDIR"); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			t.Fatalf("AHP_COMPUTE_TEST_WORKDIR=%s: %v", v, err)
		}
		return abs
	}
	root := repoRoot(t)
	if filepath.Base(root) == "src" {
		return filepath.Dir(root)
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(os.TempDir(), "ahp-handbook-compute")
	}
	return filepath.Join(home, "ahp-handbook-compute")
}

// liveDSN fails the suite (no offline fallback) when the dedicated test
// database is not configured, and otherwise hands out this run's isolated
// database (isolation_test.go) so concurrent suites cannot contend over one
// schema. The returned DSN is also what the smoke test gives to the real
// compute-host process it spawns, so service and fixtures share one database.
func liveDSN(t *testing.T) string {
	t.Helper()
	return isolatedDSN(t)
}

func liveRedisAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("AHP_REDIS_ADDR")
	if addr == "" {
		addr = redisDefault
	}
	redis := compute.NewRedis(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redis.Ping(ctx); err != nil {
		t.Fatalf("live redis at %s unreachable: %v", addr, err)
	}
	return addr
}

// ctx bounds one test.
func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return c
}

// scope builds an isolated live scope (unique tenant/deployment per test).
func scope(subject string) contracts.Scope {
	return contracts.Scope{
		Tenant:     "t-" + contracts.NewID(),
		Deployment: "d-" + contracts.NewID(),
		Subject:    subject,
	}
}

// liveEnv wires store + runner + redis lease layer against the live PG and
// Redis, with a throwaway jail base inside the workdir.
type liveEnv struct {
	t          *testing.T
	pool       *pgxpool.Pool
	store      *compute.Store
	redis      *compute.Redis
	leases     *compute.LeaseService
	runner     *compute.Runner
	jailBase   string
	tenant     string
	deployment string
	signer     *auth.Signer
}

func newLiveEnv(t *testing.T) *liveEnv {
	t.Helper()
	dsn := liveDSN(t)
	addr := liveRedisAddr(t)
	c := ctx(t)

	pool, err := pgxpool.New(c, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := compute.New(pool)
	if err := store.Migrate(c); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	redis := compute.NewRedis(addr)
	leases, err := compute.NewLeaseService(store, redis)
	if err != nil {
		t.Fatalf("lease service: %v", err)
	}

	jailBase := filepath.Join(workdirRoot(t), "run", "jail-"+contracts.NewID())
	if err := os.MkdirAll(jailBase, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(jailBase) })

	registry := compute.NewRegistry(compute.NewHostBackend(jailBase))
	runner := compute.NewRunner(store, registry, jailBase, "test-node", nil).WithLeaseService(leases)

	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	_ = pub
	return &liveEnv{
		t: t, pool: pool, store: store, redis: redis, leases: leases,
		runner: runner, jailBase: jailBase, tenant: tenant,
		deployment: deployment, signer: signer,
	}
}

func (e *liveEnv) mustCreatePersonal(t *testing.T, sc contracts.Scope, realm string) compute.Computer {
	t.Helper()
	c, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: realm,
	}, "key-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer(personal): %v", err)
	}
	return c
}

// mustRun executes a command on the Computer through the live runner.
func (e *liveEnv) mustRun(t *testing.T, sc contracts.Scope, computerID, command string) compute.CommandRecord {
	t.Helper()
	rec, err := e.runner.RunCommand(ctx(t), sc, computerID, "run-"+contracts.NewID(), command, "")
	if err != nil {
		t.Fatalf("RunCommand(%q): %v", command, err)
	}
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("RunCommand(%q) status=%s stderr=%q", command, rec.Status, rec.Stderr)
	}
	return rec
}

func (e *liveEnv) home(computerID string) string {
	return filepath.Join(e.jailBase, "computers", computerID, "home")
}

// ---------------------------------------------------------------------------
// mTLS fixtures (same pattern as tests/compute): CA, workload client certs
// with SPIFFE URIs, and a signer for task tokens.
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
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "vm2compute-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
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
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "vm2compute-test-client"},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (ca *testCA) serverCert(t *testing.T, host string, ip net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: host},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{host},
		IPAddresses: []net.IP{ip},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func workloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

func signTask(t *testing.T, signer *auth.Signer, tenant, deployment, subject, task string) string {
	t.Helper()
	tok, err := signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask,
		Tenant: tenant, Deployment: deployment,
		Subject: subject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// liveHTTPEnv is an in-process mTLS server fronting computeapi with live
// store/runner — used for the store-level HTTP assertions (the separate
// real-binary smoke test covers 127.0.0.1:54203).
type liveHTTPEnv struct {
	t          *testing.T
	e          *liveEnv
	srv        *http.Server
	url        string
	ca         *testCA
	client     *http.Client
	tenant     string
	deployment string
	signer     *auth.Signer
}

func newLiveHTTPEnv(t *testing.T) *liveHTTPEnv {
	t.Helper()
	e := newLiveEnv(t)
	return newLiveHTTPEnvFor(t, e)
}

func newLiveHTTPEnvFor(t *testing.T, e *liveEnv) *liveHTTPEnv {
	t.Helper()
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{
		testKID: e.signer.Public(),
	})
	authorize := func(context.Context, auth.Claims, string) error { return nil }

	h, err := computeapi.New(e.store, verifier, authorize, e.runner)
	if err != nil {
		t.Fatalf("computeapi.New: %v", err)
	}
	ca := newCA(t)
	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	client := ca.clientCert(t, workloadURI(tenant, deployment))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{
		Handler: h,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{ca.serverCert(t, "127.0.0.1", net.ParseIP("127.0.0.1"))},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPoolFromPEM(t, ca.pem),
			MinVersion:   tls.VersionTLS12,
		},
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = server.ServeTLS(ln, "", "") }()
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shCtx)
	})

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      certPoolFromPEM(t, ca.pem),
			Certificates: []tls.Certificate{client},
			MinVersion:   tls.VersionTLS12,
		},
	}
	return &liveHTTPEnv{
		t: t, e: e, srv: server, url: "https://" + ln.Addr().String(),
		ca: ca, client: &http.Client{Transport: tr, Timeout: 60 * time.Second},
		tenant: tenant, deployment: deployment, signer: e.signer,
	}
}

func certPoolFromPEM(t *testing.T, pemBytes []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("test CA PEM rejected")
	}
	return pool
}

// call posts op with a task token (subject/tenant/deployment from henv) and
// returns status + body.
func (h *liveHTTPEnv) call(t *testing.T, subject, op, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url+"/v1/compute/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", op, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s body: %v", op, err)
	}
	return resp.StatusCode, string(b)
}

// mustStatus asserts the HTTP status of a transcript step.
func mustStatus(t *testing.T, got, want int, body string) {
	t.Helper()
	if got != want {
		t.Fatalf("status = %d, want %d (body: %s)", got, want, body)
	}
}

func fmtBody(op string, status int, body string) string {
	return fmt.Sprintf("[%s] -> %d %s", op, status, body)
}
