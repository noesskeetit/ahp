package execution_test

// a2a_harness_test.go -- a REAL State API on isolated PostgreSQL, reached over
// mTLS with a SPIFFE workload certificate, plus an Ed25519 signer/verifier that
// mints genuine KindTask tokens. This is the authoritative State service the
// Space Coordinator drives; nothing about State is faked. The mTLS client cert
// (transport.Workload/transport.Task require a verified workload certificate) is
// TEST-ONLY PKI generated in-process; it is not the production PKI and touches
// no live deployment.

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
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	a2aAudience = "ahp"
	a2aIssuer   = "https://identity.test"
	a2aKID      = "a2a-k1"
)

type a2aCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func a2aNewCA(t *testing.T) *a2aCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-a2a-test-ca"},
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
	return &a2aCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func workloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

func (ca *a2aCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
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
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-a2a-test-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
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

// a2aEnv is one real State service over isolated PostgreSQL + mTLS + task tokens.
type a2aEnv struct {
	srv        *httptest.Server
	ctx        context.Context
	tenant     string
	deployment string
	ca         *a2aCA
	signer     *auth.Signer
	verifier   *auth.Verifier
	client     *space.StateClient
}

func newA2AEnv(t *testing.T) *a2aEnv {
	t.Helper()
	dsn := requireRealDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := state.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("state Migrate: %v", err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(a2aIssuer, a2aKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(a2aIssuer, map[string]ed25519.PublicKey{a2aKID: pub})

	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, sc contracts.Scope, taskID string) (string, error) {
		return signer.Sign(auth.Claims{
			Audience: a2aAudience, Kind: auth.KindTask,
			Tenant: sc.Tenant, Deployment: sc.Deployment,
			Subject: sc.Subject, Task: taskID, Root: "root-" + taskID,
			Context: "ctx-" + taskID, Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}
	handler, err := stateapi.New(store, verifier, authorize, issue)
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}

	ca := a2aNewCA(t)
	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	client := ca.clientCert(t, workloadURI(tenant, deployment))

	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{client}
	srv.Client().Transport = tr
	t.Cleanup(srv.Close)

	return &a2aEnv{srv: srv, ctx: ctx, tenant: tenant, deployment: deployment,
		ca: ca, signer: signer, verifier: verifier,
		client: space.NewStateClient(srv.URL, srv.Client())}
}

// taskToken mints a KindTask token bound to the env workload and the given subject.
func (e *a2aEnv) taskToken(t *testing.T, subject, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: a2aAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: subject, Task: task, Root: "root-" + task,
		Context: "ctx-" + task, Grant: "grant-" + task, Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *a2aEnv) scope(subject string) contracts.Scope {
	return contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: subject}
}

// requireRealDB enforces the fail-closed discipline for real dependencies: a
// missing real PostgreSQL is a hard FAIL with a reason, never a silent green.
// The ONLY way to downgrade to a skip is the explicit opt-out
// AHP_ALLOW_SKIP_REAL_DB=1, which prints the unmistakable SKIP_EXPLICIT marker
// that acceptance must not count as a pass.
//
// It is also the one place this package resolves a DSN, so it is where the
// per-run database from isolation_test.go is handed out: the isolation covers
// every pool in the package through this single choke point, and the
// fail/skip semantics below are unchanged by it.
func requireRealDB(t *testing.T) string {
	t.Helper()
	dsn := executionDSN()
	if dsn != "" {
		isolateRunDatabase(dsn)
		t.Log("execution lane database isolation: " + isolateNote)
		if isolateRun != "" {
			return isolateRun
		}
		return dsn
	}
	if os.Getenv("AHP_ALLOW_SKIP_REAL_DB") == "1" {
		t.Log("SKIP_EXPLICIT AHP_TEST_DATABASE_URL unset -- opt-out skip, NOT counted as acceptance")
		t.Skip("SKIP_EXPLICIT: real PostgreSQL required for the A2A journey")
	}
	t.Fatalf("AHP_TEST_DATABASE_URL (or DATABASE_URL) is required for the real A2A journey; refusing to fake a pass. " +
		"Set AHP_ALLOW_SKIP_REAL_DB=1 only to downgrade this to a SKIP_EXPLICIT skip.")
	return ""
}

// requireArtifactPlane enforces the same fail-closed discipline for the object
// storage (Artifacts) dependency: a configured artifact plane is a real,
// required dependency of the executor-backed journey, so a missing one is a hard
// FAIL that names the exact env it needs -- never a silent pass that crashes
// later in an unrelated assertion with a bare "not configured". The only
// downgrade is the explicit opt-out AHP_ALLOW_SKIP_REAL_DB=1, which prints the
// unmistakable SKIP_EXPLICIT marker that acceptance must not count as a pass.
func requireArtifactPlane(t *testing.T) {
	t.Helper()
	var missing []string
	for _, key := range []string{"AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return
	}
	if os.Getenv("AHP_ALLOW_SKIP_REAL_DB") == "1" {
		t.Log("SKIP_EXPLICIT artifact plane unset (" + strings.Join(missing, ", ") + ") -- opt-out skip, NOT counted as acceptance")
		t.Skip("SKIP_EXPLICIT: artifact plane required")
	}
	t.Fatalf("missing artifact plane env: %s -- the executor-backed journey needs a real object store "+
		"(e.g. AHP_S3_ENDPOINT=http://127.0.0.1:19000 + MinIO creds; see env/journey-minio.env). "+
		"Set AHP_ALLOW_SKIP_REAL_DB=1 only to downgrade this to a SKIP_EXPLICIT skip.",
		strings.Join(missing, ", "))
}

// requireLiveQwen enforces the same fail-closed discipline for the LIVE model
// dependency. A quiet skip here would turn "N PASS" into a fake proof: the
// platform claim is a REAL model turn, so without AHP_LIVE_QWEN=1 the test FAILS
// with the reason instead of reporting green. The only downgrade is the explicit
// opt-out AHP_ALLOW_SKIP_LIVE_QWEN=1, which logs the unmistakable SKIP_EXPLICIT
// marker that acceptance must not count as a pass. When the switch IS set, the
// real credential source must exist too: the native Qwen key comes only from the
// scoped Vault runtime secret, so a missing Vault is a named FAIL rather than a
// late, unrelated error inside the model call.
func requireLiveQwen(t *testing.T) {
	t.Helper()
	if os.Getenv("AHP_LIVE_QWEN") != "1" {
		if os.Getenv("AHP_ALLOW_SKIP_LIVE_QWEN") == "1" {
			t.Log("SKIP_EXPLICIT AHP_LIVE_QWEN!=1 -- opt-out skip, NOT counted as acceptance")
			t.Skip("SKIP_EXPLICIT: live Qwen proof not executed (AHP_ALLOW_SKIP_LIVE_QWEN=1)")
		}
		t.Fatalf("AHP_LIVE_QWEN=1 is required: this test is the LIVE model proof and must never pass silently. " +
			"Run it with AHP_LIVE_QWEN=1 plus the real credential (env/shared-vault.env), or set " +
			"AHP_ALLOW_SKIP_LIVE_QWEN=1 to downgrade it to a SKIP_EXPLICIT skip that acceptance does not count.")
	}
	var missing []string
	if strings.TrimSpace(os.Getenv(secretstore.EnvVaultAddr)) == "" &&
		strings.TrimSpace(os.Getenv(secretstore.EnvVaultAddrAlt)) == "" {
		missing = append(missing, secretstore.EnvVaultAddr+"|"+secretstore.EnvVaultAddrAlt)
	}
	if strings.TrimSpace(os.Getenv(secretstore.EnvVaultToken)) == "" &&
		strings.TrimSpace(os.Getenv(secretstore.EnvVaultTokenAlt)) == "" {
		missing = append(missing, secretstore.EnvVaultToken+"|"+secretstore.EnvVaultTokenAlt)
	}
	if len(missing) > 0 {
		t.Fatalf("AHP_LIVE_QWEN=1 but the real Qwen credential source is unconfigured: missing %s -- "+
			"the native credential is read only from the scoped Vault runtime secret (env/shared-vault.env); "+
			"refusing to fake a live model pass.", strings.Join(missing, ", "))
	}
	requireRealDB(t)
	requireArtifactPlane(t)
	// The credential must actually be READABLE, not merely configured. Measured
	// on this node: a single Vault read took 7.4 s at load 15 and blew the secret
	// client's own timeout, which failed a live leg that had nothing to do with
	// the product. A bounded window removes "we asked once at the worst moment";
	// exhaustion is still a FAIL that names the dependency, never a skip.
	preflightVault(t)
}

// preflightVault reads the real Qwen credential with a bounded retry window and
// fails with a named reason (VAULT_UNAVAILABLE) when it cannot. It cannot make a
// dead Vault green -- it can only stop a healthy-but-slow Vault from being
// measured as a product failure.
func preflightVault(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	deadline := time.Now().Add(75 * time.Second)
	var last error
	for attempt := 1; ; attempt++ {
		started := time.Now()
		sec, err := secretstore.QwenFromEnv(ctx)
		if err == nil {
			t.Logf("VAULT_PREFLIGHT=OK attempt=%d elapsed=%s model=%s",
				attempt, time.Since(started).Round(time.Millisecond), sec.Model)
			return
		}
		last = err
		t.Logf("VAULT_PREFLIGHT_RETRY attempt=%d elapsed=%s err=%v",
			attempt, time.Since(started).Round(time.Millisecond), err)
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("VAULT_UNAVAILABLE: the live Qwen credential was not readable within 75 s (%v). "+
		"This is a real dependency failure and is reported as such: check Vault (env/shared-vault.env) "+
		"and rerun. Not a skip, and not evidence about the code under test.", last)
}
