package artifactsapi

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

const (
	testIssuer = "https://identity.test"
	testKID    = "k1"
	alice      = "alice"
	mallory    = "mallory"
)

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
			t.Fatal(err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-test-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, URIs: parsed,
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

type env struct {
	srv        *httptest.Server
	tenant     string
	deployment string
	signer     *auth.Signer
}

// newEnv spins up the Artifact API against real PostgreSQL + MinIO. It fails
// (not skips) when the environment is missing so it cannot pass vacuously.
func newEnv(t *testing.T, opts ...artifacts.Option) *env {
	return newEnvWithAuthorize(t, nil, opts...)
}

// newEnvWithAuthorize is newEnv with the store's per-action authorization
// decision replaced. That is where the deletion plane's actions ("gc",
// "restore", "pin", "purge", "retain", "hold") are decided, so this is how a
// test shows an admin-only operation is refused for a credential the policy has
// not carried those actions on.
func newEnvWithAuthorize(t *testing.T, az artifacts.Authorize, opts ...artifacts.Option) *env {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for artifactsapi integration tests")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set for artifactsapi integration tests")
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Fatal("MinIO credentials must be configured for artifactsapi integration tests")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	// The pool is the run's isolated database, shared by every fixture here.
	pool := isolatedPool(t)
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(rnd[:])
	bucket := "go-h17-api-" + suffix
	registerBucket(bucket)

	storeAuthorize := func(context.Context, contracts.Scope, string, string) error { return nil }
	if az != nil {
		storeAuthorize = az
	}
	store, err := artifacts.New(pool, client, bucket, storeAuthorize, opts...)
	if err != nil {
		t.Fatalf("store New: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(testIssuer, testKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	// The route-level decision stays permissive here: the capability is "is this
	// a bound task token for the artifacts plane", and the fine-grained action
	// decisions under test live in the store.
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	handler, err := New(store, verifier, authorize)
	if err != nil {
		t.Fatalf("api New: %v", err)
	}

	ca := newCA(t)
	tenant := "t-" + suffix
	deployment := "d-1"
	client0 := ca.clientCert(t, workloadURI(tenant, deployment))
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{client0}
	srv.Client().Transport = tr
	t.Cleanup(srv.Close)
	return &env{srv: srv, tenant: tenant, deployment: deployment, signer: signer}
}

// signTask mints a task token bound to the env workload for a given subject.
func (e *env) signTask(t *testing.T, subject string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: subject, Task: "task-1", Root: "root-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *env) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/artifacts/"+op, strings.NewReader(body))
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

// put uploads bytes to a presigned URL returned by BeginUpload.
func put(t *testing.T, signed, body string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, signed, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT failed: %T", err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}
}

func shaHex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// uploadSeal runs BeginUpload -> PUT -> Seal over HTTP and returns the id.
func (e *env) uploadSeal(t *testing.T, token, key, body string, set labels.Set) string {
	t.Helper()
	req := map[string]any{"key": key, "name": "a.bin", "mime": "application/octet-stream",
		"sha256": shaHex(body), "size": len(body), "labels": set}
	code, up := e.call(t, "BeginUpload", token, mustJSON(t, req))
	mustStatus(t, code, http.StatusOK, up)
	signed, _ := up["presigned_url"].(string)
	if signed == "" {
		t.Fatalf("no presigned url: %v", up)
	}
	put(t, signed, body)
	id, _ := up["id"].(string)
	code, ref := e.call(t, "Seal", token, mustJSON(t, map[string]any{"id": id}))
	mustStatus(t, code, http.StatusOK, ref)
	return id
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// downloadRaw fetches bytes with an optional Range header.
func (e *env) downloadRaw(t *testing.T, token, body, rangeHdr string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/artifacts/Download", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}
