package workspaceapi

import (
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// This file drives the Workspace owner surface over real HTTPS/mTLS with real
// task credentials against live PostgreSQL + MinIO: the trusted-CI gate is
// verified where the platform actually meets callers, not only at the store
// boundary. The fixture mirrors internal/artifactsapi/fixtures_test.go.

const (
	testIssuer = "https://identity.test"
	testKID    = "k1"
	userAud    = "ahp-user"
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

type apiEnv struct {
	srv        *httptest.Server
	userSrv    *httptest.Server // the sanctioned user edge face (task or user tokens)
	tenant     string
	deployment string
	signer     *auth.Signer
	pool       *pgxpool.Pool
	ws         *workspace.Store
	arts       *artifacts.Store
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for workspaceapi integration tests")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set for workspaceapi integration tests")
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Fatal("MinIO credentials must be configured for workspaceapi integration tests")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
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
	bucket := "ahp-go-wsapi-" + suffix
	tenant := "t-" + suffix
	deployment := "d-1"

	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	var wsRef *workspace.Store
	arts, err := artifacts.New(pool, client, bucket, allow,
		artifacts.WithWorkspaceAccess(func(ctx context.Context, s contracts.Scope, w string) error {
			return wsRef.IsMember(ctx, s, w)
		}))
	if err != nil {
		t.Fatalf("artifacts New: %v", err)
	}
	if err := arts.Migrate(ctx); err != nil {
		t.Fatalf("artifacts Migrate: %v", err)
	}
	ws, err := workspace.New(pool, arts, allow)
	if err != nil {
		t.Fatalf("workspace New: %v", err)
	}
	wsRef = ws
	if err := ws.Migrate(ctx); err != nil {
		t.Fatalf("workspace Migrate: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 30*time.Second)
		defer cn()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_workspace CASCADE`)
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
	// Route-level capability: "a credential bound to this plane". The fine
	// rights under test live in the store.
	route := func(context.Context, auth.Claims, string) error { return nil }
	face, err := New(ws, verifier, route)
	if err != nil {
		t.Fatal(err)
	}
	userFace, err := NewAcceptingUsers(ws, verifier, route, userAud)
	if err != nil {
		t.Fatal(err)
	}

	ca := newCA(t)
	clientCert := ca.clientCert(t, "spiffe://ahp/tenant/"+tenant+"/deployment/"+deployment)
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	start := func(h http.Handler) *httptest.Server {
		srv := httptest.NewUnstartedServer(h)
		srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
		srv.StartTLS()
		tr := srv.Client().Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{clientCert}
		srv.Client().Transport = tr
		t.Cleanup(srv.Close)
		return srv
	}
	return &apiEnv{srv: start(face), userSrv: start(userFace), tenant: tenant,
		deployment: deployment, signer: signer, pool: pool, ws: ws, arts: arts}
}

// sealCode seals one artifact through the artifact store and publishes it to
// the workspace so the rights gate is satisfied. The Workspace journey under
// test starts at the candidate.
func (e *apiEnv) sealCode(t *testing.T, subject, wsName, key, body string, mode int) workspace.File {
	t.Helper()
	ctx := context.Background()
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: subject}
	up, err := e.arts.BeginUpload(ctx, sc, key, artifacts.UploadRequest{
		Name: key, MIME: "application/octet-stream", SHA256: shaHex(body), Size: int64(len(body)), Labels: labels.Set{},
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPut, up.PresignedURL, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}
	ref, err := e.arts.Seal(ctx, sc, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if err := e.arts.PublishToWorkspace(ctx, sc, ref.ID, wsName); err != nil {
		t.Fatalf("PublishToWorkspace: %v", err)
	}
	return workspace.File{Path: key, ArtifactID: ref.ID, Mode: mode, Digest: ref.SHA256}
}

func (e *apiEnv) signTaskAs(t *testing.T, tenant, subject, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindTask,
		Tenant: tenant, Deployment: e.deployment,
		Subject: subject, Task: task, Root: "root-" + subject,
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *apiEnv) signHuman(t *testing.T, subject string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: userAud, Kind: auth.KindHuman,
		Tenant: e.tenant, Subject: subject,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func call(t *testing.T, srv *httptest.Server, op, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/workspace/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()
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

// callRaw keeps the response bytes for shapes that are JSON arrays at the
// top level (the runner registry), which the map-shaped helper cannot carry.
func callRaw(t *testing.T, srv *httptest.Server, op, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/workspace/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func shaHex(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// TestTrustedCIOverOwnerHTTP proves the audit's boundary (п.2) end to end on
// the live owner surface: verdicts posted by authors or unregistered task
// principals cannot merge code; only a registered CI runner task's verdict,
// bound to the candidate digest, merges. A human credential forwarded by a
// sanctioned user edge is refused before any store call.
func TestTrustedCIOverOwnerHTTP(t *testing.T) {
	e := newAPIEnv(t)
	const alice, bob, runner = "alice", "bob", "ci-runner-1"
	aliceTok := e.signTaskAs(t, e.tenant, alice, "task-alice")
	bobTok := e.signTaskAs(t, e.tenant, bob, "task-bob")
	runnerTok := e.signTaskAs(t, e.tenant, runner, "ci-task-1")

	code, _ := call(t, e.srv, "Create", aliceTok, `{"workspace":"codeproj"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, _ = call(t, e.srv, "AddMember", aliceTok, `{"workspace":"codeproj","subject":"bob"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, _ = call(t, e.srv, "AddMember", aliceTok, `{"workspace":"codeproj","subject":"ci-runner-1"}`)
	mustStatus(t, code, http.StatusOK, nil)

	// Registry policy over HTTP: owner-only, self-registration refused.
	code, _ = call(t, e.srv, "RegisterCIRunner", aliceTok, `{"workspace":"codeproj","subject":"alice"}`)
	mustStatus(t, code, http.StatusForbidden, nil)
	code, _ = call(t, e.srv, "RegisterCIRunner", bobTok, `{"workspace":"codeproj","subject":"ci-2"}`)
	mustStatus(t, code, http.StatusForbidden, nil)
	code, _ = call(t, e.srv, "RegisterCIRunner", aliceTok, `{"workspace":"codeproj","subject":"ci-runner-1"}`)
	mustStatus(t, code, http.StatusOK, nil)
	codeRaw, listRaw := callRaw(t, e.srv, "ListCIRunners", aliceTok, `{"workspace":"codeproj"}`)
	mustStatus(t, codeRaw, http.StatusOK, nil)
	if !strings.Contains(string(listRaw), "ci-runner-1") {
		t.Fatalf("runner registry not returned over HTTP: %s", listRaw)
	}

	// A code candidate prepared by bob (an editor prepares; the owner commits).
	mainFile := e.sealCode(t, alice, "codeproj", "main.go", "package main\n", 0o644)
	code, cc := call(t, e.srv, "CreateCandidate", bobTok, `{"workspace":"codeproj"}`)
	mustStatus(t, code, http.StatusOK, cc)
	candID, _ := cc["id"].(string)
	if candID == "" {
		t.Fatalf("no candidate id: %v", cc)
	}
	files, _ := json.Marshal([]workspace.File{mainFile})
	code, _ = call(t, e.srv, "UpdateCandidateFiles", bobTok,
		`{"candidate_id":"`+candID+`","files":`+string(files)+`}`)
	mustStatus(t, code, http.StatusOK, nil)

	// Bob's own task posts green: stored, worthless for merging code.
	code, _ = call(t, e.srv, "SetCheck", bobTok,
		`{"candidate_id":"`+candID+`","name":"unit-tests","status":"passed"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, pub := call(t, e.srv, "Publish", aliceTok,
		`{"candidate_id":"`+candID+`","expected_head":""}`)
	mustStatus(t, code, http.StatusConflict, pub)
	if !strings.Contains(strings.ToLower(mustJSONStr(pub)), "trusted ci") {
		t.Fatalf("conflict did not name the trusted-CI gate: %v", pub)
	}

	// The registered runner says failed: honoured.
	code, _ = call(t, e.srv, "SetCheck", e.signTaskAs(t, e.tenant, runner, "ci-task-fail"),
		`{"candidate_id":"`+candID+`","name":"unit-tests","status":"failed","detail":"vet"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, _ = call(t, e.srv, "Publish", aliceTok, `{"candidate_id":"`+candID+`","expected_head":""}`)
	mustStatus(t, code, http.StatusConflict, nil)

	// A foreign tenant's task cannot inject a verdict or read the candidate.
	foreignTok := e.signTaskAs(t, e.tenant+"-foreign", "mallory", "task-m")
	code, _ = call(t, e.srv, "SetCheck", foreignTok,
		`{"candidate_id":"`+candID+`","name":"unit-tests","status":"passed"}`)
	if code == http.StatusOK {
		t.Fatal("foreign tenant posted a check on someone else's candidate")
	}

	// The registered runner says passed with a task: the merge happens, and
	// the review shows the recorded provenance.
	code, _ = call(t, e.srv, "SetCheck", runnerTok,
		`{"candidate_id":"`+candID+`","name":"unit-tests","status":"passed"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, pub = call(t, e.srv, "Publish", aliceTok, `{"candidate_id":"`+candID+`","expected_head":""}`)
	mustStatus(t, code, http.StatusOK, pub)
	revID, _ := pub["id"].(string)
	if revID == "" {
		t.Fatalf("publish returned no revision: %v", pub)
	}
	code, det := call(t, e.srv, "CandidateDetails", aliceTok, `{"candidate_id":"`+candID+`"}`)
	mustStatus(t, code, http.StatusOK, det)
	db, _ := json.Marshal(det)
	for _, want := range []string{`"trusted":true`, `"runner_task":"ci-task-1"`, `"runner_subject":"ci-runner-1"`} {
		if !strings.Contains(string(db), want) {
			t.Fatalf("provenance missing over HTTP (%s): %s", want, db)
		}
	}

	// Stale-green replay: new bytes clear the green check; the previous
	// trusted verdict cannot be reused against the new digest.
	nextFile := e.sealCode(t, alice, "codeproj", "main2.go", "package main\nvar x = 1\n", 0o644)
	code, cc2 := call(t, e.srv, "CreateCandidate", bobTok, `{"workspace":"codeproj","base_rev":"`+revID+`"}`)
	mustStatus(t, code, http.StatusOK, cc2)
	cand2, _ := cc2["id"].(string)
	files2, _ := json.Marshal([]workspace.File{mainFile, nextFile})
	code, _ = call(t, e.srv, "UpdateCandidateFiles", bobTok,
		`{"candidate_id":"`+cand2+`","files":`+string(files2)+`}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, _ = call(t, e.srv, "Publish", aliceTok, `{"candidate_id":"`+cand2+`","expected_head":"`+revID+`"}`)
	mustStatus(t, code, http.StatusConflict, nil)
	code, _ = call(t, e.srv, "SetCheck", e.signTaskAs(t, e.tenant, runner, "ci-task-2"),
		`{"candidate_id":"`+cand2+`","name":"unit-tests","status":"passed"}`)
	mustStatus(t, code, http.StatusOK, nil)
	code, pub2 := call(t, e.srv, "Publish", aliceTok, `{"candidate_id":"`+cand2+`","expected_head":"`+revID+`"}`)
	mustStatus(t, code, http.StatusOK, pub2)

	// The sanctioned user edge forwards a human credential to this plane —
	// the verdict writer still refuses it before touching the store.
	humanTok := e.signHuman(t, alice)
	code, _ = call(t, e.userSrv, "SetCheck", humanTok,
		`{"candidate_id":"`+cand2+`","name":"unit-tests","status":"passed"}`)
	mustStatus(t, code, http.StatusForbidden, nil)
	// The same human edge may still read the project (the refusal is the
	// verdict path, not the face).
	code, _ = call(t, e.userSrv, "GetHead", humanTok, `{"workspace":"codeproj"}`)
	mustStatus(t, code, http.StatusOK, nil)
}

func mustJSONStr(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
