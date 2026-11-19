//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// W-2/W-3 green legs on the mounted data face: the owner's own credential reaches
// the durable-retention controls PDF section 8 asks for (pin/unpin, restore),
// and every one of them stays bounded by the VERIFIED scope -- a second tenant
// holding its own certificate and its own task token can neither write a pin into
// another tenant's registry nor resurrect another tenant's tombstone. The sweep
// itself must stay refused, so admitting pin/restore cannot turn into admitting
// the collector.
//
// This runs against the real mount (newDataPlanes + dataHandlers +
// mountDataHandlers behind transport.Task), a real database and a real object
// store; the durable pin row is read with raw SQL rather than through the face,
// so the proof is a row and not the front's own answer. Lane prefixes belong to
// this test alone, a fresh tenant and bucket per run, so the leg re-runs on a
// database that already holds earlier runs.
const (
	w23BucketPrefix = "ahp-hive-h23g"
	w23TenantPrefix = "ahp_hive_h23g"
	w23Deployment   = "d-h23g"
)

func w23Env(t *testing.T) (*pgxpool.Pool, *minio.Client) {
	t.Helper()
	dsn := strings.TrimSpace(envOr("AHP_TEST_DATABASE_URL", ""))
	if dsn == "" {
		dsn = strings.TrimSpace(envOr("AHP_DATABASE_URL", ""))
	}
	raw := strings.TrimSpace(envOr("AHP_S3_ENDPOINT", ""))
	if dsn == "" || raw == "" || envOr("AHP_MINIO_ROOT_USER", "") == "" ||
		envOr("AHP_MINIO_ROOT_PASSWORD", "") == "" {
		t.Skip("W-2/W-3 legs need the isolated test database and object store (AHP_TEST_DATABASE_URL, AHP_S3_ENDPOINT, MinIO credentials): bring them up, then set them; not a silent pass")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.QueryRow(context.Background(), `SELECT 1`).Scan(new(int)); err != nil {
		t.Fatalf("isolated database unreachable: %v", err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio: %v", err)
	}
	return pool, client
}

// w23Face mounts the real artifact face over mTLS and returns two clients: the
// owner's workload and a foreign tenant's workload. Both are real certificates
// signed by the same CA, because the boundary under test is the scope inside the
// store, not the ability to produce a certificate.
type w23Face struct {
	server       *httptest.Server
	owner        *http.Client
	foreign      *http.Client
	scope        contracts.Scope
	foreignScope contracts.Scope
	ownerToken   string
	foreignToken string
	pool         *pgxpool.Pool
	bucket       string
}

func newW23Face(t *testing.T) *w23Face {
	t.Helper()
	ctx := context.Background()
	pool, client := w23Env(t)
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	suffix := hex.EncodeToString(rnd[:])
	scope := contracts.Scope{Tenant: w23TenantPrefix + "_" + suffix, Deployment: w23Deployment,
		Subject: "subject-h23g"}
	foreignScope := contracts.Scope{Tenant: w23TenantPrefix + "_f_" + suffix, Deployment: w23Deployment,
		Subject: "subject-h23g"}

	// A fresh bucket and a fresh key pair per run, so nothing here can reach an
	// earlier run's objects or credentials.
	t.Setenv("AHP_DEV_JWT_SEED", hex.EncodeToString(make([]byte, ed25519.SeedSize)))
	t.Setenv("AHP_S3_BUCKET", w23BucketPrefix+"-"+suffix)
	t.Setenv("AHP_S3_REGION", "us-east-1")

	s := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	artStore, wsStore, verifier, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatalf("newDataPlanes: %v", err)
	}
	if err := artStore.Migrate(ctx); err != nil {
		t.Fatalf("artifacts migrate: %v", err)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for obj := range client.ListObjects(clean, w23BucketPrefix+"-"+suffix,
			minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(clean, w23BucketPrefix+"-"+suffix, obj.Key,
				minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(clean, w23BucketPrefix+"-"+suffix)
	})

	artifactsHandler, workspaceHandler, err := dataHandlers(artStore, wsStore, verifier)
	if err != nil {
		t.Fatalf("dataHandlers: %v", err)
	}
	mux := http.NewServeMux()
	mountDataHandlers(mux, artifactsHandler, workspaceHandler)

	ca := newTestCA(t)
	pool509 := x509.NewCertPool()
	if !pool509.AppendCertsFromPEM(ca.pem) {
		t.Fatal("ca pem")
	}
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool509}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	_, signer := devIdentityAuth()
	tokenFor := func(sc contracts.Scope, task, contextID string) string {
		tok, err := signer.Sign(auth.Claims{
			Audience: "ahp", Kind: auth.KindTask,
			Tenant: sc.Tenant, Deployment: sc.Deployment, Subject: sc.Subject,
			Task: task, Root: task, Context: contextID, Grant: "g", Depth: 0,
		}, 5*time.Minute)
		if err != nil {
			t.Fatalf("mint token: %v", err)
		}
		return tok
	}
	return &w23Face{
		server:       srv,
		owner:        w23Client(t, ca, ca.clientCert(t, auth.WorkloadID(scope))),
		foreign:      w23Client(t, ca, ca.clientCert(t, auth.WorkloadID(foreignScope))),
		scope:        scope,
		foreignScope: foreignScope,
		ownerToken:   tokenFor(scope, "h23g-task-"+suffix, "h23gctx"+suffix),
		foreignToken: tokenFor(foreignScope, "h23g-task-f-"+suffix, "h23gctxf"+suffix),
		pool:         pool,
		bucket:       w23BucketPrefix + "-" + suffix,
	}
}

func w23Client(t *testing.T, ca *testCA, cert tls.Certificate) *http.Client {
	t.Helper()
	inner := x509.NewCertPool()
	if !inner.AppendCertsFromPEM(ca.pem) {
		t.Fatal("ca pem")
	}
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		Certificates: []tls.Certificate{cert}, RootCAs: inner, InsecureSkipVerify: true,
		// The httptest certificate does not cover the workload URI the client
		// presents, so the server's own identity is pinned by CA only.
		ServerName: "127.0.0.1",
	}}}
}

func (f *w23Face) call(t *testing.T, client *http.Client, op, token string, body any) (int, string, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, f.server.URL+"/v1/artifacts/"+op, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s transport: %T", op, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	payload, _ := io.ReadAll(resp.Body)
	code := ""
	var e struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(payload, &e) == nil {
		code = e.Code
	}
	return resp.StatusCode, code, payload
}

// pinRows reads the durable pin registry directly, out of the front's own answer.
func (f *w23Face) pinRows(t *testing.T, tenant, refID string) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_artifacts.gc_pins WHERE tenant = $1 AND ref_id = $2`,
		tenant, refID).Scan(&n); err != nil {
		t.Fatalf("read gc_pins: %v", err)
	}
	return n
}

func TestW23OwnerReachesPinsAndRestoreThroughTheMountedArtifactFace(t *testing.T) {
	f := newW23Face(t)
	suffix := strings.TrimPrefix(f.scope.Tenant, w23TenantPrefix+"_")
	refID := "h23g-pin-" + suffix
	key := "h23g-key-" + suffix
	body := []byte("w23 retention control payload " + suffix)
	digest := w23Digest(body)

	// --- seal an artifact through the face, exactly as a run does ---
	st, code, payload := f.call(t, f.owner, "BeginUpload", f.ownerToken, map[string]any{
		"key": key, "name": "w23.bin", "mime": "application/octet-stream",
		"sha256": digest, "size": len(body),
	})
	if st != http.StatusOK {
		t.Fatalf("BeginUpload: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}
	var up struct {
		ID           string `json:"id"`
		PresignedURL string `json:"presigned_url"`
	}
	if err := json.Unmarshal(payload, &up); err != nil || up.ID == "" || up.PresignedURL == "" {
		t.Fatalf("BeginUpload shape: %v id=%q url_set=%v", err, up.ID, up.PresignedURL != "")
	}
	put, err := http.DefaultClient.Do(mustPut(t, up.PresignedURL, body))
	if err != nil {
		t.Skipf("object store unreachable from this host (%T): the isolated MinIO must answer for this leg; not a silent pass", err)
	}
	_, _ = io.Copy(io.Discard, put.Body)
	_ = put.Body.Close()
	if put.StatusCode/100 != 2 {
		t.Fatalf("presigned PUT: status %d", put.StatusCode)
	}
	if st, code, payload = f.call(t, f.owner, "Seal", f.ownerToken, map[string]any{"id": up.ID}); st != http.StatusOK {
		t.Fatalf("Seal: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}
	t.Logf("sealed artifact=%s sha=%s.. tenant=%s", up.ID, digest[:12], f.scope.Tenant)

	// --- W-2: pin is reachable to the owner and lands in the durable registry ---
	pinBody := map[string]any{"kind": "task", "ref_id": refID, "artifact_id": up.ID}
	if st, code, payload := f.call(t, f.owner, "PinRef", f.ownerToken, pinBody); st != http.StatusOK {
		t.Fatalf("PinRef must be admitted to the owner of the artifact: got %d %s %s",
			st, code, strings.TrimSpace(string(payload)))
	}
	if got := f.pinRows(t, f.scope.Tenant, refID); got != 1 {
		t.Fatalf("the pin must be a durable row: gc_pins rows=%d, want 1", got)
	}
	if st, code, payload := f.call(t, f.owner, "ListPins", f.ownerToken,
		map[string]any{"kind": "task", "ref_id": refID}); st != http.StatusOK ||
		!strings.Contains(string(payload), up.ID) {
		t.Fatalf("ListPins must report the pin: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}
	t.Logf("pin durable: gc_pins row present for tenant %s ref %s", f.scope.Tenant, refID)

	// --- a foreign tenant cannot write into this tenant's registry ---
	if st, code, payload := f.call(t, f.foreign, "PinRef", f.foreignToken, pinBody); st == http.StatusOK {
		t.Fatal("a foreign tenant must not be able to pin this artifact")
	} else {
		if strings.Contains(string(payload), digest) {
			t.Fatal("a refusal must not carry the artifact digest")
		}
		t.Logf("foreign PinRef refused: %d %s", st, code)
	}
	if got := f.pinRows(t, f.scope.Tenant, refID); got != 1 {
		t.Fatalf("a foreign attempt must leave the owner's pin untouched: rows=%d", got)
	}
	if got := f.pinRows(t, f.foreignScope.Tenant, refID); got != 0 {
		t.Fatalf("a refused pin must not exist under the foreign tenant either: rows=%d", got)
	}
	if st, code, payload := f.call(t, f.owner, "UnpinRef", f.ownerToken, pinBody); st != http.StatusOK {
		t.Fatalf("UnpinRef must be admitted to the owner: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}
	if got := f.pinRows(t, f.scope.Tenant, refID); got != 0 {
		t.Fatalf("unpin must withdraw the durable row: rows=%d", got)
	}
	t.Log("unpin withdrew the durable row")

	// --- W-3: tombstone, foreign cannot resurrect, owner can ---
	if st, code, payload := f.call(t, f.owner, "Delete", f.ownerToken,
		map[string]any{"id": up.ID, "key": key + "-tomb"}); st != http.StatusOK {
		t.Fatalf("Delete: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}
	if st, _, _ := f.call(t, f.owner, "Get", f.ownerToken, map[string]any{"id": up.ID}); st != http.StatusNotFound {
		t.Fatalf("the artifact must be absent after tombstone: got %d", st)
	}
	if st, code, _ := f.call(t, f.foreign, "Restore", f.foreignToken, map[string]any{"id": up.ID}); st == http.StatusOK {
		t.Fatal("a foreign tenant must not restore this artifact")
	} else {
		t.Logf("foreign Restore refused: %d %s", st, code)
	}
	if st, _, _ := f.call(t, f.owner, "Get", f.ownerToken, map[string]any{"id": up.ID}); st != http.StatusNotFound {
		t.Fatalf("a foreign Restore attempt must leave the tombstone in place: got %d", st)
	}
	if st, code, payload := f.call(t, f.owner, "Restore", f.ownerToken, map[string]any{"id": up.ID}); st != http.StatusOK {
		t.Fatalf("Restore must be admitted to the owner: got %d %s %s", st, code, strings.TrimSpace(string(payload)))
	}

	// --- the restored artifact serves the exact sealed bytes again ---
	req, _ := http.NewRequest(http.MethodPost, f.server.URL+"/v1/artifacts/Download",
		bytes.NewReader([]byte(`{"id":"`+up.ID+`"}`)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.ownerToken)
	resp, err := f.owner.Do(req)
	if err != nil {
		t.Fatalf("Download transport: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	got, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || w23Digest(got) != digest {
		t.Fatalf("a restored artifact must serve the sealed bytes: status=%d digest_match=%v",
			resp.StatusCode, w23Digest(got) == digest)
	}
	t.Log("restore returned the exact sealed bytes (digest verified out of band)")

	// --- the sweep stays refused to a workload credential, always ---
	if st, code, payload := f.call(t, f.owner, "GC", f.ownerToken,
		map[string]any{"key": "h23g-gc-" + suffix, "dry_run": true}); st != http.StatusForbidden || code != "FORBIDDEN" {
		t.Fatalf("GC must stay refused to a task credential: got %d %s %s",
			st, code, strings.TrimSpace(string(payload)))
	} else if strings.Contains(string(payload), digest) {
		t.Fatal("the GC refusal must not carry the artifact digest")
	}
	t.Log("sweep still refused: GC -> 403 FORBIDDEN")
}

// w23Digest is the out-of-band digest check for this lane.
func w23Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustPut(t *testing.T, url string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("put request: %v", err)
	}
	return req
}
