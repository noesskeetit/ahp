//go:build ahp_data

package main

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
	"fmt"
	"io"
	"log/slog"
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestDeleteThreadCascadesToArtifacts is the orphan the fleet measured:
// DeleteThread answered 200 and the thread went 404, while artifacts/Get and
// artifacts/Download kept serving the deleted thread's bytes. It runs the real
// State face, the real artifact store and the real composition-root wiring over
// mTLS; only the model turn is absent, because the obligation is about deletion
// and holds no matter which deploy produced the bytes.
func TestDeleteThreadCascadesToArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool, store, scope := cascadeEnv(t, ctx)

	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatalf("migrateStateAll: %v", err)
	}

	// The subject has a thread whose artifacts arrived two different ways: one
	// bound to the context (the artifact plane saw the token's context), one
	// bound only to the task the run carried. A second, unrelated thread of the
	// same subject must not lose anything.
	gone, err := st.CreateThread(ctx, scope, "ctx-cascade-thread", "k-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := st.CreateRun(ctx, scope, gone.ID, "task-cascade-1", "k-run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	kept, err := st.CreateThread(ctx, scope, "ctx-kept-thread", "k-thread-2")
	if err != nil {
		t.Fatalf("CreateThread 2: %v", err)
	}

	// Four writers, four names for the same thread: the context id State issued,
	// the thread id the run loop sees, the A2A task id and the State run id. The
	// cascade must close all of them, because the platform does not get to choose
	// which one a deploy happened to hold.
	byContext := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: gone.ContextID}, "deleted-thread-bytes")
	byThreadID := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: gone.ID}, "deleted-thread-id-bytes")
	byTask := sealArtifact(t, ctx, store, scope, artifacts.Binding{TaskID: run.TaskID}, "deleted-thread-task-bytes")
	byRunID := sealArtifact(t, ctx, store, scope, artifacts.Binding{TaskID: run.ID}, "deleted-thread-run-bytes")
	other := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: kept.ContextID, TaskID: "task-other"}, "kept-thread-bytes")

	srv := cascadeServer(t, st, store, scope)
	token := signTaskToken(t, srv, scope, "task-cascade-1", gone.ContextID)

	code, body := srv.call(t, "DeleteThread", token, `{"thread_id":"`+gone.ID+`"}`)
	if code != http.StatusOK {
		t.Fatalf("DeleteThread = %d %v, want 200", code, body)
	}
	if _, err := store.Get(ctx, scope, byContext); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("context-bound artifact survived DeleteThread: %v", err)
	}
	if _, err := store.Get(ctx, scope, byThreadID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("thread-id-bound artifact survived DeleteThread: %v", err)
	}
	if _, err := store.Get(ctx, scope, byTask); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("task-bound artifact survived DeleteThread: %v", err)
	}
	if _, err := store.Get(ctx, scope, byRunID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("run-id-bound artifact survived DeleteThread: %v", err)
	}
	if _, err := store.Get(ctx, scope, other); err != nil {
		t.Fatalf("another thread's artifact was collateral damage: %v", err)
	}

	// A repeated deletion is a no-op for State and for the artifacts alike.
	if code, body := srv.call(t, "DeleteThread", token, `{"thread_id":"`+gone.ID+`"}`); code != http.StatusOK {
		t.Fatalf("repeated DeleteThread = %d, want successful durable replay (%v)", code, body)
	}
	if _, err := store.Get(ctx, scope, byContext); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("artifact became readable again: %v", err)
	}
}

// TestLegalholdBlocksTheWholeCascade: the thread hold and the artifact hold are
// one obligation. A held thread keeps its artifacts readable; releasing the hold
// lets one deletion take both.
func TestLegalholdBlocksTheWholeCascade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool, store, scope := cascadeEnv(t, ctx)

	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatalf("migrateStateAll: %v", err)
	}
	thread, err := st.CreateThread(ctx, scope, "ctx-held-thread", "k-held-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	held := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: thread.ContextID}, "held-thread-bytes")
	pinned := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: thread.ContextID}, "legal-hold-bytes")
	if err := store.SetLegalHold(ctx, scope, pinned, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	if err := st.SetLegalhold(ctx, scope, thread.ID, "acceptance"); err != nil {
		t.Fatalf("SetLegalhold: %v", err)
	}

	srv := cascadeServer(t, st, store, scope)
	token := signTaskToken(t, srv, scope, "task-held", thread.ContextID)

	if code, body := srv.call(t, "DeleteThread", token, `{"thread_id":"`+thread.ID+`"}`); code == http.StatusOK {
		t.Fatalf("held thread was deleted: %v", body)
	}
	if _, err := store.Get(ctx, scope, held); err != nil {
		t.Fatalf("refused deletion still removed artifacts: %v", err)
	}

	if err := st.RemoveLegalhold(ctx, scope, thread.ID); err != nil {
		t.Fatalf("RemoveLegalhold: %v", err)
	}
	code, body := srv.call(t, "DeleteThread", token, `{"thread_id":"`+thread.ID+`"}`)
	if code != http.StatusOK {
		t.Fatalf("DeleteThread after release = %d %v, want 200", code, body)
	}
	if _, err := store.Get(ctx, scope, held); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unpinned artifact survived the cascade: %v", err)
	}
	if _, err := store.Get(ctx, scope, pinned); err != nil {
		t.Fatalf("the artifact hold was ignored: %v", err)
	}
}

// TestDeleteSubjectCascadesToArtifacts covers the second verb of the same PDF
// line: "удаление чата и сотрудника".
func TestDeleteSubjectCascadesToArtifacts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool, store, scope := cascadeEnv(t, ctx)

	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatalf("migrateStateAll: %v", err)
	}
	first, err := st.CreateThread(ctx, scope, "ctx-subject-1", "k-subject-1")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	second, err := st.CreateThread(ctx, scope, "ctx-subject-2", "k-subject-2")
	if err != nil {
		t.Fatalf("CreateThread 2: %v", err)
	}
	a := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: first.ContextID}, "subject-bytes-1")
	b := sealArtifact(t, ctx, store, scope, artifacts.Binding{ContextID: second.ContextID}, "subject-bytes-2")

	srv := cascadeServer(t, st, store, scope)
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "DeleteSubject", token, scope, `{"key":"delete-subject-test"}`)
	if code != http.StatusOK {
		t.Fatalf("DeleteSubject = %d %v, want 200", code, body)
	}
	for _, id := range []string{a, b} {
		if _, err := store.Get(ctx, scope, id); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("artifact %s survived DeleteSubject: %v", id, err)
		}
	}
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type cascadeService struct {
	revoked *atomic.Bool
	srv     *httptest.Server
	signer  *auth.Signer
}

func (c *cascadeService) call(t *testing.T, op, token, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, c.srv.URL+"/v1/state/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.srv.Client().Do(req)
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

// cascadeEnv opens the real stores the composition root wires together. It
// skips when the integration environment is absent, and uses a throwaway tenant
// and bucket so it can never touch a neighbour's rows.
func cascadeEnv(t *testing.T, ctx context.Context, authorizers ...artifacts.Authorize) (*pgxpool.Pool, *artifacts.Store, contracts.Scope) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_DATABASE_URL")
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if dsn == "" || raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" ||
		os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("cascade wiring test needs AHP_TEST_DATABASE_URL and MinIO credentials")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
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
		t.Fatalf("minio: %v", err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	bucket := "ahp-cascade-" + hex.EncodeToString(rnd[:])
	authorize := artifacts.Authorize(func(context.Context, contracts.Scope, string, string) error { return nil })
	if len(authorizers) > 0 {
		authorize = authorizers[0]
	}
	store, err := artifacts.New(pool, client, bucket, authorize)
	if err != nil {
		t.Fatalf("artifacts.New: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("artifacts Migrate: %v", err)
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
	scope := contracts.Scope{
		Tenant:     "t-cascade-" + hex.EncodeToString(rnd[:]),
		Deployment: "d-cascade",
		Subject:    "subject-cascade",
	}
	return pool, store, scope
}

// cascadeServer mounts the State face exactly as serveAll does - wrapped in the
// deletion cascade - behind the mTLS the task routes demand.
func cascadeServer(t *testing.T, st *state.Store, store *artifacts.Store, scope contracts.Scope) *cascadeService {
	return cascadeServerConfigured(t, st, store, scope, nil)
}
func cascadeServerConfigured(t *testing.T, st *state.Store, store *artifacts.Store, scope contracts.Scope, configure func(*server), control ...ControlDeletionConfig) *cascadeService {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("https://identity.test", "k1", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("https://identity.test", map[string]ed25519.PublicKey{"k1": pub})
	issue := func(_ context.Context, s contracts.Scope, taskID string) (string, error) {
		return signer.Sign(auth.Claims{
			Audience: "ahp", Kind: auth.KindTask,
			Tenant: s.Tenant, Deployment: s.Deployment, Subject: s.Subject,
			Task: taskID, Root: taskID, Context: "issued", Grant: "g", Depth: 0,
		}, 5*time.Minute)
	}
	revoked := &atomic.Bool{}
	stateHandler, err := stateapi.NewWithAdmin(st, verifier,
		func(context.Context, auth.Claims, string) error {
			if revoked.Load() {
				return contracts.NewError(contracts.ErrForbidden, "grant revoked")
			}
			return nil
		}, issue, stateapi.AdminConfig{Audience: "ahp", Authorize: func(_ context.Context, claims auth.Claims, cap string) error {
			if claims.Subject != "compliance-admin" || (cap != stateapi.AdminCapability && cap != stateapi.TenantDeleteCapability) {
				return contracts.NewError(contracts.ErrForbidden, "not admin")
			}
			return nil
		}})
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}
	s := &server{
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		cascade: artifactStoreCascade{store: store},
	}
	mem, err := memory.New(st.Pool(), store, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = mem.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.New(st.Pool(), store, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = identity.New(st.Pool()).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = registry.New(st.Pool()).Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.cascade = withMemoryDeletion(s.cascade, mem, ws)
	if configure != nil {
		configure(s)
	}
	ca := newTestCA(t)
	client := ca.clientCert(t, auth.WorkloadID(scope))
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.pem) {
		t.Fatal("ca pem")
	}
	srv := httptest.NewUnstartedServer(s.withDeletionCascade(stateHandler, st, verifier, control...))
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{client}
	srv.Client().Transport = tr
	t.Cleanup(srv.Close)
	return &cascadeService{srv: srv, signer: signer, revoked: revoked}
}

func signTaskToken(t *testing.T, srv *cascadeService, scope contracts.Scope, taskID, contextID string) string {
	t.Helper()
	tok, err := srv.signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask,
		Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject,
		Task: taskID, Root: taskID, Context: contextID, Grant: "g", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// sealArtifact writes one sealed artifact through the artifact plane itself.
func sealArtifact(t *testing.T, ctx context.Context, store *artifacts.Store, scope contracts.Scope,
	b artifacts.Binding, body string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := store.BeginUploadWithBinding(ctx, scope, b, "k-"+contracts.NewID(), artifacts.UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUploadWithBinding: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, up.PresignedURL, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT: %T", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("PUT status %d", resp.StatusCode)
	}
	ref, err := store.Seal(ctx, scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref.ID
}

type testCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-cascade-test-ca"},
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
	for _, raw := range uris {
		p, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-cascade-test-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, URIs: parsed,
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

func (c *cascadeService) adminCall(t *testing.T, op, token string, scope contracts.Scope, input string) (int, map[string]any) {
	t.Helper()
	body := fmt.Sprintf(`{"target":{"deployment":%q,"subject":%q},"input":%s}`, scope.Deployment, scope.Subject, input)
	req, err := http.NewRequest(http.MethodPost, c.srv.URL+"/v1/state-admin/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}
