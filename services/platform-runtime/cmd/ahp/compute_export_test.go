//go:build ahp_data && ahp_exec

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	artifactsdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
	computesdk "github.com/ahp-platform/ahp/sdk/go/compute"
	"github.com/jackc/pgx/v5/pgxpool"
)

type exportFaultOwner struct {
	artifactExportOwner
	puts        atomic.Int64
	loseSealAck atomic.Bool
	beforeSeal  atomic.Pointer[func() error]
	last        atomic.Pointer[artifactsdk.Upload]
	after       atomic.Pointer[func() error]
}

func (o *exportFaultOwner) Upload(ctx context.Context, u artifactsdk.Upload, b []byte) error {
	if err := o.artifactExportOwner.Upload(ctx, u, b); err != nil {
		return err
	}
	o.puts.Add(1)
	o.last.Store(&u)
	if f := o.after.Load(); f != nil {
		return (*f)()
	}
	return nil
}

func (o *exportFaultOwner) Seal(ctx context.Context, id string) (artifactsdk.ArtifactRef, error) {
	if f := o.beforeSeal.Load(); f != nil {
		if e := (*f)(); e != nil {
			return artifactsdk.ArtifactRef{}, e
		}
	}
	ref, err := o.artifactExportOwner.Seal(ctx, id)
	if err != nil {
		return ref, err
	}
	if o.loseSealAck.Swap(false) {
		return artifactsdk.ArtifactRef{}, contracts.NewError(contracts.ErrUnavailable, "injected lost Seal ACK")
	}
	return ref, nil
}

// Real mounted Data/Compute owners, PostgreSQL, Redis, MinIO and mTLS. The
// filesystem is a temporary host computer, not a microVM-isolation claim.
func TestComputeExportRealOwners(t *testing.T) {
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" || os.Getenv("AHP_REDIS_ADDR") == "" || os.Getenv("AHP_S3_ENDPOINT") == "" {
		t.Fatal("real PG/Redis/MinIO required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	poolConfig, err := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	poolConfig.MaxConns = 4
	bootstrap, e := pgxpool.NewWithConfig(ctx, poolConfig)
	if e != nil {
		t.Fatal(e)
	}
	e = state.New(bootstrap).Migrate(ctx)
	bootstrap.Close()
	if e != nil {
		t.Fatal(e)
	}
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	jail := t.TempDir()
	t.Setenv("AHP_COMPUTE_JAIL_BASE", jail)
	t.Setenv("AHP_ARTIFACTS_URL", "")
	t.Setenv("AHP_S3_BUCKET", "ahp-export-"+strings.ToLower(strings.ReplaceAll(contracts.NewID(), "_", "-"))[:40])
	s := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ah, wh, err := s.buildDataHandler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.artifactExportOwner == nil {
		t.Fatal("Data mount did not install owner")
	}
	fault := &exportFaultOwner{artifactExportOwner: s.artifactExportOwner}
	s.artifactExportOwner = fault
	ch, err := s.buildComputeHandler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mountDataHandlers(mux, ah, wh)
	mountComputePlane(mux, ch)
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	scope := contracts.Scope{Tenant: "export-" + contracts.NewID(), Deployment: "agent", Subject: "alice"}
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	hc.Transport = tr
	_, signer := devIdentityAuth()
	task := func(name, subject, contextID string) string {
		t.Helper()
		sc := scope
		sc.Subject = subject
		_, err := ids.CreateRoot(ctx, sc, identity.RootRequest{Task: name, Context: scope.Tenant + "-" + contextID, Deployment: sc.Deployment, Capabilities: []string{"compute", "artifacts"}, TTL: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		tok, err := ids.MintTask(ctx, sc, name, signer, "ahp", 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	token := task("export-main", "alice", "export-context")
	call := func(tok, op string, in any, want int) map[string]any {
		t.Helper()
		b, _ := json.Marshal(in)
		r, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/compute/"+op, bytes.NewReader(b))
		r.Header.Set("Authorization", "Bearer "+tok)
		r.Header.Set("Content-Type", "application/json")
		resp, err := hc.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		if resp.StatusCode != want {
			t.Fatalf("%s: status=%d want=%d body=%s", op, resp.StatusCode, want, raw)
		}
		return out
	}
	comp := call(token, "CreateComputer", map[string]any{"class": "S", "profile": "personal", "key": "create"}, 200)
	id, _ := comp["id"].(string)
	if id == "" {
		t.Fatalf("no computer id: %v", comp)
	}
	home := compute.NewRunner(compute.New(pool), compute.NewRegistry(), jail, "", nil).HomeDir(id)
	if err = os.MkdirAll(home, 0700); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("AHP export exact bytes\n"), 4000)
	path := filepath.Join(home, "out.txt")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"computer_id": id, "path": "out.txt", "key": "export-one"}
	ref := call(token, "Export", input, 200)
	aid, _ := ref["artifact_id"].(string)
	sum := sha256.Sum256(data)
	if aid == "" || ref["digest"] != hex.EncodeToString(sum[:]) || ref["size_bytes"] != float64(len(data)) || ref["version"] == "" {
		t.Fatalf("wrong sealed identity: %v", ref)
	}
	if _, ok := ref["sealed_at"]; ok {
		t.Fatal("fabricated sealed_at")
	}
	ac := func(tok string) *artifactsdk.Client {
		t.Helper()
		c, e := artifactsdk.New(artifactsdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return tok, nil }})
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	got, err := ac(token).Download(ctx, aid)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("exact download: bytes=%d err=%v", len(got), err)
	}
	part, e := ac(token).ReadRange(ctx, aid, 17, 1024)
	if e != nil || !bytes.Equal(part, data[17:1041]) {
		t.Fatalf("range bytes mismatch: %v", e)
	}
	before := fault.puts.Load()
	replay := call(token, "Export", input, 200)
	if replay["artifact_id"] != aid || replay["version"] != ref["version"] || fault.puts.Load() != before {
		t.Fatal("same key replay uploaded again or changed artifact")
	}
	fault.loseSealAck.Store(true)
	lostInput := map[string]any{"computer_id": id, "path": "out.txt", "key": "lost-seal-ack"}
	call(token, "Export", lostInput, 503)
	lostID := fault.last.Load().ID
	lostPuts := fault.puts.Load()
	resolved := call(token, "Export", lostInput, 200)
	if resolved["artifact_id"] != lostID || fault.puts.Load() != lostPuts {
		t.Fatal("lost Seal ACK replay uploaded twice")
	}
	if err = os.WriteFile(path, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	call(token, "Export", input, 400)
	_ = os.WriteFile(path, data, 0600)
	_ = os.WriteFile(filepath.Join(home, "other.txt"), data, 0600)
	call(token, "Export", map[string]any{"computer_id": id, "path": "other.txt", "key": "export-one"}, 400)
	call(token, "Export", map[string]any{"computer_id": id, "path": "out.txt"}, 400)
	foreign := task("export-foreign", "bob", "foreign-context")
	call(foreign, "Export", input, 404)
	if _, err = ac(foreign).Download(ctx, aid); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign bytes exposed: %v", err)
	}
	bobComputer := call(foreign, "CreateComputer", map[string]any{"class": "S", "profile": "personal", "key": "bob-create"}, 200)["id"].(string)
	bobHome := compute.NewRunner(compute.New(pool), compute.NewRegistry(), jail, "", nil).HomeDir(bobComputer)
	if e := os.MkdirAll(bobHome, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(bobHome, "out.txt"), data, 0600); e != nil {
		t.Fatal(e)
	}
	call(foreign, "Export", map[string]any{"computer_id": bobComputer, "path": "out.txt", "key": "export-one"}, 403)
	otherCtx := task("export-context-two", "alice", "other-context")
	if _, err = ac(otherCtx).Get(ctx, aid); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("cross-context artifact: %v", err)
	}

	// The separate HTTP owner path uses the public SDK and the same workload
	// identity as its task; an unrelated deployment certificate is denied.
	verifier, _ := devIdentityAuth()
	claims, err := verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	remoteReq := func() compute.ArtifactExportRequest {
		return compute.ArtifactExportRequest{Scope: scope, ComputerID: id, TaskID: claims.Task, ContextID: claims.Context, Key: "remote-owner", SourcePath: "out.txt", Filename: "out.txt", ContentType: "application/octet-stream", Body: bytes.NewReader(data), SizeHint: int64(len(data)), Check: func(c context.Context) error { return ids.Authorize(c, claims, "artifacts") }}
	}
	remote, err := compute.NewArtifactExporter(srv.URL, hc, func(context.Context) (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	remoteRef, err := remote.Export(ctx, remoteReq())
	if err != nil {
		t.Fatal("remote owner", err)
	}
	if b, e := ac(token).Download(ctx, remoteRef.ArtifactID); e != nil || !bytes.Equal(b, data) {
		t.Fatal("remote bytes", e)
	}
	wrongScope := scope
	wrongScope.Deployment = "another-workload"
	wrongHC := *hc
	wrongTR := tr.Clone()
	wrongTR.TLSClientConfig = tr.TLSClientConfig.Clone()
	wrongTR.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(wrongScope))}
	wrongHC.Transport = wrongTR
	wrong, err := compute.NewArtifactExporter(srv.URL, &wrongHC, func(context.Context) (string, error) { return token, nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, e := wrong.Export(ctx, remoteReq()); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatalf("different workload accepted: %v", e)
	}

	// A supplied key is deployment-wide; target/task belong to its immutable
	// fingerprint, not a namespace in which the same key can publish again.
	second := call(token, "CreateComputer", map[string]any{"class": "S", "profile": "personal", "key": "create-second", "realm": "second"}, 200)["id"].(string)
	secondHome := compute.NewRunner(compute.New(pool), compute.NewRegistry(), jail, "", nil).HomeDir(second)
	if err = os.MkdirAll(secondHome, 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(secondHome, "out.txt"), data, 0600); err != nil {
		t.Fatal(err)
	}
	secondTask := task("same-key-new-task", "alice", "export-context")
	for _, target := range []struct{ name, token, computer string }{{"different-computer", token, second}, {"different-task", secondTask, id}} {
		in := map[string]any{"computer_id": target.computer, "path": "out.txt", "key": "export-one"}
		b, _ := json.Marshal(in)
		rq, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/compute/Export", bytes.NewReader(b))
		rq.Header.Set("Authorization", "Bearer "+target.token)
		rq.Header.Set("Content-Type", "application/json")
		rs, e := hc.Do(rq)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := io.ReadAll(rs.Body)
		rs.Body.Close()
		if rs.StatusCode != 400 || !bytes.Contains(raw, []byte("KEY_REUSE_MISMATCH")) {
			t.Errorf("same deployment key reused on %s: status=%d body=%s", target.name, rs.StatusCode, raw)
		}
	}
	t.Run("PublicGoPythonExportInteroperability", func(t *testing.T) {
		cc, e := computesdk.New(computesdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if e != nil {
			t.Fatal(e)
		}
		goRef, e := cc.Export(ctx, id, "out.txt", "sdk-go-export")
		if e != nil {
			t.Fatal(e)
		}
		if goRef.Digest != hex.EncodeToString(sum[:]) || goRef.SizeBytes != int64(len(data)) {
			t.Fatal("Go SDK returned wrong export identity")
		}
		revoked := task("sdk-revoked", "alice", "sdk-revoked-context")
		if e = ids.CancelSubtree(ctx, scope, "sdk-revoked"); e != nil {
			t.Fatal(e)
		}
		before := fault.puts.Load()
		pyRef := runComputeExportPython(t, ctx, srv, tr.TLSClientConfig.Certificates[0], map[string]any{"base": srv.URL, "token": token, "foreign": foreign, "revoked": revoked, "computer": id, "go_ref": goRef, "digest": hex.EncodeToString(sum[:]), "size": len(data)})
		if fault.puts.Load() != before+1 {
			t.Fatal("Go/Python same-key replay performed extra PUT")
		}
		if b, e := ac(token).Download(ctx, pyRef.ArtifactID); e != nil || !bytes.Equal(b, data) {
			t.Fatal("Go read of Python export differs", e)
		}
	})
	// Even a cryptographically valid old JWT loses publication immediately after
	// the physical upload; the staged object is never sealed/readable.
	revokeToken := task("export-revoke", "alice", "revoke-context")
	revoke := func() error { return ids.CancelSubtree(ctx, scope, "export-revoke") }
	fault.after.Store(&revoke)
	call(revokeToken, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "revoked"}, 403)
	fault.after.Store(nil)
	staged := fault.last.Load()
	if staged == nil {
		t.Fatal("no physical PUT before revoke")
	}
	if _, err = ac(revokeToken).Get(ctx, staged.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked read: %v", err)
	}
	// A new task of the same context still cannot read the unsealed object.
	replacement := task("export-replacement", "alice", "revoke-context")
	if _, err = ac(replacement).Get(ctx, staged.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("revoked export was published: %v", err)
	}

	task("export-expire", "alice", "expire-context")
	expiring, err := ids.MintTask(ctx, scope, "export-expire", signer, "ahp", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	expire := func() error { time.Sleep(2100 * time.Millisecond); return nil }
	fault.after.Store(&expire)
	call(expiring, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "expired-after-put"}, 403)
	fault.after.Store(nil)
	expiryReplacement := task("export-expire-replacement", "alice", "expire-context")
	if _, e := ac(expiryReplacement).Get(ctx, fault.last.Load().ID); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatalf("expired export was published: %v", e)
	}
	// A lifecycle writer entering inside paused Seal cannot change the source
	// generation before publication, even with the ordinary pool MaxConns=1.
	patchLeases, e := compute.NewLeaseService(compute.New(pool), compute.NewRedis(os.Getenv("AHP_REDIS_ADDR")))
	if e != nil {
		t.Fatal(e)
	}
	patchRunner := compute.NewRunner(compute.New(pool), compute.NewRegistry(), jail, "test", nil).WithLeaseService(patchLeases)
	var blocked atomic.Bool
	fence := func() error {
		patchCtx, stopPatch := context.WithTimeout(ctx, 200*time.Millisecond)
		_, pe := patchRunner.TypedPatch(patchCtx, scope, id, "main", "out.txt", hex.EncodeToString(sum[:]), []byte("must-not-write"), 0600)
		stopPatch()
		if contracts.Code(pe) != contracts.ErrConflict {
			return contracts.NewError(contracts.ErrConflict, "patch bypassed export writer guard")
		}
		c, stop := context.WithTimeout(ctx, 150*time.Millisecond)
		defer stop()
		_, e := compute.New(pool).AcquireIncarnation(c, scope, id, "host", "test", "blocked-generation")
		if e == nil || c.Err() == nil {
			return contracts.NewError(contracts.ErrConflict, "lifecycle changed inside Seal")
		}
		blocked.Store(true)
		return nil
	}
	fault.beforeSeal.Store(&fence)
	call(token, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "lifecycle-serialized"}, 200)
	fault.beforeSeal.Store(nil)
	if !blocked.Load() {
		t.Fatal("paused Seal did not exercise lifecycle mutation")
	}
	inc, e := compute.New(pool).AcquireIncarnation(ctx, scope, id, "host", "test", "after-seal")
	if e != nil {
		t.Fatal("guard not released", e)
	}
	if e = compute.New(pool).StopIncarnation(ctx, scope, id, inc.ID); e != nil {
		t.Fatal(e)
	}
	// A writer can occupy the only ordinary connection while waiting on the
	// source guard. Identity revalidation must use the separate authority pool.
	writerDone := make(chan error, 1)
	var concurrentInc compute.Incarnation
	writerCtx, stopWriter := context.WithTimeout(ctx, 5*time.Second)
	defer stopWriter()
	startWriter := func() error {
		go func() {
			var e error
			concurrentInc, e = compute.New(pool).AcquireIncarnation(writerCtx, scope, id, "host", "test", "concurrent-writer")
			writerDone <- e
		}()
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		for pool.Stat().AcquiredConns() != 1 {
			select {
			case <-deadline.C:
				return contracts.NewError(contracts.ErrUnavailable, "writer failed to occupy pool1")
			case <-time.After(time.Millisecond):
			}
		}
		return nil
	}
	checkWaiting := func() error {
		select {
		case e := <-writerDone:
			return contracts.NewError(contracts.ErrConflict, "writer passed source guard before Seal: "+fmt.Sprint(e))
		default:
			return nil
		}
	}
	fault.after.Store(&startWriter)
	fault.beforeSeal.Store(&checkWaiting)
	call(token, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "pool-one-concurrent-writer"}, 200)
	fault.after.Store(nil)
	fault.beforeSeal.Store(nil)
	if e := <-writerDone; e != nil {
		t.Fatal("concurrent writer failed after Seal", e)
	}
	if e := compute.New(pool).StopIncarnation(ctx, scope, id, concurrentInc.ID); e != nil {
		t.Fatal(e)
	}
	// Current grant labels are inherited at the owner, rather than caller labels.
	if _, err = ids.AddLabels(ctx, scope, "export-main", labels.Set{Workspaces: []string{"not-visible-project"}}); err != nil {
		t.Fatal(err)
	}
	labelled := call(token, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "labelled"}, 200)
	labelRef, e := ac(token).Get(ctx, labelled["artifact_id"].(string))
	if e != nil || len(labelRef.Labels.Workspaces) != 1 || labelRef.Labels.Workspaces[0] != "not-visible-project" {
		t.Fatalf("live labels not frozen: %+v %v", labelRef.Labels, e)
	}
	secret := func() error {
		_, e := ids.AddLabels(ctx, scope, "export-main", labels.Set{Taints: []string{labels.TaintSecret}})
		return e
	}
	fault.after.Store(&secret)
	call(token, "Export", map[string]any{"computer_id": id, "path": "out.txt", "key": "secret-after-put"}, 403)
	fault.after.Store(nil)
}

func TestComputeExportAuthorityStartupFailureClosesPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, e := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	name := ("compute-authority-" + contracts.NewID())[:50]
	cfg.ConnConfig.RuntimeParams["application_name"] = name
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	t.Setenv("AHP_ARTIFACTS_URL", "https://example.invalid")
	t.Setenv("AHP_ARTIFACTS_TOKEN", "synthetic-static-refused")
	s := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, e = s.buildComputeHandler(ctx); e == nil {
		t.Fatal("startup accepted static bearer")
	}
	count := func() int {
		t.Helper()
		var n int
		if e := pool.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1", name).Scan(&n); e != nil {
			t.Fatal(e)
		}
		return n
	}
	if n := count(); n != 1 {
		t.Fatalf("startup error leaked authority connections: %d", n)
	}
	t.Setenv("AHP_ARTIFACTS_URL", "")
	t.Setenv("AHP_ARTIFACTS_TOKEN", "")
	mountedCtx, stop := context.WithCancel(ctx)
	if _, e = s.buildComputeHandler(mountedCtx); e != nil {
		stop()
		t.Fatal(e)
	}
	if n := count(); n < 2 {
		stop()
		t.Fatal("authority pool was not actually connected")
	}
	stop()
	deadline := time.Now().Add(2 * time.Second)
	for count() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("authority pool remained connected after context shutdown")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func runComputeExportPython(t *testing.T, ctx context.Context, srv *httptest.Server, cert tls.Certificate, cfg map[string]any) computesdk.ArtifactRef {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if e := os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
		return path
	}
	cfg["ca"] = write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	cfg["cert"] = write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
	key, e := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if e != nil {
		t.Fatal(e)
	}
	cfg["key"] = write("client.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	output := filepath.Join(dir, "output.json")
	cfg["output"] = output
	raw, _ := json.Marshal(cfg)
	input := write("config.json", raw)
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	python := os.Getenv("AHP_SDK_PYTHON")
	if python == "" {
		python = "python3"
	}
	cmd := exec.CommandContext(ctx, python, "cmd/ahp/testdata/compute_export.py", input)
	cmd.Dir = root
	path := filepath.Join(root, "sdk/python")
	if extra := os.Getenv("PYTHONPATH"); extra != "" {
		path += string(os.PathListSeparator) + extra
	}
	cmd.Env = append(os.Environ(), "PYTHONPATH="+path)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("public Python export: %v\n%s", e, out)
	} else {
		t.Log(strings.TrimSpace(string(out)))
	}
	raw, e = os.ReadFile(output)
	if e != nil {
		t.Fatal(e)
	}
	var ref computesdk.ArtifactRef
	if e = json.Unmarshal(raw, &ref); e != nil {
		t.Fatal(e)
	}
	return ref
}
