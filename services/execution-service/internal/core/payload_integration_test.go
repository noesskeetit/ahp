package core

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	artifactstore "github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/space"
	statestore "github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	artifactclient "github.com/ahp-platform/ahp/sdk/go/artifacts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// TestRealPayloadOwners uses real State/Artifacts owners, PostgreSQL and MinIO
// through mTLS and signed task grants. It never uses an LLM and is not LLM E2E.
func TestRealPayloadOwners(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL required for real payload owners")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid PG configuration")
	}
	defer pool.Close()
	ep, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || ep.Host == "" {
		t.Fatal("S3 endpoint required")
	}
	s3, err := minio.New(ep.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: ep.Scheme == "https"})
	if err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("ahp-payload-%d", time.Now().UnixNano())
	var tightenedLabels atomic.Bool
	ast, err := artifactstore.New(pool, s3, bucket, func(context.Context, contracts.Scope, string, string) error { return nil }, artifactstore.WithGrantLabels(func(_ context.Context, s contracts.Scope) (labels.Set, error) {
		current := labels.Set{Subjects: []string{s.Subject}}
		if tightenedLabels.Load() {
			current.Workspaces = []string{"review-workspace"}
		}
		return current, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err = ast.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for obj := range s3.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err == nil {
				_ = s3.RemoveObject(context.Background(), bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
			}
		}
		_ = s3.RemoveBucket(context.Background(), bucket)
	}()
	st := statestore.New(pool)
	st.SetCheckpointArtifacts(ast)
	source := statestore.ReferenceSource(pool, "ahp_state")
	scan := artifactstore.ReferenceScanFunc(func(ctx context.Context, tenant string, arts []artifactstore.ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(arts))
		for i, a := range arts {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := source.InspectReferences(ctx, tenant, nil, queries)
		return result.Referenced, err
	})
	artifactstore.WithReferenceKeeper(scan)(ast)
	artifactstore.WithCheckpointSettlement(st.CheckpointOperationSettled)(ast)
	if err = st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("payload-test", "k", priv)
	verifier := auth.NewVerifier("payload-test", map[string]ed25519.PublicKey{"k": pub})
	tenant := fmt.Sprintf("payload-%d", time.Now().UnixNano())
	scope := contracts.Scope{Tenant: tenant, Deployment: "runner", Subject: "alice"}
	mint := func(subject, contextID, task string) string {
		tok, e := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: "runner", Subject: subject, Task: task, Root: "root", Context: contextID, Grant: "grant"}, 10*time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		return tok
	}
	var revoked, denyUpload atomic.Bool
	authorize := func(context.Context, auth.Claims, string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "revoked")
		}
		return nil
	}
	ah, err := artifactsapi.New(ast, verifier, authorize)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := stateapi.New(st, verifier, authorize, func(_ context.Context, _ contracts.Scope, task string) (string, error) {
		return mint("alice", "ctx", task), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/state/", sh)
	mux.Handle("/v1/artifacts/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if denyUpload.Load() && r.URL.Path == "/v1/artifacts/BeginUpload" {
			w.WriteHeader(503)
			return
		}
		ah.ServeHTTP(w, r)
	}))
	ca := newPayloadCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	clientCert := ca.clientCert(t, "spiffe://ahp/tenant/"+tenant+"/deployment/runner")
	tr.TLSClientConfig.Certificates = []tls.Certificate{clientCert}
	hc.Transport = tr
	token := mint("alice", "ctx", "bootstrap")
	sc, err := sdkstate.New(sdkstate.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	th, err := sc.CreateThread(ctx, "ctx", "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := sc.CreateRun(ctx, th.ID, "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := sc.ClaimRunFor(ctx, "worker", 120000, run.ID)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	token = lease.TaskToken
	fence := sdkstate.Fence{RunID: run.ID, Worker: "worker", Epoch: lease.Lease.Epoch}
	big := bytes.Repeat([]byte("large checkpoint\x00\xff"), 100000)
	metadata, _ := json.Marshal(map[string]string{"large": string(bytes.Repeat([]byte("m"), 600000)), "serializer": "preserved"})
	cp := sdkstate.Checkpoint{ID: "cp", ThreadID: th.ID, Namespace: "payload", Version: 1, Metadata: metadata, Parts: []sdkstate.CheckpointPart{{Channel: "large", Version: "v1", Data: big}}}
	if _, err = sc.PutCheckpoint(ctx, fence, cp, "cp"); err != nil {
		t.Fatal(err)
	}
	tightenedLabels.Store(true)
	if _, err = sc.PutCheckpoint(ctx, fence, cp, "cp"); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	stored, err := st.GetPart(ctx, scope, th.ID, "payload", "large", "v1")
	if err != nil || len(stored) > 1<<20 || bytes.Equal(stored, big) {
		t.Fatalf("State did not store a bounded reference: %v", err)
	}
	fresh := sc.WithTokenSource(func(context.Context) (string, error) { return token, nil })
	got, err := fresh.GetPart(ctx, th.ID, "payload", "large", "v1")
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("real hydrate: %v", err)
	}
	header, err := fresh.GetCheckpoint(ctx, th.ID, "payload", "cp")
	if err != nil || !bytes.Equal(header.Metadata, metadata) {
		t.Fatalf("metadata mismatch %v", err)
	}
	if err = sc.PutWrites(ctx, fence, "payload", "cp", []sdkstate.PendingWrite{{Task: "pending", Index: -1, Channel: "large", Data: big}}); err != nil {
		t.Fatal(err)
	}
	pending, err := fresh.GetWrites(ctx, th.ID, "payload", "cp")
	if err != nil || len(pending) != 1 || !bytes.Equal(pending[0].Data, big) {
		t.Fatal("pending writes mismatch", err)
	}

	// Upgrade reads start from owner-stored legacy values, never from SDK spill.
	legacyMetadata := json.RawMessage(`{"$ahp_checkpoint_payload_v1":"AEFIUC1QQVlMT0FELXYxCkl7ImtlZXAiOiJsZWdhY3kifQ=="}`)
	for i, raw := range [][]byte{[]byte("\x00AHP-PAYLOAD-v1\nIlegacy"), []byte("\x00AHP-PAYLOAD-v1\nRnot-a-reference")} {
		id := fmt.Sprint("legacy", i)
		legacy := statestore.Checkpoint{ID: id, Namespace: "upgrade", Version: 1, Metadata: legacyMetadata, Parts: []statestore.Part{{Channel: id, Version: "v1", Data: raw}}}
		if _, e := st.PutCheckpoint(ctx, scope, statestore.Fence{RunID: run.ID, Worker: fence.Worker, Epoch: fence.Epoch}, legacy, id); e != nil {
			t.Fatal(e)
		}
		got, e := fresh.GetPart(ctx, th.ID, "upgrade", id, "v1")
		if e != nil || !bytes.Equal(got, raw) {
			t.Fatalf("preexisting owner bytes changed: %x %v", got, e)
		}
		gotCP, e := fresh.GetCheckpoint(ctx, th.ID, "upgrade", id)
		if e != nil {
			t.Fatal(e)
		}
		var a, b any
		_ = json.Unmarshal(gotCP.Metadata, &a)
		_ = json.Unmarshal(legacyMetadata, &b)
		if !reflect.DeepEqual(a, b) {
			t.Fatal("preexisting owner metadata changed")
		}
	}
	// The aggregate includes base64 plus JSON escaping and both batch halves.
	var batchParts []sdkstate.CheckpointPart
	var batchWrites []sdkstate.PendingWrite
	boundary := bytes.Repeat([]byte{0xff, 0x00, 0x22, 0x5c}, 50000)
	for i := 0; i < 4; i++ {
		batchParts = append(batchParts, sdkstate.CheckpointPart{Channel: fmt.Sprint("c", i), Version: "v1", Data: boundary})
		batchWrites = append(batchWrites, sdkstate.PendingWrite{Task: "batch-task", Index: int32(i), Channel: fmt.Sprint("c", i), Data: boundary})
	}
	batchMeta, _ := json.Marshal(map[string]string{"escaped": string(bytes.Repeat([]byte{0}, 100000))})
	batchCP := sdkstate.Checkpoint{ID: "batch", ThreadID: th.ID, Namespace: "aggregate", Version: 1, Metadata: batchMeta, Parts: batchParts}
	if _, err = sc.PutBatch(ctx, fence, sdkstate.BatchRequest{Key: "batch", Checkpoint: &batchCP, Writes: batchWrites}); err != nil {
		t.Fatalf("aggregate spill failed: %v", err)
	}
	for _, p := range batchParts {
		data, e := fresh.GetPart(ctx, th.ID, "aggregate", p.Channel, "v1")
		if e != nil || !bytes.Equal(data, boundary) {
			t.Fatal("aggregate bytes changed", e)
		}
	}
	batchRows, e := fresh.GetWrites(ctx, th.ID, "aggregate", "batch")
	if e != nil || len(batchRows) != 4 || !bytes.Equal(batchRows[3].Data, boundary) {
		t.Fatal("batch pending bytes changed", e)
	}
	// A distinct version with an empty value is not a reference to a missing blob.
	if _, err = sc.PutCheckpoint(ctx, fence, sdkstate.Checkpoint{ID: "empty", ThreadID: th.ID, Namespace: "boundary", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "empty", Version: "v1", Data: []byte{}}}}, "empty"); err != nil {
		t.Fatal("empty channel lost", err)
	}

	for _, n := range []int{1, 256 << 10, (256 << 10) + 1, 1 << 20} {
		raw := bytes.Repeat([]byte{0x81}, n)
		id := fmt.Sprint("boundary-", n)
		if _, err = sc.PutCheckpoint(ctx, fence, sdkstate.Checkpoint{ID: id, ThreadID: th.ID, Namespace: "boundary", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "sized", Version: id, Data: raw}}}, id); err != nil {
			t.Fatalf("boundary %d: %v", n, err)
		}
		stored, e := st.GetPart(ctx, scope, th.ID, "boundary", "sized", id)
		if e != nil {
			t.Fatal(e)
		}
		if (n <= 256<<10) != bytes.Equal(stored, raw) {
			t.Fatalf("wrong spill boundary %d", n)
		}
		restored, e := fresh.GetPart(ctx, th.ID, "boundary", "sized", id)
		if e != nil || !bytes.Equal(restored, raw) {
			t.Fatalf("boundary restore %d: %v", n, e)
		}
	}

	ac, err := artifactclient.New(artifactclient.Config{BaseURL: srv.URL, HTTPClient: hc})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(string(bytes.Repeat([]byte("й"), 1200000)))
	ex := &payloadTool{data: raw}
	registry := NewToolRegistry(ex)
	registry.Register(ToolDef{Name: "large"})
	rt := NewRuntime(RuntimeConfig{Tools: registry, Artifacts: ac, State: space.NewStateClient(srv.URL, hc)})
	req := &space.CoreRequest{RunID: run.ID, ThreadID: th.ID, ContextID: "ctx", TaskToken: token, Worker: fence.Worker, Epoch: fence.Epoch, Scope: scope}
	tc := ToolCall{ID: "effect", Name: "large", Args: json.RawMessage(`{}`)}
	result, err := rt.executeTool(ctx, req, rt.newMutationJournal(req), tc)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) > 64<<10 {
		t.Fatal("large result in State")
	}
	var out externalOutput
	if err = json.Unmarshal(result, &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Artifact.Labels.Subjects) != 1 || out.Artifact.Labels.Subjects[0] != "alice" {
		t.Fatal("task labels lost")
	}
	duplicate, err := rt.executeTool(ctx, req, rt.newMutationJournal(req), tc)
	var replay externalOutput
	_ = json.Unmarshal(duplicate, &replay)
	if err != nil || !reflect.DeepEqual(replay, out) || ex.calls != 1 {
		t.Fatalf("double dispatch/replay mismatch: calls=%d err=%v", ex.calls, err)
	}
	args, _ := json.Marshal(map[string]any{"id": out.Artifact.ID, "version": out.Artifact.Version, "offset": 4, "length": 32768})
	if _, err = rt.readOutputChunk(ctx, req, args); err != nil {
		t.Fatal(err)
	}
	for _, foreign := range []struct{ subject, context string }{{"mallory", "ctx"}, {"alice", "foreign"}} {
		copy := *req
		copy.TaskToken = mint(foreign.subject, foreign.context, run.ID)
		if _, err = rt.readOutputChunk(ctx, &copy, args); err == nil {
			t.Fatal("foreign output read allowed")
		}
	}
	// Failed upload after a completed effect is durably UNKNOWN. Restart may not dispatch again.
	denyUpload.Store(true)
	j := rt.newMutationJournal(req)
	j.next = 1
	if _, err = rt.executeTool(ctx, req, j, tc); contracts.ReasonOf(err) != "SIDE_EFFECT_UNKNOWN" {
		t.Fatalf("wanted unknown: %v", err)
	}
	denyUpload.Store(false)
	j = rt.newMutationJournal(req)
	j.next = 1
	if _, err = rt.executeTool(ctx, req, j, tc); !EffectRefused(err) || ex.calls != 2 {
		t.Fatalf("unknown redispatch: %d %v", ex.calls, err)
	}
	// Optionally launch the real LangGraph saver against these exact owners.
	if python := os.Getenv("AHP_PAYLOAD_PYTHON"); python != "" {
		dir := t.TempDir()
		write := func(name string, data []byte) string {
			path := filepath.Join(dir, name)
			if e := os.WriteFile(path, data, 0600); e != nil {
				t.Fatal(e)
			}
			return path
		}
		serverDER := srv.TLS.Certificates[0].Certificate[0]
		caFile := write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}))
		certFile := write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]}))
		keyDER, e := x509.MarshalPKCS8PrivateKey(clientCert.PrivateKey)
		if e != nil {
			t.Fatal(e)
		}
		keyFile := write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
		handshake, _ := json.Marshal(map[string]any{"base_url": srv.URL, "ca_file": caFile, "cert_file": certFile, "key_file": keyFile, "task_token": token, "thread_id": th.ID, "fence": fence})
		handFile := write("handshake.json", handshake)
		cmd := exec.CommandContext(ctx, python, "../../sdk/python/ahp_state/tests/payload_live.py", handFile)
		cmd.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python")
		if raw, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("LangGraph real owner failed: %v\n%s", e, raw)
		} else {
			t.Log(string(raw))
		}
	}
	revoked.Store(true)
	if _, err = fresh.GetPart(ctx, th.ID, "payload", "large", "v1"); err == nil {
		t.Fatal("revoked checkpoint read allowed")
	}
	if _, err = rt.readOutputChunk(ctx, req, args); err == nil {
		t.Fatal("revoked output read allowed")
	}
	revoked.Store(false)
	testCheckpointGarbageCollection(t, ctx, st, ast, pool, scope, th.ID, stored, func() {
		got, e := fresh.GetPart(ctx, th.ID, "payload", "large", "v1")
		if e != nil || !bytes.Equal(got, big) {
			t.Fatalf("post-GC hydration: %v", e)
		}
	})
	t.Log("real PG + MinIO + mTLS checkpoint/pending/core/chunk/replay/unknown/authority checks passed")
}

// This proves sequential retention and lifecycle cleanup against real owners.
// Write-versus-collection arbitration is a separate cross-owner contract.
func testCheckpointGarbageCollection(t *testing.T, ctx context.Context, st *statestore.Store, ast *artifactstore.Store, pool *pgxpool.Pool, scope contracts.Scope, thread string, descriptor []byte, hydrate func()) {
	t.Helper()
	var ref artifactclient.ArtifactRef
	if err := json.Unmarshal(descriptor, &ref); err != nil {
		t.Fatal(err)
	}
	source := statestore.ReferenceSource(pool, "ahp_state")
	scan := artifactstore.ReferenceScanFunc(func(ctx context.Context, tenant string, arts []artifactstore.ArtifactQuery) (map[string]bool, error) {
		queries := make([]references.Artifact, len(arts))
		for i, a := range arts {
			queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		result, err := source.InspectReferences(ctx, tenant, nil, queries)
		return result.Referenced, err
	})
	gc, err := artifactstore.NewGC(ast, artifactstore.WithReferenceScan(scan))
	if err != nil {
		t.Fatal(err)
	}
	if err = ast.Delete(ctx, scope, ref.ID, "retention-test"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("live checkpoint artifact deletion must be refused: %v", err)
	}
	// Exact private fixture row only: simulate a legacy tombstone. Current
	// Delete refuses it above, but GC must also protect old tombstones that
	// already exist. This does not bypass the API in the caller-visible path.
	if _, err = pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted=true,deleted_at=now()-interval '8 days' WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	policy := artifactstore.GCPolicy{TombstoneRetention: time.Millisecond, StagedGrace: time.Hour, OrphanGrace: time.Hour}
	report, err := gc.RunOnce(ctx, scope, policy)
	if err != nil || report.PurgedTombstoned != 0 || report.SkippedLive < 1 {
		t.Fatalf("checkpoint reference lost to GC: %+v %v", report, err)
	}
	if _, err = ast.Restore(ctx, scope, ref.ID); err != nil {
		t.Fatal(err)
	}
	hydrate()
	if err = st.SetLegalhold(ctx, scope, thread, "checkpoint retention test"); err != nil {
		t.Fatal(err)
	}
	if err = st.DeleteThread(ctx, scope, thread); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("held thread deletion: %v", err)
	}
	protected, err := scan.ArtifactsReferenced(ctx, scope.Tenant, []artifactstore.ArtifactQuery{{ID: ref.ID}})
	if err != nil || !protected[ref.ID] {
		t.Fatalf("legal hold lost checkpoint reference: %v %v", protected, err)
	}
	if err = st.RemoveLegalhold(ctx, scope, thread); err != nil {
		t.Fatal(err)
	}
	if err = st.DeleteThread(ctx, scope, thread); err != nil {
		t.Fatal(err)
	}
	protected, err = scan.ArtifactsReferenced(ctx, scope.Tenant, []artifactstore.ArtifactQuery{{ID: ref.ID}})
	if err != nil || protected[ref.ID] {
		t.Fatalf("deleted thread retained reference: %v %v", protected, err)
	}
	if err = ast.Delete(ctx, scope, ref.ID, "cleanup-test"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET deleted_at=now()-interval '8 days' WHERE id=$1`, ref.ID); err != nil {
		t.Fatal(err)
	}
	report, err = gc.RunOnce(ctx, scope, policy)
	if err != nil || report.PurgedTombstoned != 1 || report.VersionsRemoved < 1 {
		t.Fatalf("unreferenced checkpoint artifact not collected: %+v %v", report, err)
	}
	t.Log("real GC preserves checkpoint bytes, restores exact hydration, respects legal hold, and collects after thread cleanup")
}
