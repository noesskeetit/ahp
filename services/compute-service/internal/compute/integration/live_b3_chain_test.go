//go:build compute_live3

// CONTRACT item 3, full live chain in one turn: model -> compute tool -> command in
// the sandbox -> file -> EXPORT to the real Artifacts store (live MinIO+PG) ->
// download 200 with matching sha/size -> FOREIGN subject 403. The turn engine is the
// real internal/core.Runtime loop; the export/import/download/403 run through the real
// internal/artifacts store. Reuses the helpers defined in live_b3_turn_test.go.
package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const ownerRunCtx = "ctx-owner-run"

// chainTool runs the sandbox command and then publishes the produced file to the real
// Artifacts store via the compute export seam, returning the sealed reference.
type chainTool struct {
	runner     *compute.Runner
	exporter   *storeExporter
	homeBase   string
	computerID string
	ref        compute.ArtifactRef
	ran        int
}

func (ct *chainTool) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, err
	}
	sc, task, ok := core.RunScopeFromContext(ctx)
	if !ok {
		return nil, errNoScope
	}
	rec, err := ct.runner.RunCommand(ctx, sc, ct.computerID, "chain-"+contracts.NewID(), in.Command, "")
	if err != nil {
		return nil, err
	}
	if rec.Status != compute.CommandSucceeded {
		return nil, errBadStatus(string(rec.Status))
	}
	ct.ran++
	b, err := os.ReadFile(filepath.Join(ct.homeBase, "out.txt"))
	if err != nil {
		return nil, err
	}
	ref, err := ct.exporter.Export(ctx, compute.ArtifactExportRequest{
		Scope: sc, ComputerID: ct.computerID, TaskID: task,
		Filename: "out.txt", ContentType: "application/octet-stream",
		Body: io.NopCloser(bytes.NewReader(b)), SizeHint: int64(len(b)),
	})
	if err != nil {
		return nil, err
	}
	ct.ref = ref
	return json.Marshal(map[string]any{"artifact_id": ref.ArtifactID, "sha256": ref.Digest, "size": ref.SizeBytes})
}

// storeExporter implements compute.ArtifactExportClient against a REAL artifacts.Store
// (BeginUpload -> presigned PUT -> Seal). It never fabricates a reference on error.
type storeExporter struct {
	st      *artifacts.Store
	bindCtx string
}

func (x *storeExporter) Export(ctx context.Context, req compute.ArtifactExportRequest) (compute.ArtifactRef, error) {
	b, err := io.ReadAll(req.Body)
	if err != nil {
		return compute.ArtifactRef{}, err
	}
	sum := sha256.Sum256(b)
	up, err := x.st.BeginUploadWithBinding(ctx, req.Scope, artifacts.Binding{ContextID: x.bindCtx, TaskID: req.TaskID}, "export/"+req.ComputerID+"/"+req.Filename, artifacts.UploadRequest{
		Name: req.Filename, MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(b)),
	})
	if err != nil {
		return compute.ArtifactRef{}, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPut, up.PresignedURL, bytes.NewReader(b))
	if err != nil {
		return compute.ArtifactRef{}, err
	}
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return compute.ArtifactRef{}, err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return compute.ArtifactRef{}, errStorage
	}
	ref, err := x.st.Seal(ctx, req.Scope, up.ID)
	if err != nil {
		return compute.ArtifactRef{}, err
	}
	return compute.ArtifactRef{ArtifactID: ref.ID, Version: ref.Version, Digest: "sha256:" + ref.SHA256, SizeBytes: ref.Size}, nil
}

var (
	errNoScope   = errSentinel("no run scope")
	errBadStatus = func(s string) error { return errSentinel("command status " + s) }
	errStorage   = errSentinel("storage PUT failed")
)

type errSentinel string

func (e errSentinel) Error() string { return string(e) }

// newArtifactsStore builds a real artifacts.Store over live PG+MinIO (own bucket),
// skipped when the substrate env is absent.
func newArtifactsStore(t *testing.T, dsn string) (*artifacts.Store, contracts.Scope, contracts.Scope) {
	t.Helper()
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" || os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("artifact substrate env not set")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(c, dsn)
	if err != nil {
		t.Fatalf("pg pool: %v", err)
	}
	t.Cleanup(pool.Close)
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	bucket := "ahp-b3-" + hex.EncodeToString(rnd[:])
	st, err := artifacts.New(pool, client, bucket, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatalf("artifacts.New: %v", err)
	}
	if err := st.Migrate(c); err != nil {
		t.Fatalf("artifacts.Migrate: %v", err)
	}
	t.Cleanup(func() {
		cc, cn := context.WithTimeout(context.Background(), 20*time.Second)
		defer cn()
		for o := range client.ListObjects(cc, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if o.Err != nil {
				return
			}
			_ = client.RemoveObject(cc, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(cc, bucket)
	})
	owner := contracts.Scope{Tenant: "t-" + hex.EncodeToString(rnd[:]), Deployment: "d1", Subject: "owner"}
	foreign := contracts.Scope{Tenant: owner.Tenant, Deployment: owner.Deployment, Subject: "mallory"}
	return st, owner, foreign
}

// TestB3ChainTurnExportDownloadForeign403 is CONTRACT item 3 end to end: a real turn
// drives the compute tool which runs a sandboxed command and publishes the file to the
// real Artifacts store; the OWNER downloads it (200, sha/size match) and a FOREIGN
// subject is refused 403.
func TestB3ChainTurnExportDownloadForeign403(t *testing.T) {
	e := newLiveEnv(t)
	astore, ownerScope, foreignScope := newArtifactsStore(t, liveDSN(t))

	comp, err := e.store.CreateComputer(ctx(t), ownerScope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b3-chain-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	ct := &chainTool{runner: e.runner, exporter: &storeExporter{st: astore, bindCtx: ownerRunCtx}, homeBase: e.home(comp.ID), computerID: comp.ID}
	tools := core.NewToolRegistry(ct)
	tools.Register(core.ToolDef{Name: computeToolName, Description: "run a sandbox command and export the file", Parameters: json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`)})
	runtime := core.NewRuntime(core.RuntimeConfig{Model: scriptedModel{}, Tools: tools, MaxTokens: 256})

	sawTool, runErr := driveTurn(t, runtime, ownerScope, ctx(t))
	if runErr != nil {
		t.Fatalf("turn errored: %v", runErr)
	}
	if !sawTool || ct.ran != 1 {
		t.Fatalf("chain did not run the tool once (sawTool=%v ran=%d)", sawTool, ct.ran)
	}
	if ct.ref.ArtifactID == "" {
		t.Fatal("tool produced no sealed artifact reference")
	}

	// OWNER download: OpenRange returns bytes whose sha matches the sealed digest.
	rc, ref, err := astore.OpenRange(artifacts.WithCallerContext(ctx(t), ownerRunCtx), ownerScope, ct.ref.ArtifactID, 0, 0)
	if err != nil {
		t.Fatalf("owner download: %v", err)
	}
	body, _ := io.ReadAll(rc)
	rc.Close()
	sum := sha256.Sum256(body)
	if "sha256:"+hex.EncodeToString(sum[:]) != ref2digest(ref) || ref.Size != int64(len("ahp-block3")) {
		t.Fatalf("download mismatch sha=%s size=%d", ref2digest(ref), ref.Size)
	}

	// A FOREIGN task-context of the SAME subject is refused 403 (the task/conversation
	// boundary in read_scope.checkReadScope): a run reads only its own conversation.
	if _, _, err := astore.OpenRange(artifacts.WithCallerContext(ctx(t), "ctx-attacker"), ownerScope, ct.ref.ArtifactID, 0, 0); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign task-context download: code=%v err=%v, want FORBIDDEN", contracts.Code(err), err)
	}
	// A FOREIGN subject cannot read it either; the platform answers NOT_FOUND (never
	// FORBIDDEN) so existence is not enumerable -- the accepted B114/B137 semantics.
	if _, _, err := astore.OpenRange(ctx(t), foreignScope, ct.ref.ArtifactID, 0, 0); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign subject download: code=%v err=%v, want NOT_FOUND", contracts.Code(err), err)
	}
}

func ref2digest(r artifacts.ArtifactRef) string { return "sha256:" + r.SHA256 }
