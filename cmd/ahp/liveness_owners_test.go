//go:build ahp_data

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// W-1, owner authority through the owners' own APIs.
//
// liveness_workspace_memory_test.go already proves the workspace and memory
// reference queries answer for the durable row shapes. What was not proven is
// the seam: that the workspace and memory packages WRITE exactly those shapes
// when used the way the front uses them. A reference query that reads a column
// no product path ever fills would pass the SQL-shaped leg and still leave
// published material unprotected.
//
// So these legs build the reference through the owner's own API - Create ->
// CreateCandidate -> UpdateCandidateFiles -> SetCheck -> Publish to head, and
// memory.Propose with an evidence artifact - and only then tombstone the
// artifact. Each leg also proves its own decisiveness: move the head / forget
// the memory, and the very next sweep collects the row.
//
// The conversation is deliberately settled (a terminal A2A task) so the ONLY
// thing protecting the artifact is the owner reference under test.

func ownerOnlyScope(t *testing.T, ctx context.Context, e *livenessEnv, body string) (string, string, string) {
	t.Helper()
	contextID := "ctxown-" + t.Name()
	taskID := "taskown-" + t.Name()
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.contexts (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,'d-h23w','subject-h23w','caller')`,
		contextID, e.scope.Tenant); err != nil {
		t.Fatalf("exec context: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_exec.tasks (task_id, tenant, deployment, subject, caller, context_id, state, terminal_at)
VALUES ($1,$2,'d-h23w','subject-h23w','caller',$3,'TASK_STATE_COMPLETED', now())`,
		taskID, e.scope.Tenant, contextID); err != nil {
		t.Fatalf("exec task: %v", err)
	}
	id := sealArtifact(t, ctx, e.seed, e.scope, artifacts.Binding{ContextID: contextID, TaskID: taskID}, body)
	sum := sha256.Sum256([]byte(body))
	return id, hex.EncodeToString(sum[:]), contextID
}

// tombstoneAndExpire deletes the artifact and pushes its retention floor an hour
// into the past: from here on, only a live reference can save it. The store is a
// parameter because an artifact that joined a workspace is readable only through
// a store wired with workspace access - the same fail-closed invisibility the
// production front relies on.
func tombstoneAndExpire(t *testing.T, ctx context.Context, e *livenessEnv, store *artifacts.Store, id string) {
	t.Helper()
	if err := store.Delete(ctx, e.scope, id, "del-"+id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	past := time.Now().UTC().Add(-time.Hour)
	if err := store.SetRetention(ctx, e.scope, id, &past); err != nil {
		t.Fatalf("SetRetention: %v", err)
	}
}

func TestPublishedWorkspaceHeadKeepsArtifactReferenced(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	// The wiring the front uses (cmd/ahp/role_data.go): the artifacts store can
	// read a workspace-restricted artifact only where the caller is a member.
	// Without this option PublishToWorkspace refuses, which is what the first
	// run of this leg proved.
	var wsRef *workspace.Store
	wsArts, err := artifacts.New(e.pool, e.client, e.bucket, allow,
		artifacts.WithWorkspaceAccess(func(ctx context.Context, sc contracts.Scope, w string) error {
			return wsRef.IsMember(ctx, sc, w)
		}))
	if err != nil {
		t.Fatalf("artifacts store with workspace access: %v", err)
	}
	ws, err := workspace.New(e.pool, wsArts, allow)
	if err != nil {
		t.Fatalf("workspace store: %v", err)
	}
	wsRef = ws
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, q := range []string{
			`DELETE FROM ahp_workspace.files WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.candidate_files WHERE candidate_id IN (SELECT c.id FROM ahp_workspace.candidates c JOIN ahp_workspace.workspaces w ON w.id=c.workspace_id WHERE w.tenant=$1)`,
			`DELETE FROM ahp_workspace.checks WHERE candidate_id IN (SELECT c.id FROM ahp_workspace.candidates c JOIN ahp_workspace.workspaces w ON w.id=c.workspace_id WHERE w.tenant=$1)`,
			`DELETE FROM ahp_workspace.pins WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.branches WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.revisions WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.candidates WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.members WHERE workspace_id IN (SELECT id FROM ahp_workspace.workspaces WHERE tenant=$1)`,
			`DELETE FROM ahp_workspace.workspaces WHERE tenant=$1`,
		} {
			_, _ = e.pool.Exec(c, q, e.scope.Tenant)
		}
	})

	id, digest, _ := ownerOnlyScope(t, ctx, e, "published workspace body")

	project, err := ws.Create(ctx, e.scope, "proj-h23w")
	if err != nil {
		t.Fatalf("workspace Create: %v", err)
	}
	// The publication grant is the product's own: an artifact joins a workspace
	// through PublishToWorkspace, and Publish refuses without it.
	if err := wsArts.PublishToWorkspace(ctx, e.scope, id, project.Name); err != nil {
		t.Fatalf("PublishToWorkspace: %v", err)
	}
	cand, err := ws.CreateCandidate(ctx, e.scope, project.Name, "")
	if err != nil {
		t.Fatalf("CreateCandidate: %v", err)
	}
	if _, err := ws.UpdateCandidateFiles(ctx, e.scope, cand.ID, []workspace.File{
		{Path: "notes.md", ArtifactID: id, Mode: 0o644, Digest: digest},
	}); err != nil {
		t.Fatalf("UpdateCandidateFiles: %v", err)
	}
	if err := ws.SetCheck(ctx, e.scope, cand.ID, "ci", "passed", ""); err != nil {
		t.Fatalf("SetCheck: %v", err)
	}
	rev, err := ws.Publish(ctx, e.scope, cand.ID, "")
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Out of band: the durable head really points at the revision carrying the
	// artifact, so the protection that follows cannot come from a cache.
	var onHead bool
	if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_workspace.files f
		JOIN ahp_workspace.workspaces w ON w.id=f.workspace_id
		WHERE w.tenant=$1 AND f.artifact_id=$2 AND w.head_rev=$3)`,
		e.scope.Tenant, id, rev.ID).Scan(&onHead); err != nil {
		t.Fatalf("workspace head probe: %v", err)
	}
	if !onHead {
		t.Fatal("the published head revision does not carry the artifact; this leg would prove nothing")
	}

	tombstoneAndExpire(t, ctx, e, wsArts, id)

	referenced, err := e.live.ArtifactsReferenced(ctx, e.scope.Tenant,
		[]artifacts.ArtifactQuery{{ID: id, Digest: digest}})
	if err != nil {
		t.Fatalf("ArtifactsReferenced: %v", err)
	}
	if !referenced[id] {
		t.Fatal("the workspace owner must report its head revision's artifact as referenced")
	}

	object := e.objectOf(t, ctx, id)
	report := e.sweep(t, ctx, "workspace-head",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("a workspace head revision must keep its artifact: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("protected bytes must survive: versions=%d", got)
	}

	// Decisiveness: publish the next head WITHOUT that file. Nothing alive
	// refers to the artifact now, so the same policy collects it.
	next, err := ws.CreateCandidate(ctx, e.scope, project.Name, rev.ID)
	if err != nil {
		t.Fatalf("CreateCandidate 2: %v", err)
	}
	replacement := sealArtifact(t, ctx, e.seed, e.scope,
		artifacts.Binding{ContextID: "ctxown-" + t.Name(), TaskID: "taskown-" + t.Name()}, "replacement body")
	if err := wsArts.PublishToWorkspace(ctx, e.scope, replacement, project.Name); err != nil {
		t.Fatalf("PublishToWorkspace replacement: %v", err)
	}
	if _, err := ws.UpdateCandidateFiles(ctx, e.scope, next.ID, []workspace.File{
		{Path: "other.md", ArtifactID: replacement, Mode: 0o644},
	}); err != nil {
		t.Fatalf("UpdateCandidateFiles 2: %v", err)
	}
	if err := ws.SetCheck(ctx, e.scope, next.ID, "ci", "passed", ""); err != nil {
		t.Fatalf("SetCheck 2: %v", err)
	}
	if _, err := ws.Publish(ctx, e.scope, next.ID, rev.ID); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}
	referenced, err = e.live.ArtifactsReferenced(ctx, e.scope.Tenant,
		[]artifacts.ArtifactQuery{{ID: id, Digest: digest}})
	if err != nil {
		t.Fatalf("ArtifactsReferenced after head moved: %v", err)
	}
	if referenced[id] {
		t.Fatal("an artifact that left the head revision must stop being referenced")
	}
	report = e.sweep(t, ctx, "workspace-head-moved",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("with the head moved away the row is collectable: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("collected bytes must be gone: versions=%d", got)
	}
}

func TestProposedMemoryEvidenceKeepsArtifactReferenced(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	allow := func(context.Context, contracts.Scope, string, string) error { return nil }
	mem, err := memory.New(e.pool, e.seed, allow, nil)
	if err != nil {
		t.Fatalf("memory store: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = e.pool.Exec(c, `DELETE FROM ahp_memory.operations WHERE tenant=$1`, e.scope.Tenant)
		_, _ = e.pool.Exec(c, `DELETE FROM ahp_memory.memories WHERE tenant=$1`, e.scope.Tenant)
	})

	id, digest, _ := ownerOnlyScope(t, ctx, e, "memory evidence body")

	res, err := mem.Propose(ctx, e.scope, memory.ProposeRequest{
		Kind:       memory.KindUser,
		Text:       "the note that cites the artifact",
		OpKey:      "opk-h23w-" + id,
		SourceKind: "chat",
		Evidence:   []memory.Evidence{{SourceKind: "artifact", Artifacts: []string{id}}},
	})
	if err != nil {
		t.Fatalf("memory Propose: %v", err)
	}

	var cited bool
	if err := e.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_memory.memories
		WHERE tenant=$1 AND state <> 'deleted' AND evidence::text LIKE '%'||chr(34)||$2||chr(34)||'%')`,
		e.scope.Tenant, id).Scan(&cited); err != nil {
		t.Fatalf("memory evidence probe: %v", err)
	}
	if !cited {
		t.Fatalf("memory %s did not durably cite the artifact; this leg would prove nothing", res.Memory.ID)
	}

	tombstoneAndExpire(t, ctx, e, e.seed, id)

	referenced, err := e.live.ArtifactsReferenced(ctx, e.scope.Tenant,
		[]artifacts.ArtifactQuery{{ID: id, Digest: digest}})
	if err != nil {
		t.Fatalf("ArtifactsReferenced: %v", err)
	}
	if !referenced[id] {
		t.Fatal("a live memory record citing the artifact must keep it referenced")
	}

	object := e.objectOf(t, ctx, id)
	report := e.sweep(t, ctx, "memory-evidence",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("a memory record's evidence must keep its artifact: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("protected bytes must survive: versions=%d", got)
	}

	// Decisiveness: forget the memory, and the artifact becomes collectable.
	if err := mem.Delete(ctx, e.scope, res.Memory.ID, "forget-"+id); err != nil {
		t.Fatalf("memory Delete: %v", err)
	}
	referenced, err = e.live.ArtifactsReferenced(ctx, e.scope.Tenant,
		[]artifacts.ArtifactQuery{{ID: id, Digest: digest}})
	if err != nil {
		t.Fatalf("ArtifactsReferenced after forgetting: %v", err)
	}
	if referenced[id] {
		t.Fatal("a deleted memory record must stop referencing the artifact")
	}
	report = e.sweep(t, ctx, "memory-forgotten",
		artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("nothing alive refers to this artifact any more: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("collected bytes must be gone: versions=%d", got)
	}
}
