//go:build ahp_data

package main

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The two reference branches W-1 shipped untested: a workspace revision (head,
// branch or pin) and a memory record that names the artifact or the digest of
// its bytes. DEF-H24-15 proved the second workspace shape matters on its own --
// an artifact that became a file of an OPEN candidate is in use by a Publish
// that has not happened yet, which is exactly what a chat deletion used to
// destroy.
//
// Both packages are migrated for real and the durable rows are written into
// their own tables, so the probe is checked against the schema it will actually
// read. The per-action allowlists of those two planes are not the subject here
// (they are mounted and tested separately), hence the permissive authorize.

const notReferencedDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func livenessAllow(context.Context, contracts.Scope, string, string) error { return nil }

func migrateWorkspace(t *testing.T, ctx context.Context, e *livenessEnv) {
	t.Helper()
	wsStore, err := workspace.New(e.pool, e.seed, livenessAllow)
	if err != nil {
		t.Fatalf("workspace store: %v", err)
	}
	if err := wsStore.Migrate(ctx); err != nil {
		t.Fatalf("workspace migrate: %v", err)
	}
}

func migrateMemory(t *testing.T, ctx context.Context, e *livenessEnv) {
	t.Helper()
	memStore, err := memory.New(e.pool, nil, livenessAllow, nil)
	if err != nil {
		t.Fatalf("memory store: %v", err)
	}
	if err := memStore.Migrate(ctx); err != nil {
		t.Fatalf("memory migrate: %v", err)
	}
}

func (e *livenessEnv) digestOf(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	var sha string
	if err := e.pool.QueryRow(ctx,
		`SELECT COALESCE(sha256,'') FROM ahp_artifacts.artifacts WHERE id = $1`, id).Scan(&sha); err != nil {
		t.Fatalf("read digest of %s: %v", id, err)
	}
	if sha == "" {
		t.Fatalf("artifact %s has no sealed digest to reference", id)
	}
	return sha
}

func (e *livenessEnv) sweepWithAuthority(t *testing.T, ctx context.Context, name string) artifacts.GCReport {
	t.Helper()
	return e.sweep(t, ctx, name, artifacts.WithLiveness(e.live), artifacts.WithReferenceScan(e.live))
}

// A file of the workspace's head revision is a live reference: the sweep must
// pass it, and it must become collectable once the head moves away.
func TestWorkspaceHeadRevisionReferenceSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	migrateWorkspace(t, ctx, e)

	id, object := e.sealTombstonedExpired(t, ctx, "ctxwsh-"+t.Name(), "taskwsh-"+t.Name(), "workspace head body")
	wsID, revID := "ws-"+e.scope.Tenant, "rev-"+e.scope.Tenant
	// Order follows the schema's own foreign keys: workspace, then its revision,
	// then the file row, then the head pointer that makes the revision current.
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.workspaces (id, tenant, name, head_rev)
VALUES ($1,$2,$3,NULL)`, wsID, e.scope.Tenant, "wsname-"+t.Name()); err != nil {
		t.Fatalf("workspace: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.revisions (id, workspace_id, seq, digest, author_subject)
VALUES ($1,$2,1,'digest-head','subject-h23w')`, revID, wsID); err != nil {
		t.Fatalf("revision: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.files (workspace_id, rev_id, path, artifact_id, mode, digest)
VALUES ($1,$2,'report.bin',$3,420,'file-digest')`, wsID, revID, id); err != nil {
		t.Fatalf("file row: %v", err)
	}
	if _, err := e.pool.Exec(ctx,
		`UPDATE ahp_workspace.workspaces SET head_rev = $2 WHERE id = $1`, wsID, revID); err != nil {
		t.Fatalf("point head: %v", err)
	}

	report := e.sweepWithAuthority(t, ctx, "workspace-head-protects")
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("the head revision still uses this artifact: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("protected bytes must survive: versions=%d", got)
	}

	// Move the head to another revision. The reference is gone with it, so the
	// same policy collects the same row: the probe is decisive, not permissive.
	if _, err := e.pool.Exec(ctx,
		`UPDATE ahp_workspace.workspaces SET head_rev = 'rev-elsewhere' WHERE id = $1`, wsID); err != nil {
		t.Fatalf("move head: %v", err)
	}
	report = e.sweepWithAuthority(t, ctx, "head-moved-collects")
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("nothing references this artifact any more: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("bytes must be gone: versions=%d", got)
	}
}

// DEF-H24-15: an artifact that became a file of an OPEN workspace candidate is
// in use by a Publish that has not happened yet. Deleting the conversation that
// produced it must not take the bytes; a rejected candidate must not hold them
// forever either.
func TestOpenWorkspaceCandidateReferenceSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	migrateWorkspace(t, ctx, e)

	id, object := e.sealTombstonedExpired(t, ctx, "ctxwc-"+t.Name(), "taskwc-"+t.Name(), "candidate body")
	wsID, candID := "wsc-"+e.scope.Tenant, "cand-"+e.scope.Tenant
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.workspaces (id, tenant, name, head_rev)
VALUES ($1,$2,$3,NULL)`, wsID, e.scope.Tenant, "wscname-"+t.Name()); err != nil {
		t.Fatalf("workspace: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.candidates (id, workspace_id, base_rev, head_at_create, digest, state)
VALUES ($1,$2,'','',  'cand-digest','pending')`, candID, wsID); err != nil {
		t.Fatalf("candidate: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.candidate_files (candidate_id, path, artifact_id, mode, digest)
VALUES ($1,'report.bin',$2,420,'file-digest')`, candID, id); err != nil {
		t.Fatalf("candidate file: %v", err)
	}

	report := e.sweepWithAuthority(t, ctx, "open-candidate-protects")
	if !e.rowPresent(t, ctx, id) {
		t.Fatalf("an open candidate still names this artifact: report=%+v", report)
	}
	if got := e.versions(t, object); got != 1 {
		t.Fatalf("the bytes a Publish needs must survive: versions=%d", got)
	}

	// A rejected candidate is not a reason to keep bytes: the same row, the same
	// policy, collected.
	if _, err := e.pool.Exec(ctx,
		`UPDATE ahp_workspace.candidates SET state = 'rejected' WHERE id = $1`, candID); err != nil {
		t.Fatalf("reject candidate: %v", err)
	}
	report = e.sweepWithAuthority(t, ctx, "rejected-candidate-collects")
	if e.rowPresent(t, ctx, id) {
		t.Fatalf("a rejected candidate must not pin bytes forever: report=%+v", report)
	}
	if got := e.versions(t, object); got != 0 {
		t.Fatalf("bytes must be gone: versions=%d", got)
	}
}

// A memory record protects the artifact its evidence names, and -- because a
// memory may cite the bytes rather than the row -- the digest of an artifact is
// a reference too. Both stop protecting when the record goes.
func TestMemoryEvidenceReferenceSurvivesSweep(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	migrateMemory(t, ctx, e)

	byID, objID := e.sealTombstonedExpired(t, ctx, "ctxmid-"+t.Name(), "taskmid-"+t.Name(), "memory evidence body")
	byDigest, objDigest := e.sealTombstonedExpired(t, ctx, "ctxmdg-"+t.Name(), "taskmdg-"+t.Name(), "memory digest body")
	sha := e.digestOf(t, ctx, byDigest)

	insertMemory := func(id, evidence string) {
		t.Helper()
		if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_memory.memories
    (id, tenant, deployment, kind, subject, text, text_digest, op_key, op_digest, evidence)
VALUES ($1,$2,'d-h23w','user','subject-h23w',$3,$4,$5,'op-digest',$6::jsonb)`,
			id, e.scope.Tenant, "memory citing "+id, "text-digest-"+id, "opkey-"+id, evidence); err != nil {
			t.Fatalf("memory row %s: %v", id, err)
		}
	}
	// The evidence shape is the package's own: a list of objects naming artifacts.
	insertMemory("mem-by-id-"+e.scope.Tenant, `[{"artifact_id":"`+byID+`","kind":"artifact"}]`)
	insertMemory("mem-by-digest-"+e.scope.Tenant, `[{"sha256":"`+sha+`","kind":"artifact"}]`)

	report := e.sweepWithAuthority(t, ctx, "memory-evidence-protects")
	for _, pair := range [][2]string{{byID, objID}, {byDigest, objDigest}} {
		if !e.rowPresent(t, ctx, pair[0]) {
			t.Fatalf("a memory record names this artifact (%s): report=%+v", pair[0], report)
		}
		if got := e.versions(t, pair[1]); got != 1 {
			t.Fatalf("bytes named by memory evidence must survive (%s): versions=%d", pair[0], got)
		}
	}

	if _, err := e.pool.Exec(ctx, `DELETE FROM ahp_memory.memories WHERE tenant = $1`, e.scope.Tenant); err != nil {
		t.Fatalf("remove memory rows: %v", err)
	}
	report = e.sweepWithAuthority(t, ctx, "memory-removed-collects")
	for _, id := range []string{byID, byDigest} {
		if e.rowPresent(t, ctx, id) {
			t.Fatalf("nothing cites %s any more: report=%+v", id, report)
		}
	}
}

// TestReferenceAuthorityAnswersForCandidateAndMemory is the authority-level leg,
// asked directly rather than through a sweep. On the canonical commit this call
// fails with SQLSTATE 42P18 in its memory branch, which means every sweep was a
// fail-closed guess and the collector could never have run with the authority
// mounted. Here it must answer, and it must see both durable facts.
func TestReferenceAuthorityAnswersForCandidateAndMemory(t *testing.T) {
	ctx := context.Background()
	e := newLivenessEnv(t, ctx)
	migrateWorkspace(t, ctx, e)
	migrateMemory(t, ctx, e)

	candID, _ := e.sealTombstonedExpired(t, ctx, "ctxac-"+t.Name(), "taskac-"+t.Name(), "authority candidate")
	memID, _ := e.sealTombstonedExpired(t, ctx, "ctxam-"+t.Name(), "taskam-"+t.Name(), "authority memory")
	sha := e.digestOf(t, ctx, memID)

	wsID := "wsauth-" + e.scope.Tenant
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.workspaces (id, tenant, name, head_rev) VALUES ($1,$2,$3,NULL)`,
		wsID, e.scope.Tenant, "wsauthname-"+e.scope.Tenant); err != nil {
		t.Fatalf("workspace: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.candidates (id, workspace_id, base_rev, head_at_create, digest, state)
VALUES ($1,$2,'','','auth-digest','pending')`, "candauth-"+e.scope.Tenant, wsID); err != nil {
		t.Fatalf("candidate: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_workspace.candidate_files (candidate_id, path, artifact_id, mode, digest)
VALUES ($1,'a.bin',$2,420,'file-digest')`, "candauth-"+e.scope.Tenant, candID); err != nil {
		t.Fatalf("candidate file: %v", err)
	}
	if _, err := e.pool.Exec(ctx, `
INSERT INTO ahp_memory.memories
    (id, tenant, deployment, kind, subject, text, text_digest, op_key, op_digest, evidence)
VALUES ($1,$2,'d-h23w','user','subject-h23w',$3,$4,$5,'op-digest',$6::jsonb)`,
		"memauth-"+e.scope.Tenant, e.scope.Tenant, "authority memory text", "text-digest-auth",
		"opkey-auth", `[{"sha256":"`+sha+`","kind":"artifact"}]`); err != nil {
		t.Fatalf("memory row: %v", err)
	}

	referenced, err := e.live.ArtifactsReferenced(ctx, e.scope.Tenant, []artifacts.ArtifactQuery{
		{ID: candID, Digest: e.digestOf(t, ctx, candID)},
		{ID: memID, Digest: sha},
		{ID: "art-not-referenced", Digest: notReferencedDigest},
	})
	if err != nil {
		t.Fatalf("the reference authority must answer, not fail closed: %v", err)
	}
	for _, want := range []string{candID, memID} {
		if !referenced[want] {
			t.Fatalf("%s is named by a durable row and must be reported: %v", want, referenced)
		}
	}
	if referenced["art-not-referenced"] {
		t.Fatalf("nothing names this artifact: %v", referenced)
	}
}
