package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The import-hydration seam, owned by the artifact.
//
// A cross-context State import copies the checkpoint, which names an artifact
// bound to the SOURCE conversation; the imported conversation may not read it
// (TestCrossContextImportDoesNotHydrateArtifactReads, cmd/ahp). That refusal is
// correct in isolation and useless in production, so the widening has to be an
// operation OF THIS PACKAGE: the owner conversation admits one more of the same
// subject's conversations, durably, with the source kept immutable and every
// abuse refused. These legs are the contract.

func TestContextGrantHydratesAnotherOfTheSameSubjectsConversations(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	source, target := "ctx-grant-src", "ctx-grant-dst"
	body := "the checkpoint of an exported conversation names these bytes"

	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: source, TaskID: "task-" + source}, "k-"+source, body)
	srcCtx := WithCallerContext(ctx, source)
	dstCtx := WithCallerContext(ctx, target)

	// The gap, restated so the fix is measured against it and not around it.
	if _, err := e.store.Get(dstCtx, e.scope, ref.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("before any grant the imported conversation must be refused, got %v", err)
	}

	if err := e.store.GrantContext(srcCtx, e.scope, ref.ID, target, "grant-1"); err != nil {
		t.Fatalf("GrantContext: %v", err)
	}
	granted, err := e.store.Get(dstCtx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("hydration: the admitted conversation still cannot read: %v", err)
	}
	if granted.SHA256 != ref.SHA256 {
		t.Fatalf("admitted read returned a different record: %+v", granted)
	}
	object := e.objectOf(t, ref.ID)
	raw, err := e.rawGet(t, object, ref.Version)
	if err != nil || string(raw) != body {
		t.Fatalf("out-of-band bytes after grant: err=%v len=%d", err, len(raw))
	}

	// Durable, attributable, and the source is not moved.
	grant, err := e.store.GetContextGrant(srcCtx, e.scope, ref.ID, target)
	if err != nil {
		t.Fatalf("the grant is not durable: %v", err)
	}
	if grant.SourceContext != source || grant.GrantedBy != source || grant.Revoked {
		t.Fatalf("grant row lost its provenance: %+v", grant)
	}
	var binding string
	if err := e.store.pool.QueryRow(ctx,
		`SELECT COALESCE(context_id,'') FROM ahp_artifacts.artifacts WHERE id=$1`, ref.ID).Scan(&binding); err != nil {
		t.Fatalf("read binding: %v", err)
	}
	if binding != source {
		t.Fatalf("the grant rewrote the artifact's binding context: %q", binding)
	}

	// Idempotency: the same key and the same arguments are one effect, not two.
	if err := e.store.GrantContext(srcCtx, e.scope, ref.ID, target, "grant-1"); err != nil {
		t.Fatalf("replaying the same grant must succeed: %v", err)
	}
	if rows := e.grantRows(t, ctx, ref.ID); rows != 1 {
		t.Fatalf("a replayed grant produced %d rows, want exactly 1", rows)
	}
	if rows := e.grantAuditRows(t, ctx, "grant-1"); rows != 1 {
		t.Fatalf("a replayed grant wrote %d audit rows, want exactly 1", rows)
	}
	// The same key with different arguments is the platform's CONFLICT, not a
	// silent second effect.
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, ref.ID, "ctx-other-target", "grant-1"),
		string(contracts.ErrConflict))

	// Revocation: the admitted conversation loses the read again, and the
	// artifact and its own conversation are untouched.
	if err := e.store.RevokeContext(srcCtx, e.scope, ref.ID, target, "revoke-1"); err != nil {
		t.Fatalf("RevokeContext: %v", err)
	}
	if _, err := e.store.Get(dstCtx, e.scope, ref.ID); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("after revoke the admitted conversation must be refused again, got %v", err)
	}
	if _, err := e.store.Get(srcCtx, e.scope, ref.ID); err != nil {
		t.Fatalf("revoking a grant must not disturb the owner conversation: %v", err)
	}
	if g, err := e.store.GetContextGrant(srcCtx, e.scope, ref.ID, target); err != nil || !g.Revoked {
		t.Fatalf("the revoked grant row must remain readable for audit: %+v err=%v", g, err)
	}
}

// The refusals. Each one is a way the widening could become the hole
// read_scope.go exists to close: a third conversation pulling its subject's
// artifacts to itself, another subject's artifact, tainted labels laundered
// into a new audience, or a tombstone being re-opened by the back door.
func TestContextGrantRefusals(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	source, target := "ctx-refuse-src", "ctx-refuse-dst"

	owner, _ := e.sealBoundWithKey(t, Binding{ContextID: source, TaskID: "task-" + source}, "k-refuse", "consent matters")
	srcCtx := WithCallerContext(ctx, source)

	// 1. No consent: a different conversation of the SAME subject cannot widen
	// an artifact to itself. This is exactly the measured stand defect
	// same_subject_other_task_download=200, in grant form.
	third := WithCallerContext(ctx, "ctx-refuse-third")
	codeIs(t, e.store.GrantContext(third, e.scope, owner.ID, "ctx-refuse-third", "g-third"),
		string(contracts.ErrForbidden))
	if _, err := e.store.Get(WithCallerContext(ctx, "ctx-refuse-third"), e.scope, owner.ID); err == nil {
		t.Fatal("an unconsented conversation reads the artifact: the gate is gone")
	}

	// 2. Another subject of the same tenant.
	codeIs(t, e.store.GrantContext(srcCtx, e.foreign, owner.ID, target, "g-mallory"),
		string(contracts.ErrForbidden))
	// A different tenant does not even see the row.
	otherTenant := contracts.Scope{Tenant: "t-not-mine", Deployment: "d1", Subject: "alice"}
	codeIs(t, e.store.GrantContext(srcCtx, otherTenant, owner.ID, target, "g-tenant"),
		string(contracts.ErrNotFound))

	// 3. Tainted labels are not laundered into a new conversation. The store
	// refuses to ingest a secret- or external-tainted artifact for this caller
	// at all (both measured above), so the taint is written the way a
	// policy-derived label would be: into the stored label set of a sealed
	// artifact. The leg under test is what a grant does about it, not how the
	// label arrived.
	tainted, err := e.store.BeginUploadWithBinding(ctx, e.scope,
		Binding{ContextID: source, TaskID: "task-taint"}, "k-taint", UploadRequest{
			Name: "t.bin", MIME: "text/plain",
			SHA256: shaHexOf("bytes that became tainted"), Size: int64(len("bytes that became tainted")),
		})
	if err != nil {
		t.Fatalf("BeginUploadWithBinding: %v", err)
	}
	put(t, tainted.PresignedURL, "bytes that became tainted")
	sealedTainted, err := e.store.Seal(ctx, e.scope, tainted.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if _, err := e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET labels = $2::jsonb WHERE id=$1`,
		sealedTainted.ID, `{"taints":["secret"]}`); err != nil {
		t.Fatalf("store the taint: %v", err)
	}
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, sealedTainted.ID, target, "g-taint"),
		string(contracts.ErrForbidden))

	// 4. A tombstone is not re-opened through the grant path.
	if err := e.store.Delete(ctx, e.scope, owner.ID, "del-refuse"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, owner.ID, target, "g-deleted"),
		string(contracts.ErrNotFound))

	// 5. A malformed target is refused before anything is written.
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, owner.ID, "bad/target id", "g-bad"),
		string(contracts.ErrInvalid))
	// 6. The policy callback still decides: a denied authority changes nothing.
	// It is asked about an artifact that still exists, so the refusal that comes
	// back is the authority's, not the tombstone's.
	e.allow.Store(false)
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, sealedTainted.ID, target, "g-policy"),
		string(contracts.ErrForbidden))
	e.allow.Store(true)
	if rows := e.grantRows(t, ctx, sealedTainted.ID); rows != 0 {
		t.Fatalf("a refused grant still wrote %d rows", rows)
	}
	// 7. And a target that names no conversation at all is refused before
	// anything is written: a grant to nobody is a grant to everybody.
	codeIs(t, e.store.GrantContext(srcCtx, e.scope, sealedTainted.ID, "", "g-empty"),
		string(contracts.ErrInvalid))
}

func (e *env) grantRows(t *testing.T, ctx context.Context, artifactID string) int {
	t.Helper()
	var n int
	if err := e.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_artifacts.context_grants WHERE artifact_id=$1`, artifactID).Scan(&n); err != nil {
		t.Fatalf("count grant rows: %v", err)
	}
	return n
}

func (e *env) grantAuditRows(t *testing.T, ctx context.Context, opKey string) int {
	t.Helper()
	var n int
	if err := e.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_artifacts.context_grant_audit WHERE op_key=$1`, opKey).Scan(&n); err != nil {
		t.Fatalf("count audit rows: %v", err)
	}
	return n
}

// An idempotency key belongs to the scope that used it, exactly as
// (tenant, deployment, subject, idem_key) arbitrates uploads. A global key
// would make one owner's replay a CONFLICT for an unrelated owner who happens
// to pick the same string, and inside a shared database that is not a
// hypothetical: it is what the first version of this table did.
func TestContextGrantIdempotencyKeyIsScopedToItsOwner(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	other := contracts.Scope{Tenant: e.scope.Tenant + "-b", Deployment: e.scope.Deployment, Subject: e.scope.Subject}
	srcA, srcB := "ctx-scope-src-a", "ctx-scope-src-b"
	dstA, dstB := "ctx-scope-dst-a", "ctx-scope-dst-b"
	const sharedKey = "grant-key-reused-by-two-owners"

	owned, _ := e.sealBoundWithKey(t, Binding{ContextID: srcA, TaskID: "task-" + srcA}, "k-scope-a", "first owner's bytes")
	secondBody := "second owner's bytes"
	up, err := e.store.BeginUploadWithBinding(ctx, other,
		Binding{ContextID: srcB, TaskID: "task-" + srcB}, "k-scope-b", UploadRequest{
			Name: "b.bin", MIME: "text/plain",
			SHA256: shaHexOf(secondBody), Size: int64(len(secondBody)),
		})
	if err != nil {
		t.Fatalf("BeginUploadWithBinding: %v", err)
	}
	put(t, up.PresignedURL, secondBody)
	ownedByOther, err := e.store.Seal(ctx, other, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	srcACtx, srcBCtx := WithCallerContext(ctx, srcA), WithCallerContext(ctx, srcB)
	if err := e.store.GrantContext(srcACtx, e.scope, owned.ID, dstA, sharedKey); err != nil {
		t.Fatalf("first owner's grant: %v", err)
	}
	if err := e.store.GrantContext(srcBCtx, other, ownedByOther.ID, dstB, sharedKey); err != nil {
		t.Fatalf("a second owner may reuse the same key string: %v", err)
	}
	if _, err := e.store.Get(WithCallerContext(ctx, dstA), e.scope, owned.ID); err != nil {
		t.Fatalf("the first owner's grant was lost: %v", err)
	}
	if _, err := e.store.Get(WithCallerContext(ctx, dstB), other, ownedByOther.ID); err != nil {
		t.Fatalf("the second owner's grant was lost: %v", err)
	}

	// Each scope keeps its own single audit row, and within its own scope the
	// key is still one-use: replay is a replay, other arguments are a conflict.
	if n := e.scopedGrantAuditRows(t, ctx, e.scope, sharedKey); n != 1 {
		t.Fatalf("first owner wrote %d audit rows for its key, want 1", n)
	}
	if n := e.scopedGrantAuditRows(t, ctx, other, sharedKey); n != 1 {
		t.Fatalf("second owner wrote %d audit rows for its key, want 1", n)
	}
	if err := e.store.GrantContext(srcACtx, e.scope, owned.ID, dstA, sharedKey); err != nil {
		t.Fatalf("replaying within the owning scope must succeed: %v", err)
	}
	codeIs(t, e.store.GrantContext(srcACtx, e.scope, owned.ID, "ctx-scope-dst-other", sharedKey),
		string(contracts.ErrConflict))
}

func (e *env) scopedGrantAuditRows(t *testing.T, ctx context.Context, scope contracts.Scope, opKey string) int {
	t.Helper()
	var n int
	if err := e.store.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_artifacts.context_grant_audit
		  WHERE op_key=$1 AND tenant=$2 AND deployment=$3 AND subject=$4`,
		opKey, scope.Tenant, scope.Deployment, scope.Subject).Scan(&n); err != nil {
		t.Fatalf("count scoped audit rows: %v", err)
	}
	return n
}
