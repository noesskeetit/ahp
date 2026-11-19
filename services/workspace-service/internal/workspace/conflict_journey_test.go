package workspace

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// The user-visible conflict journey over real PostgreSQL + MinIO (root-аудит
// 11-workspace-store.md п.1, PDF §3.7 с.37): checkout → две независимые
// правки → WORKSPACE_CONFLICT → сохранённая ветка → осознанный merge →
// новый head. Plus the negative half (аудит п.3): чужой tenant не видит и не
// пишет, viewer не публикует, revoked member между Prepare и Commit loses
// the right to merge.
func TestConflictJourneyTwoAuthorsConsciousMerge(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice, bob := "alice", "bob"

	w, err := e.ws.Create(ctx, e.scope(alice), "team")
	mustOK(t, err)
	mustOK(t, e.ws.AddMember(ctx, e.scope(alice), w.Name, bob))

	seed := e.uploadSeal(t, alice, "readme.md", "# v0", labels.Set{})
	base := e.pub(t, alice, w.Name, "", []File{seed})

	// Both authors take the same checkout (same base revision).
	candA, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, base.ID)
	mustOK(t, err)
	candB, err := e.ws.CreateCandidate(ctx, e.scope(bob), w.Name, base.ID)
	mustOK(t, err)

	// Independent edits to the SAME path. Publishing is the owner's act in
	// this product (role table: publish = owner), so the editor prepares and
	// the owner commits — the audit's journey holds with that boundary.
	fileA := repath(e.uploadSeal(t, alice, "readme-a.md", "# v0\nalice was here", labels.Set{}), "readme.md")
	e.share(t, alice, w.Name, fileA)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(alice), candA.ID, []File{fileA})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(alice), candA.ID, "ci", "passed", ""))
	revA, err := e.ws.Publish(ctx, e.scope(alice), candA.ID, base.ID)
	mustOK(t, err)

	// bob prepares the same-path edit against the moved head; when the owner
	// commits it, WORKSPACE_CONFLICT returns, head stays, and bob's snapshot
	// is preserved on a conflict branch.
	fileB := repath(e.uploadSeal(t, bob, "readme-b.md", "# v0\nbob was here", labels.Set{}), "readme.md")
	e.share(t, bob, w.Name, fileB)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(bob), candB.ID, []File{fileB})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(bob), candB.ID, "ci", "passed", ""))
	mustErr(t, publishErr(e.ws, ctx, e.scope(alice), candB.ID, base.ID), string(contracts.ErrConflict))

	head, err := e.ws.GetHead(ctx, e.scope(bob), w.Name)
	mustOK(t, err)
	if head != revA.ID {
		t.Fatalf("head moved off the merged revision: %q", head)
	}
	gotB, err := e.ws.GetCandidate(ctx, e.scope(bob), candB.ID)
	mustOK(t, err)
	if gotB.State != "conflict" || gotB.ConflictBranch != "conflicts/"+candB.ID {
		t.Fatalf("conflict not recorded for bob: %+v", gotB)
	}

	// The snapshot survives as a readable branch revision with bob's bytes.
	branchRev, err := e.ws.GetBranch(ctx, e.scope(bob), w.Name, gotB.ConflictBranch)
	mustOK(t, err)
	rc, fb, _, err := e.ws.ReadFile(ctx, e.scope(bob), w.Name, branchRev.ID, "readme.md", 0, 0)
	mustOK(t, err)
	gotBranch, _ := io.ReadAll(rc)
	rc.Close()
	wantBranch := "# v0\nbob was here"
	gotDigest := strings.TrimPrefix(fb.Digest, "sha256:")
	if string(gotBranch) != wantBranch || gotDigest != shaHex(wantBranch) {
		t.Fatalf("conflict branch content lost: %q digest %q", gotBranch, fb.Digest)
	}
	branches, err := e.ws.ListConflictBranches(ctx, e.scope(alice), w.Name)
	mustOK(t, err)
	if len(branches) != 1 || branches[0].Name != gotB.ConflictBranch {
		t.Fatalf("conflict branch missing from listing: %+v", branches)
	}

	// Conscious merge: bob re-checks out the new head and combines both
	// edits himself — the platform never auto-resolves the same-path clash.
	merged := repath(e.uploadSeal(t, bob, "readme-c.md", "# v0\nalice was here\nbob was here", labels.Set{}), "readme.md")
	e.share(t, bob, w.Name, merged)
	candC, err := e.ws.CreateCandidate(ctx, e.scope(bob), w.Name, revA.ID)
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(bob), candC.ID, []File{merged})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(bob), candC.ID, "ci", "passed", ""))
	revC, err := e.ws.Publish(ctx, e.scope(alice), candC.ID, revA.ID)
	mustOK(t, err)
	rc2, _, _, err := e.ws.ReadFile(ctx, e.scope(alice), w.Name, revC.ID, "readme.md", 0, 0)
	mustOK(t, err)
	gotMerged, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(gotMerged) != "# v0\nalice was here\nbob was here" {
		t.Fatalf("merged head wrong: %q", gotMerged)
	}
	// History is append-only: base, alice, and bob's merge all still resolve,
	// and the conflict branch was not garbage-collected by the merge.
	revs, err := e.ws.ListRevisions(ctx, e.scope(alice), w.Name)
	mustOK(t, err)
	if len(revs) != 4 { // seed + revA + bob's conflict snapshot + merged head
		t.Fatalf("revision history wrong: %+v", revs)
	}
	if _, err := e.ws.GetBranch(ctx, e.scope(bob), w.Name, gotB.ConflictBranch); err != nil {
		t.Fatalf("conflict branch lost after conscious merge: %v", err)
	}

	// A revision used by a live working copy cannot be purged (PDF §7 с.48:
	// ревизия не удаляется, пока её использует рабочая копия). bob continues
	// from his conflict snapshot; while that copy is open the snapshot is
	// undeletable, and it becomes purgeable only when the copy is closed.
	candW, err := e.ws.CreateCandidate(ctx, e.scope(bob), w.Name, branchRev.ID)
	mustOK(t, err)
	mustErr(t, e.ws.PurgeRevision(ctx, e.scope(alice), w.Name, branchRev.ID), string(contracts.ErrConflict))
	if _, err := e.ws.GetRevision(ctx, e.scope(alice), w.Name, branchRev.ID); err != nil {
		t.Fatalf("purge attempt damaged the branch revision: %v", err)
	}
	if _, err := e.ws.RejectCandidate(ctx, e.scope(alice), candW.ID); err != nil {
		t.Fatalf("reject working copy: %v", err)
	}
	mustOK(t, e.ws.PurgeRevision(ctx, e.scope(alice), w.Name, branchRev.ID))

	// Negative path: a same-tenant stranger is refused without learning the
	// project exists; a foreign tenant cannot even resolve its name; a viewer
	// cannot publish a prepared candidate.
	stranger := e.scope("carol")
	if _, err := e.ws.GetWorkspace(ctx, stranger, w.Name); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("same-tenant stranger got %s, want FORBIDDEN", contracts.Code(err))
	}
	outsider := contracts.Scope{Tenant: e.tenant + "-x", Deployment: "d-1", Subject: "mallory"}
	if _, err := e.ws.GetWorkspace(ctx, outsider, w.Name); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant saw the project: %v", err)
	}
	if _, err := e.ws.CreateCandidate(ctx, outsider, w.Name, revC.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant opened a candidate: %v", err)
	}
	// The foreign tenant cannot push a same-named project's candidate into
	// this one: from its side the workspace does not exist, and bob's
	// prepared candidate below stays unmergeable for it.
	mustOK(t, e.ws.AddMember(ctx, e.scope(alice), w.Name, "viewer1"))
	if _, err := e.pool.Exec(ctx,
		`UPDATE ahp_workspace.members SET role='viewer', is_owner=false WHERE workspace_id=$1 AND subject='viewer1'`,
		w.ID); err != nil {
		t.Fatal(err)
	}
	candV, err := e.ws.CreateCandidate(ctx, e.scope(alice), w.Name, revC.ID)
	mustOK(t, err)
	mustErr(t, publishErr(e.ws, ctx, e.scope("viewer1"), candV.ID, revC.ID), string(contracts.ErrForbidden))

	// A member revoked between Prepare and Commit loses everything on that
	// candidate, and the foreign tenant cannot merge the prepared work
	// either (аудит п.3: revoke between Prepare and Commit).
	candD, err := e.ws.CreateCandidate(ctx, e.scope(bob), w.Name, revC.ID)
	mustOK(t, err)
	draft := e.uploadSeal(t, bob, "extra.md", "late edit", labels.Set{})
	e.share(t, bob, w.Name, draft)
	_, err = e.ws.UpdateCandidateFiles(ctx, e.scope(bob), candD.ID, []File{merged, draft})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, e.scope(bob), candD.ID, "ci", "passed", ""))
	mustOK(t, e.ws.RemoveMember(ctx, e.scope(alice), w.Name, bob))
	mustErr(t, publishErr(e.ws, ctx, e.scope(bob), candD.ID, revC.ID), string(contracts.ErrForbidden))
	mustErr(t, publishErr(e.ws, ctx, outsider, candD.ID, revC.ID), string(contracts.ErrNotFound))
}

// repath places a sealed artifact at a workspace path that differs from its
// upload key: uploads are idempotent per key, revisions are addressed per
// path, and the two planes deliberately do not share a namespace.
func repath(f File, p string) File {
	f.Path = p
	return f
}
