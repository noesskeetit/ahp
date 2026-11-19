package workspace

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// pub is the canonical happy-path merge: open a candidate over baseRev, set the
// exact file set, mark a passing check, and CAS-merge at the current head.
func (e *env) pub(t *testing.T, subject, ws, baseRev string, files []File) Revision {
	t.Helper()
	ctx := context.Background()
	sc := e.scope(subject)
	head, err := e.ws.GetHead(ctx, sc, ws)
	mustOK(t, err)
	cand, err := e.ws.CreateCandidate(ctx, sc, ws, baseRev)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, files); err != nil {
		t.Fatalf("UpdateCandidateFiles: %v", err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	e.share(t, subject, ws, files...)
	rev, err := e.ws.Publish(ctx, sc, cand.ID, head)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return rev
}

func TestWorkspaceCopyOnWriteImmutable(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)

	v1 := e.uploadSeal(t, alice, "a1", "first-body", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{v1})

	v2 := e.uploadSeal(t, alice, "a2", "second-body", labels.Set{})
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{{Path: v1.Path, ArtifactID: v2.ArtifactID, Mode: 0o644, Digest: v2.Digest}})

	// The base revision's file set and content must be unchanged after the
	// later merge — copy-on-write never rewrites history.
	base, err := e.ws.ListFiles(ctx, e.scope(alice), w.Name, rev1.ID)
	mustOK(t, err)
	if len(base) != 1 || base[0].ArtifactID != v1.ArtifactID {
		t.Fatalf("base revision mutated: %+v", base)
	}
	rc, _, _, err := e.ws.ReadFile(ctx, e.scope(alice), w.Name, rev1.ID, "a1", 0, 0)
	mustOK(t, err)
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "first-body" {
		t.Fatalf("base rev content changed: %q", got)
	}
	rc2, _, _, err := e.ws.ReadFile(ctx, e.scope(alice), w.Name, rev2.ID, "a1", 0, 0)
	mustOK(t, err)
	got2, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(got2) != "second-body" {
		t.Fatalf("head rev content wrong: %q", got2)
	}

	// History: newest sequence first, immutable digests.
	revs, err := e.ws.ListRevisions(ctx, e.scope(alice), w.Name)
	mustOK(t, err)
	if len(revs) != 2 || revs[0].Seq != 2 || revs[1].Seq != 1 {
		t.Fatalf("revision order wrong: %+v", revs)
	}
	if head, err := e.ws.GetHead(ctx, e.scope(alice), w.Name); err != nil || head != rev2.ID {
		t.Fatalf("head = %q, want %q (%v)", head, rev2.ID, err)
	}
}

func TestWorkspaceCASStaleHeadConflictBranch(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	p1 := e.uploadSeal(t, alice, "a", "v1", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{p1})

	// First merge moves head and changes a.
	pA := e.uploadSeal(t, alice, "aA", "conflict-A", labels.Set{})
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{pA})

	// Second candidate built on the old base with a stale expected_head.
	pB := e.uploadSeal(t, alice, "aB", "conflict-B", labels.Set{})
	sc := e.scope(alice)
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{pB}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	e.share(t, alice, w.Name, pB)
	conflictRev, err := e.ws.Publish(ctx, sc, cand.ID, rev1.ID)
	mustErr(t, err, string(contracts.ErrConflict))

	got, err := e.ws.GetCandidate(ctx, sc, cand.ID)
	mustOK(t, err)
	if got.State != "conflict" {
		t.Fatalf("candidate state = %s, want conflict", got.State)
	}
	want := "conflicts/" + cand.ID
	if got.ConflictBranch != want {
		t.Fatalf("conflict branch = %q, want %q", got.ConflictBranch, want)
	}
	// head stays at the successful merge.
	if head, err := e.ws.GetHead(ctx, sc, w.Name); err != nil || head != rev2.ID {
		t.Fatalf("head moved on conflict: %q (%v)", head, err)
	}
	br, err := e.ws.GetBranch(ctx, sc, w.Name, want)
	mustOK(t, err)
	if br.ID != conflictRev.ID {
		t.Fatalf("branch rev = %s, want %s", br.ID, conflictRev.ID)
	}
}

func TestWorkspaceNonOverlapMergeRecheck(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	p1 := e.uploadSeal(t, alice, "a", "base", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{p1})

	p2 := e.uploadSeal(t, alice, "b", "adds-b", labels.Set{})
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{p1, p2})

	// A candidate on the same base touching a different path (c) does not
	// overlap the b-only change, so it merges at the current head after recheck.
	p3 := e.uploadSeal(t, alice, "c", "adds-c", labels.Set{})
	sc := e.scope(alice)
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{p1, p3}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	e.share(t, alice, w.Name, p3)
	rev3, err := e.ws.Publish(ctx, sc, cand.ID, rev2.ID)
	mustOK(t, err)
	if head, err := e.ws.GetHead(ctx, sc, w.Name); err != nil || head != rev3.ID {
		t.Fatalf("non-overlap merge did not advance head: %q (%v)", head, err)
	}
}

func TestWorkspaceOverlapConflict(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	p1 := e.uploadSeal(t, alice, "a", "base", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{p1})

	p2E := e.uploadSeal(t, alice, "e1", "E", labels.Set{})
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{p1, p2E})

	// Overlapping candidate: touches b, which changed between base and head,
	// even though expected_head is current.
	// The candidate adds content at path "e1", the same path head added with a
	// different artifact — a genuine overlap on a path that changed between base
	// and head, even though expected_head is current.
	p2F := e.uploadSeal(t, alice, "e2", "F", labels.Set{})
	overlapping := File{Path: "e1", ArtifactID: p2F.ArtifactID, Mode: 0o644, Digest: p2F.Digest}
	e.share(t, alice, w.Name, overlapping)
	sc := e.scope(alice)
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{p1, overlapping}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	_, err = e.ws.Publish(ctx, sc, cand.ID, rev2.ID)
	mustErr(t, err, string(contracts.ErrConflict))
	if head, err := e.ws.GetHead(ctx, sc, w.Name); err != nil || head != rev2.ID {
		t.Fatalf("overlap advanced head: %q (%v)", head, err)
	}
}

func TestWorkspaceChecksGate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	sc := e.scope(alice)
	p1 := e.uploadSeal(t, alice, "a", "body", labels.Set{})
	e.share(t, alice, w.Name, p1)

	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{p1}); err != nil {
		t.Fatal(err)
	}
	// No checks -> conflict.
	if _, err := e.ws.Publish(ctx, sc, cand.ID, ""); err == nil {
		t.Fatal("publish without checks succeeded")
	} else {
		mustErr(t, err, string(contracts.ErrConflict))
	}
	// Failed check -> conflict.
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "failed", "boom"))
	if _, err := e.ws.Publish(ctx, sc, cand.ID, ""); err != nil {
		mustErr(t, err, string(contracts.ErrConflict))
	} else {
		t.Fatal("publish with failed check succeeded")
	}
	// Passing check -> merge.
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	rev, err := e.ws.Publish(ctx, sc, cand.ID, "")
	mustOK(t, err)

	// Editing a candidate clears its checks; a reused check cannot merge.
	cand2, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev.ID)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand2.ID, []File{p1}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand2.ID, "ci", "passed", ""))
	p2 := e.uploadSeal(t, alice, "a2", "changed", labels.Set{})
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand2.ID, []File{p2}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ws.Publish(ctx, sc, cand2.ID, rev.ID); err != nil {
		mustErr(t, err, string(contracts.ErrConflict))
	} else {
		t.Fatal("publish with cleared checks succeeded")
	}
}

func TestWorkspaceReadFileRange(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	body := "hello world"
	f := e.uploadSeal(t, alice, "f", body, labels.Set{})
	rev := e.pub(t, alice, w.Name, "", []File{f})
	sc := e.scope(alice)

	rc, _, total, err := e.ws.ReadFile(ctx, sc, w.Name, rev.ID, "f", 0, 0)
	mustOK(t, err)
	all, _ := io.ReadAll(rc)
	rc.Close()
	if string(all) != body || total != int64(len(body)) {
		t.Fatalf("full read = %q/%d", all, total)
	}
	rc2, _, _, err := e.ws.ReadFile(ctx, sc, w.Name, rev.ID, "f", 6, 5)
	mustOK(t, err)
	part, _ := io.ReadAll(rc2)
	rc2.Close()
	if string(part) != "world" {
		t.Fatalf("range read = %q, want world", part)
	}
	_, _, _, err = e.ws.ReadFile(ctx, sc, w.Name, rev.ID, "f", int64(len(body)), 0)
	mustErr(t, err, string(contracts.ErrInvalid))
}

func TestWorkspacePublicationLeakGate(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	carol := "carol"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	sc := e.scope(alice)

	// A single-subject private artifact may NOT be merged into a workspace.
	priv := e.uploadSeal(t, alice, "secret", "private-fact", labels.Set{Subjects: []string{alice}})
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{priv}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	_, err = e.ws.Publish(ctx, sc, cand.ID, "")
	mustErr(t, err, string(contracts.ErrForbidden))
	if head, err := e.ws.GetHead(ctx, sc, w.Name); err != nil || head != "" {
		t.Fatalf("private fact entered workspace, head=%q (%v)", head, err)
	}

	// Only after explicit workspace publication is the merge allowed.
	mustOK(t, e.arts.PublishToWorkspace(ctx, sc, priv.ArtifactID, w.Name))
	rev, err := e.ws.Publish(ctx, sc, cand.ID, "")
	mustOK(t, err)

	// A non-member in the same tenant cannot read it.
	_, _, _, err = e.ws.ReadFile(ctx, e.scope(carol), w.Name, rev.ID, "secret", 0, 0)
	mustErr(t, err, string(contracts.ErrForbidden))
}

func TestWorkspaceCrossTenantNotFound(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "shared")
	mustOK(t, err)
	p := e.uploadSeal(t, alice, "a", "x", labels.Set{})
	e.pub(t, alice, w.Name, "", []File{p})

	other := contracts.Scope{Tenant: e.tenant + "-other", Deployment: "d-1", Subject: "mallory"}
	_, err = e.ws.GetWorkspace(ctx, other, "shared")
	mustErr(t, err, string(contracts.ErrNotFound))
	_, err = e.ws.ListMembers(ctx, other, "shared")
	mustErr(t, err, string(contracts.ErrNotFound))
}

func TestWorkspacePinAndPurgeGuards(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	alice := "alice"
	w, err := e.ws.Create(ctx, e.scope(alice), "proj")
	mustOK(t, err)
	sc := e.scope(alice)
	p1 := e.uploadSeal(t, alice, "a", "r1", labels.Set{})
	rev1 := e.pub(t, alice, w.Name, "", []File{p1})
	p2 := e.uploadSeal(t, alice, "b", "r2", labels.Set{})
	rev2 := e.pub(t, alice, w.Name, rev1.ID, []File{p1, p2})

	// Head cannot be purged.
	mustErr(t, e.ws.PurgeRevision(ctx, sc, w.Name, rev2.ID), string(contracts.ErrConflict))

	// A pinned non-head revision cannot be purged; unpinned it is blocked by children.
	mustOK(t, e.ws.Pin(ctx, sc, w.Name, rev1.ID))
	mustErr(t, e.ws.PurgeRevision(ctx, sc, w.Name, rev1.ID), string(contracts.ErrConflict))
	mustOK(t, e.ws.Unpin(ctx, sc, w.Name, rev1.ID))
	mustErr(t, e.ws.PurgeRevision(ctx, sc, w.Name, rev1.ID), string(contracts.ErrConflict))

	// A conflict-branch revision is a reachable-but-off-mainline leaf; it can be purged.
	pX := e.uploadSeal(t, alice, "aX", "orphan", labels.Set{})
	e.share(t, alice, w.Name, pX)
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if _, err := e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{pX}); err != nil {
		t.Fatal(err)
	}
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	orphan, err := e.ws.Publish(ctx, sc, cand.ID, rev1.ID)
	mustErr(t, err, string(contracts.ErrConflict)) // stale head -> conflict
	mustOK(t, e.ws.PurgeRevision(ctx, sc, w.Name, orphan.ID))
}

// --- B118: Project Versions — immutable revisions, candidate, conflicts ---

func TestB118ObjectStoreImmutability(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-immutable")
	mustOK(t, err)

	v1 := e.uploadSeal(t, "alice", "b118-a1", "immutable-content-v1", labels.Set{})
	rev1 := e.pub(t, "alice", w.Name, "", []File{
		{Path: "doc.txt", ArtifactID: v1.ArtifactID, Mode: 0o644, Digest: v1.Digest},
	})

	// Verify integrity at the object-store level: reads the actual bytes from
	// MinIO and hashes them, comparing against the revision's recorded digest.
	report, err := e.ws.VerifyRevisionIntegrity(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if !report.OK {
		t.Fatalf("integrity check failed: %+v", report)
	}
	if len(report.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(report.Files))
	}
	if report.Files[0].Expected != report.Files[0].Actual {
		t.Fatalf("digest mismatch: expected %s got %s", report.Files[0].Expected, report.Files[0].Actual)
	}
	t.Logf("B118 immutability: revision %s verified at object-store level (%d files)", rev1.ID, len(report.Files))
}

func TestB118ConflictBranchListing(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-conflict")
	mustOK(t, err)

	v1 := e.uploadSeal(t, "alice", "b118-c1", "base-content", labels.Set{})
	rev1 := e.pub(t, "alice", w.Name, "", []File{
		{Path: "a.txt", ArtifactID: v1.ArtifactID, Mode: 0o644, Digest: v1.Digest},
	})

	// Create a candidate against rev1.
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	v2 := e.uploadSeal(t, "alice", "b118-c2", "conflicting-edit", labels.Set{})
	e.share(t, "alice", w.Name, v2)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{
		{Path: "a.txt", ArtifactID: v2.ArtifactID, Mode: 0o644, Digest: v2.Digest},
	})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))

	// Move head with a different candidate.
	cand2, err := e.ws.CreateCandidate(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	v3 := e.uploadSeal(t, "alice", "b118-c3", "head-mover", labels.Set{})
	e.share(t, "alice", w.Name, v3)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, cand2.ID, []File{
		{Path: "b.txt", ArtifactID: v3.ArtifactID, Mode: 0o644, Digest: v3.Digest},
	})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, cand2.ID, "ci", "passed", ""))
	_, err = e.ws.Publish(ctx, sc, cand2.ID, rev1.ID)
	mustOK(t, err)

	// Now the first candidate is stale: publish must conflict.
	_, err = e.ws.Publish(ctx, sc, cand.ID, rev1.ID)
	mustErr(t, err, string(contracts.ErrConflict))

	// List conflict branches.
	branches, err := e.ws.ListConflictBranches(ctx, sc, w.Name)
	mustOK(t, err)
	if len(branches) == 0 {
		t.Fatal("expected at least one conflict branch")
	}
	if !strings.HasPrefix(branches[0].Name, "conflicts/") {
		t.Fatalf("branch name should start with 'conflicts/', got %q", branches[0].Name)
	}
	t.Logf("B118 conflict: %d branches, first=%s rev=%s seq=%d", len(branches), branches[0].Name, branches[0].RevID, branches[0].Seq)
}

func TestB118CandidateAbandon(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-abandon")
	mustOK(t, err)

	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	v1 := e.uploadSeal(t, "alice", "b118-ab1", "to-be-abandoned", labels.Set{})
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{
		{Path: "x.txt", ArtifactID: v1.ArtifactID, Mode: 0o644, Digest: v1.Digest},
	})
	mustOK(t, err)

	abandoned, err := e.ws.RejectCandidate(ctx, sc, cand.ID)
	mustOK(t, err)
	if abandoned.State != "rejected" {
		t.Fatalf("state = %q, want rejected", abandoned.State)
	}
	// Publishing a rejected candidate must fail.
	_, err = e.ws.Publish(ctx, sc, cand.ID, "")
	mustErr(t, err, string(contracts.ErrConflict))
	t.Log("B118 abandon: candidate rejected, publish correctly refused")
}

func TestB118CrossTenantIsolation(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	w, err := e.ws.Create(ctx, e.scope("alice"), "b118-isolated")
	mustOK(t, err)

	// A different tenant cannot see the workspace.
	otherScope := contracts.Scope{Tenant: "t-other-tenant", Deployment: w.Tenant, Subject: "alice"}
	_, err = e.ws.GetWorkspace(ctx, otherScope, w.Name)
	mustErr(t, err, string(contracts.ErrNotFound))
	t.Log("B118 cross-tenant: correctly rejected with NOT_FOUND")
}

func TestB118LeakGateCeiling(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-leak")
	mustOK(t, err)

	// The leak gate is enforced during Publish via CheckPublication: every
	// artifact must be publishable to the workspace audience. We verify the
	// gate is wired by doing a clean publish (positive case). The rejection
	// path is covered by TestWorkspacePublicationLeakGate.
	v1 := e.uploadSeal(t, "alice", "b118-lk-ok", "clean-data", labels.Set{})
	e.share(t, "alice", w.Name, v1)
	cand, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, cand.ID, []File{
		{Path: "clean.txt", ArtifactID: v1.ArtifactID, Mode: 0o644, Digest: v1.Digest},
	})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, cand.ID, "ci", "passed", ""))
	rev, err := e.ws.Publish(ctx, sc, cand.ID, "")
	mustOK(t, err)
	if rev.Seq != 1 {
		t.Fatalf("seq = %d, want 1", rev.Seq)
	}
	t.Log("B118 leak gate: clean publish succeeds (gate wired and functional)")
}

func TestB118TamperDetection(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-tamper")
	mustOK(t, err)

	original := "immutable-bytes-v1"
	v1 := e.uploadSeal(t, "alice", "b118-tamper-1", original, labels.Set{})
	rev1 := e.pub(t, "alice", w.Name, "", []File{v1})

	// Initial integrity check: must pass.
	report, err := e.ws.VerifyRevisionIntegrity(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if !report.OK {
		t.Fatalf("pre-tamper integrity should pass: %+v", report)
	}

	// TAMPER: delete the exact object version the artifact is bound to.
	// In a versioned bucket, a plain PutObject would create a new version
	// while the old (sealed) version remains — so we must remove the
	// specific version to simulate an object-store tamper.
	var (
		objKey  string
		version string
	)
	if err := e.pool.QueryRow(ctx,
		`SELECT object, version FROM ahp_artifacts.artifacts WHERE id = $1`, v1.ArtifactID).
		Scan(&objKey, &version); err != nil {
		t.Fatalf("lookup object key/version: %v", err)
	}
	if err := e.minio.RemoveObject(ctx, e.bucket, objKey,
		minio.RemoveObjectOptions{VersionID: version}); err != nil {
		t.Fatalf("tamper remove: %v", err)
	}
	t.Logf("tampered: deleted version %s of object %s", version, objKey)

	// Integrity check after tamper: must FAIL.
	report, err = e.ws.VerifyRevisionIntegrity(ctx, sc, w.Name, rev1.ID)
	mustOK(t, err)
	if report.OK {
		t.Fatal("integrity check should FAIL after tamper, but it passed")
	}
	if len(report.Files) != 1 {
		t.Fatalf("expected 1 file in report, got %d", len(report.Files))
	}
	f := report.Files[0]
	if f.Match {
		t.Fatal("file should not match after tamper")
	}
	if f.Expected == f.Actual {
		t.Fatalf("expected != actual after tamper, but both are %q", f.Expected)
	}
	actual := "(object missing)"
	if len(f.Actual) >= 16 {
		actual = f.Actual[:16] + "..."
	}
	t.Logf("B118 tamper detected: expected=%s... actual=%s (mismatch correctly flagged)",
		f.Expected[:16], actual)
}

func TestB118ConcurrentPublishRace(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-race")
	mustOK(t, err)

	// Create two candidates against the same (empty) base.
	candA, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	vA := e.uploadSeal(t, "alice", "b118-race-a", "content-A", labels.Set{})
	e.share(t, "alice", w.Name, vA)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, candA.ID, []File{
		{Path: "a.txt", ArtifactID: vA.ArtifactID, Mode: 0o644, Digest: vA.Digest},
	})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, candA.ID, "ci", "passed", ""))

	candB, err := e.ws.CreateCandidate(ctx, sc, w.Name, "")
	mustOK(t, err)
	vB := e.uploadSeal(t, "alice", "b118-race-b", "content-B", labels.Set{})
	e.share(t, "alice", w.Name, vB)
	_, err = e.ws.UpdateCandidateFiles(ctx, sc, candB.ID, []File{
		{Path: "b.txt", ArtifactID: vB.ArtifactID, Mode: 0o644, Digest: vB.Digest},
	})
	mustOK(t, err)
	mustOK(t, e.ws.SetCheck(ctx, sc, candB.ID, "ci", "passed", ""))

	// Publish both concurrently (sequentially here, but both expect head="").
	// The first should succeed; the second must conflict (head moved).
	type result struct {
		rev Revision
		err error
	}
	ch := make(chan result, 2)
	go func() {
		rev, err := e.ws.Publish(ctx, sc, candA.ID, "")
		ch <- result{rev, err}
	}()
	go func() {
		rev, err := e.ws.Publish(ctx, sc, candB.ID, "")
		ch <- result{rev, err}
	}()

	r1 := <-ch
	r2 := <-ch

	// Exactly one must succeed, the other must conflict.
	succeeded := 0
	conflicted := 0
	for _, r := range []result{r1, r2} {
		if r.err == nil {
			succeeded++
		} else if contracts.Code(r.err) == contracts.ErrConflict {
			conflicted++
		} else {
			t.Fatalf("unexpected error: %v", r.err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("expected 1 success + 1 conflict, got %d + %d", succeeded, conflicted)
	}
	t.Log("B118 concurrent race: exactly one publish succeeded, the other conflicted (CAS correct)")
}

func TestB118MultiFileTamperIdentification(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	sc := e.scope("alice")

	w, err := e.ws.Create(ctx, sc, "b118-multi-tamper")
	mustOK(t, err)

	// Build a revision with 3 files.
	f1 := e.uploadSeal(t, "alice", "b118-multi-1", "content-one", labels.Set{})
	f2 := e.uploadSeal(t, "alice", "b118-multi-2", "content-two", labels.Set{})
	f3 := e.uploadSeal(t, "alice", "b118-multi-3", "content-three", labels.Set{})
	files := []File{
		{Path: "a.txt", ArtifactID: f1.ArtifactID, Mode: 0o644, Digest: f1.Digest},
		{Path: "b.txt", ArtifactID: f2.ArtifactID, Mode: 0o644, Digest: f2.Digest},
		{Path: "c.txt", ArtifactID: f3.ArtifactID, Mode: 0o644, Digest: f3.Digest},
	}
	rev := e.pub(t, "alice", w.Name, "", files)

	// Pre-tamper: all 3 files must match.
	report, err := e.ws.VerifyRevisionIntegrity(ctx, sc, w.Name, rev.ID)
	mustOK(t, err)
	if !report.OK {
		t.Fatalf("pre-tamper integrity should pass: %+v", report)
	}
	if len(report.Files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(report.Files))
	}
	for _, f := range report.Files {
		if !f.Match {
			t.Fatalf("file %s should match pre-tamper", f.Path)
		}
	}

	// Tamper file #2 only: delete its specific MinIO version.
	var objKey, version string
	if err := e.pool.QueryRow(ctx,
		`SELECT object, version FROM ahp_artifacts.artifacts WHERE id = $1`, f2.ArtifactID).
		Scan(&objKey, &version); err != nil {
		t.Fatalf("lookup object: %v", err)
	}
	if err := e.minio.RemoveObject(ctx, e.bucket, objKey,
		minio.RemoveObjectOptions{VersionID: version}); err != nil {
		t.Fatalf("tamper remove: %v", err)
	}

	// Post-tamper: file #2 must mismatch; files #1 and #3 must still match.
	report, err = e.ws.VerifyRevisionIntegrity(ctx, sc, w.Name, rev.ID)
	mustOK(t, err)
	if report.OK {
		t.Fatal("integrity should FAIL after tampering file #2")
	}
	if len(report.Files) != 3 {
		t.Fatalf("expected 3 files in report, got %d", len(report.Files))
	}
	for _, f := range report.Files {
		switch f.Path {
		case "a.txt", "c.txt":
			if !f.Match {
				t.Fatalf("file %s should still match, but got mismatch", f.Path)
			}
		case "b.txt":
			if f.Match {
				t.Fatal("file b.txt should NOT match after tamper")
			}
		default:
			t.Fatalf("unexpected file path %q", f.Path)
		}
	}
	t.Log("B118 multi-file tamper: exactly 1 of 3 files flagged, others intact")
}
