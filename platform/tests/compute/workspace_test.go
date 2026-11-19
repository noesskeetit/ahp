package compute_test

import (
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestWorkspaceCommitHappyPathAndConflict pins the project CoW contract:
// a first commit at head 0 lands on 'main' as revision 1; a commit whose
// expectedHead is stale never overwrites — it opens a conflict-* branch and
// returns WORKSPACE_CONFLICT carrying that branch.
func TestWorkspaceCommitHappyPathAndConflict(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	cmp := mustCreate(t, st, sc, compute.ClassC, compute.ProfileProject, "ws-"+contracts.NewID())

	// Empty head is a zero revision on main.
	head, err := st.WorkspaceHead(c, sc, cmp.ID)
	if err != nil {
		t.Fatalf("WorkspaceHead empty: %v", err)
	}
	if head.Revision != 0 || head.Branch != "main" {
		t.Fatalf("empty head = rev %d branch %q, want 0/main", head.Revision, head.Branch)
	}

	// First commit expectedHead=0 -> main rev 1.
	rev1, err := st.CommitWorkspace(c, sc, cmp.ID, 0, "sha256:aaa")
	if err != nil {
		t.Fatalf("first CommitWorkspace: %v", err)
	}
	if rev1.Revision != 1 || rev1.BaseRev != 0 || rev1.Branch != "main" {
		t.Fatalf("rev1 = %+v, want rev1 base0 main", rev1)
	}

	// A stale expectedHead=0 must NOT overwrite main.
	conflictRev, err := st.CommitWorkspace(c, sc, cmp.ID, 0, "sha256:bbb")
	if contracts.Code(err) != compute.ErrWorkspaceConflict {
		t.Fatalf("stale commit err = %v, want WORKSPACE_CONFLICT", err)
	}
	if conflictRev.Branch == "main" || len(conflictRev.Branch) < len("conflict-") {
		t.Fatalf("conflict branch = %q, want conflict-* branch", conflictRev.Branch)
	}
	// Main head must still be rev 1 (no silent overwrite).
	head, err = st.WorkspaceHead(c, sc, cmp.ID)
	if err != nil {
		t.Fatalf("WorkspaceHead after conflict: %v", err)
	}
	if head.Revision != 1 {
		t.Fatalf("main head moved to %d, want 1 (no overwrite)", head.Revision)
	}

	// A correct commit on top of head 1 succeeds -> main rev 2.
	rev2, err := st.CommitWorkspace(c, sc, cmp.ID, 1, "sha256:ccc")
	if err != nil {
		t.Fatalf("second CommitWorkspace: %v", err)
	}
	if rev2.Revision != 2 || rev2.Branch != "main" {
		t.Fatalf("rev2 = %+v, want rev2 main", rev2)
	}
}

// TestWorkspaceCommitNonProjectForbidden: copy-on-write commits apply only to
// project Computers; ephemeral/personal are Forbidden.
func TestWorkspaceCommitNonProjectForbidden(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	eph := mustCreate(t, st, sc, compute.ClassC, compute.ProfileEphemeral, "")
	if _, err := st.CommitWorkspace(c, sc, eph.ID, 0, "sha256:zzz"); contracts.Code(err) != string(contracts.ErrForbidden) {
		t.Fatalf("ephemeral commit err = %v, want FORBIDDEN", err)
	}
}

// TestEnvironmentIdempotent pins EnsureEnvironment: first call creates applied
// rev 1, replay returns the same applied head without creating a new revision.
func TestEnvironmentIdempotent(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	cmp := mustCreate(t, st, sc, compute.ClassC, compute.ProfileProject, "ws-"+contracts.NewID())

	first, err := st.EnsureEnvironment(c, sc, cmp.ID, "apt:curl")
	if err != nil {
		t.Fatalf("EnsureEnvironment: %v", err)
	}
	if first.Revision != 1 || first.Status != "applied" {
		t.Fatalf("first env = rev %d status %q, want rev1 applied", first.Revision, first.Status)
	}
	replay, err := st.EnsureEnvironment(c, sc, cmp.ID, "apt:curl")
	if err != nil {
		t.Fatalf("EnsureEnvironment replay: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay created a new revision %q != %q", replay.ID, first.ID)
	}
}

// TestEnvironmentInstallRollback pins the exclusive package-install lifecycle:
// a successful install becomes the applied head; a failed install is
// rolled_back and the applied head stays the parent (rollback, no half state).
func TestEnvironmentInstallRollback(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	cmp := mustCreate(t, st, sc, compute.ClassC, compute.ProfileProject, "ws-"+contracts.NewID())

	if _, err := st.EnsureEnvironment(c, sc, cmp.ID, "base"); err != nil {
		t.Fatalf("EnsureEnvironment base: %v", err)
	}

	// Successful install -> applied head rev 2.
	pending, err := st.BeginEnvironmentInstall(c, sc, cmp.ID, "apt:curl", "node-1")
	if err != nil {
		t.Fatalf("BeginEnvironmentInstall: %v", err)
	}
	if pending.Status != "pending" || pending.Revision != 2 {
		t.Fatalf("pending env = rev %d status %q, want rev2 pending", pending.Revision, pending.Status)
	}
	applied, err := st.FinishEnvironmentInstall(c, sc, cmp.ID, pending.ID, "node-1", true, "")
	if err != nil {
		t.Fatalf("FinishEnvironmentInstall ok: %v", err)
	}
	if applied.Status != "applied" {
		t.Fatalf("applied status = %q, want applied", applied.Status)
	}
	head, err := st.CurrentEnvironment(c, sc, cmp.ID)
	if err != nil || head.Revision != 2 {
		t.Fatalf("CurrentEnvironment after ok = rev %d err %v, want rev2", head.Revision, err)
	}

	// Failed install -> rolled_back, head stays parent (rev 2).
	pending2, err := st.BeginEnvironmentInstall(c, sc, cmp.ID, "apt:broken", "node-1")
	if err != nil {
		t.Fatalf("BeginEnvironmentInstall 2: %v", err)
	}
	rolled, err := st.FinishEnvironmentInstall(c, sc, cmp.ID, pending2.ID, "node-1", false, "install failed")
	if err != nil {
		t.Fatalf("FinishEnvironmentInstall fail: %v", err)
	}
	if rolled.Status != "rolled_back" || rolled.ErrorMsg != "install failed" {
		t.Fatalf("rolled env = %+v, want rolled_back with error", rolled)
	}
	head, err = st.CurrentEnvironment(c, sc, cmp.ID)
	if err != nil || head.Revision != 2 {
		t.Fatalf("head after rollback = rev %d err %v, want parent rev2", head.Revision, err)
	}
}

// TestRecordSnapshot pins durable snapshot references (directory and memory
// kinds) and rejects invalid kinds/digests without touching guest bytes.
func TestRecordSnapshot(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	cmp := mustCreate(t, st, sc, compute.ClassC, compute.ProfileProject, "ws-"+contracts.NewID())

	snap, err := st.RecordSnapshot(c, sc, cmp.ID, compute.SnapshotDirectory, "/var/lib/compute/snap/snap-1", "sha256:deadbeef", 4096)
	if err != nil {
		t.Fatalf("RecordSnapshot: %v", err)
	}
	if snap.Kind != compute.SnapshotDirectory || snap.Digest != "sha256:deadbeef" || snap.SizeBytes != 4096 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if _, err := st.RecordSnapshot(c, sc, cmp.ID, compute.SnapshotKind("bogus"), "/x", "sha256:x", 1); err == nil {
		t.Fatal("expected invalid snapshot kind rejection")
	}
	if _, err := st.RecordSnapshot(c, sc, cmp.ID, compute.SnapshotMemory, "/x", "", 1); err == nil {
		t.Fatal("expected empty digest rejection")
	}
}
