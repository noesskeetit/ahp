package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func sha256HexOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// B49: Computer profile personal, VM from the pool, state as revisions.
// ---------------------------------------------------------------------------

func TestB49PersonalComputerStateAsRevisions(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")

	pc := e.mustCreatePersonal(t, alice, "realm-default")
	t.Cleanup(func() {
		_, _ = e.store.SetComputerStatus(context.Background(), alice, pc.ID, compute.ComputerDestroyed)
	})

	// State starts as a revision: main branch seq 0, empty tree.
	head, err := e.store.StateHead(c, alice, pc.ID, "main")
	if err != nil {
		t.Fatalf("StateHead: %v", err)
	}
	if head.Seq != 0 || len(head.Tree) != 0 || head.Branch != "main" {
		t.Fatalf("initial state revision = %+v, want seq 0 empty main", head)
	}

	// A second non-destroyed personal Computer for the same (tenant, subject,
	// realm) is rejected.
	_, err = e.store.CreateComputer(c, alice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: "realm-default",
	}, "key-"+contracts.NewID())
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("second personal computer: code=%v err=%v, want CONFLICT", contracts.Code(err), err)
	}

	// A distinct realm is a distinct trust domain: the policy assigns a
	// separate realm, so a second personal Computer is allowed.
	pc2, err := e.store.CreateComputer(c, alice, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal, Realm: "realm-isolated",
	}, "key-"+contracts.NewID())
	if err != nil {
		t.Fatalf("personal in separate realm: %v", err)
	}
	if pc2.ID == pc.ID {
		t.Fatal("separate realm must yield a distinct Computer")
	}
	t.Cleanup(func() {
		_, _ = e.store.SetComputerStatus(context.Background(), alice, pc2.ID, compute.ComputerDestroyed)
	})

	// VM from the pool: a disposable incarnation is acquired, state survives.
	inc, err := e.runner.Wake(c, alice, pc.ID)
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if inc.Seq != 1 || inc.Status != compute.IncarnationRunning {
		t.Fatalf("incarnation = %+v, want seq 1 running", inc)
	}
}

// ---------------------------------------------------------------------------
// B50: several chats share one Computer: writer lease (real Redis), typed
// patch with hash verification -> derivative revision, derivative branch.
// ---------------------------------------------------------------------------

func TestB50WriterLeaseContentionRealRedis(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	pc := e.mustCreatePersonal(t, alice, "")

	// chat-1 takes the writer lease on the working tree.
	id, err := e.leases.AcquireWriter(c, alice, pc.ID, "chat-1", 5*time.Minute)
	if err != nil || id == "" {
		t.Fatalf("chat-1 AcquireWriter: id=%q err=%v", id, err)
	}

	// The lease is a real key in the lane's Redis namespace.
	val, err := e.redis.Get(c, "vm2cmp:writer:"+pc.ID)
	if err != nil {
		t.Fatalf("redis GET writer key: %v", err)
	}
	if val != "chat-1" {
		t.Fatalf("redis writer key = %q, want chat-1", val)
	}

	// chat-2 is refused while chat-1 holds it.
	_, err = e.leases.AcquireWriter(c, alice, pc.ID, "chat-2", 5*time.Minute)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("chat-2 AcquireWriter: code=%v err=%v, want CONFLICT", contracts.Code(err), err)
	}

	// Same holder re-acquires: extends, no contention.
	if _, err := e.leases.AcquireWriter(c, alice, pc.ID, "chat-1", 5*time.Minute); err != nil {
		t.Fatalf("chat-1 re-acquire: %v", err)
	}

	// Release frees the tree for chat-2.
	if err := e.leases.ReleaseWriter(c, alice, pc.ID, "chat-1"); err != nil {
		t.Fatalf("ReleaseWriter: %v", err)
	}
	if _, err := e.leases.AcquireWriter(c, alice, pc.ID, "chat-2", 5*time.Minute); err != nil {
		t.Fatalf("chat-2 AcquireWriter after release: %v", err)
	}
	if err := e.leases.ReleaseWriter(c, alice, pc.ID, "chat-2"); err != nil {
		t.Fatalf("ReleaseWriter chat-2: %v", err)
	}

	// A stale TTL expires: after the lease lapses, the tree is free again
	// without an explicit release.
	if _, err := e.leases.AcquireWriter(c, alice, pc.ID, "chat-1", 2*time.Second); err != nil {
		t.Fatalf("chat-1 short lease: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	if _, err := e.leases.AcquireWriter(c, alice, pc.ID, "chat-2", 5*time.Minute); err != nil {
		t.Fatalf("chat-2 after TTL expiry: %v", err)
	}
	_ = e.leases.ReleaseWriter(c, alice, pc.ID, "chat-2")
}

func TestB50TypedPatchDerivativeRevision(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	pc := e.mustCreatePersonal(t, alice, "")
	_, _ = e.runner.Wake(c, alice, pc.ID)

	// A command writes a file into the shared working tree.
	e.mustRun(t, alice, pc.ID, "printf 'v1' > note.txt")
	wantHash := sha256HexOf("v1")

	// A typed patch with the wrong expected hash is refused (STALE_HASH) and
	// the file is untouched.
	_, err := e.runner.TypedPatch(c, alice, pc.ID, "main", "note.txt",
		sha256HexOf("stale"), []byte("v2"), 0o600)
	if contracts.Code(err) != compute.ErrStaleHash {
		t.Fatalf("stale typed patch: code=%v err=%v, want STALE_HASH", contracts.Code(err), err)
	}
	if b, err := os.ReadFile(filepath.Join(e.home(pc.ID), "note.txt")); err != nil || string(b) != "v1" {
		t.Fatalf("file changed despite stale patch: %q err=%v", b, err)
	}

	// The correct hash verifies and the patch appends a derivative revision.
	rev, err := e.runner.TypedPatch(c, alice, pc.ID, "main", "note.txt",
		wantHash, []byte("v2-patched"), 0o600)
	if err != nil {
		t.Fatalf("typed patch: %v", err)
	}
	if rev.Seq != 1 || rev.ParentID == "" || rev.Branch != "main" {
		t.Fatalf("derivative revision = %+v, want seq 1 on main with parent", rev)
	}
	got, ok := rev.Tree["/note.txt"]
	if !ok || got.Hash != sha256HexOf("v2-patched") {
		t.Fatalf("revision tree missing patched file: %+v", rev.Tree)
	}
	if b, err := os.ReadFile(filepath.Join(e.home(pc.ID), "note.txt")); err != nil || string(b) != "v2-patched" {
		t.Fatalf("live file not patched: %q err=%v", b, err)
	}

	// The old hash is now stale even against the durable tree.
	_, err = e.runner.TypedPatch(c, alice, pc.ID, "main", "note.txt",
		wantHash, []byte("v3"), 0o600)
	if contracts.Code(err) != compute.ErrStaleHash {
		t.Fatalf("second stale patch: code=%v, want STALE_HASH", contracts.Code(err))
	}
}

func TestB50DerivativeBranchParallelWork(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	pc := e.mustCreatePersonal(t, alice, "")
	_, _ = e.runner.Wake(c, alice, pc.ID)
	// A command writes a file into the shared working tree, and a typed
	// patch (hash verified) records it in the state revisions.
	e.mustRun(t, alice, pc.ID, "printf 'base' > shared.txt")
	if _, err := e.runner.TypedPatch(c, alice, pc.ID, "main", "shared.txt",
		sha256HexOf("base"), []byte("base"), 0o600); err != nil {
		t.Fatalf("record base file: %v", err)
	}

	// Chat A works on a derivative branch forked from main.
	fork, err := e.runner.DeriveBranch(c, alice, pc.ID, "chat-a")
	if err != nil {
		t.Fatalf("DeriveBranch: %v", err)
	}
	if fork.Seq != 0 || fork.Branch != "chat-a" || fork.ParentID == "" {
		t.Fatalf("fork = %+v, want seq 0 on chat-a with parent", fork)
	}
	if _, err := os.Stat(filepath.Join(e.runner.BranchDirForTest(pc.ID, "chat-a"), "shared.txt")); err != nil {
		t.Fatalf("branch worktree not materialised: %v", err)
	}

	// The branch sees the main file and can patch it by hash.
	head, err := e.store.StateHead(c, alice, pc.ID, "chat-a")
	if err != nil {
		t.Fatalf("StateHead chat-a: %v", err)
	}
	if got := head.Tree["/shared.txt"]; got.Hash != sha256HexOf("base") {
		t.Fatalf("branch tree = %+v, want /shared.txt base hash", head.Tree)
	}
	branchRev, err := e.runner.TypedPatch(c, alice, pc.ID, "chat-a", "shared.txt",
		sha256HexOf("base"), []byte("from-branch"), 0o600)
	if err != nil {
		t.Fatalf("branch typed patch: %v", err)
	}
	if branchRev.Seq != 1 || branchRev.Branch != "chat-a" {
		t.Fatalf("branch patch revision = %+v", branchRev)
	}

	// Main is untouched by branch work — in the revisions and on disk.
	mainHead, err := e.store.StateHead(c, alice, pc.ID, "main")
	if err != nil {
		t.Fatal(err)
	}
	if mainHead.Seq != fork.BaseSeq {
		t.Fatalf("main moved despite branch work: seq=%d, want %d", mainHead.Seq, fork.BaseSeq)
	}
	if b, err := os.ReadFile(filepath.Join(e.home(pc.ID), "shared.txt")); err != nil || string(b) != "base" {
		t.Fatalf("main working tree changed by branch work: %q err=%v", b, err)
	}
	if b, err := os.ReadFile(filepath.Join(e.runner.BranchDirForTest(pc.ID, "chat-a"), "shared.txt")); err != nil || string(b) != "from-branch" {
		t.Fatalf("branch working tree not patched: %q err=%v", b, err)
	}

	// Clean merge: main did not change shared.txt since the fork.
	merged, err := e.runner.MergeBranch(c, alice, pc.ID, "chat-a")
	if err != nil {
		t.Fatalf("MergeBranch: %v", err)
	}
	if merged.Branch != "main" || merged.Seq != fork.BaseSeq+1 {
		t.Fatalf("merged revision = %+v, want main seq %d", merged, fork.BaseSeq+1)
	}
	if got := merged.Tree["/shared.txt"]; got.Hash != sha256HexOf("from-branch") {
		t.Fatalf("merged tree = %+v", merged.Tree)
	}
	if b, err := os.ReadFile(filepath.Join(e.home(pc.ID), "shared.txt")); err != nil || string(b) != "from-branch" {
		t.Fatalf("main working tree not merged: %q err=%v", b, err)
	}
	if _, err := os.Stat(filepath.Join(e.runner.BranchDirForTest(pc.ID, "chat-a"))); !os.IsNotExist(err) {
		t.Fatalf("merged branch worktree still present (err=%v)", err)
	}

	// Overlapping change is a WORKSPACE_CONFLICT, never a silent overwrite.
	fork2, err := e.runner.DeriveBranch(c, alice, pc.ID, "chat-b")
	if err != nil {
		t.Fatalf("DeriveBranch chat-b: %v", err)
	}
	if fork2.ParentID != merged.ID {
		t.Fatalf("chat-b forked from %s, want merged head %s", fork2.ParentID, merged.ID)
	}
	if _, err := e.runner.TypedPatch(c, alice, pc.ID, "chat-b", "shared.txt",
		sha256HexOf("from-branch"), []byte("branch-b"), 0o600); err != nil {
		t.Fatalf("chat-b patch: %v", err)
	}
	// Main changes the same path on another branch.
	if _, err := e.runner.DeriveBranch(c, alice, pc.ID, "chat-c"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.runner.TypedPatch(c, alice, pc.ID, "main", "shared.txt",
		sha256HexOf("from-branch"), []byte("main-side"), 0o600); err != nil {
		t.Fatalf("main-side patch: %v", err)
	}
	_, err = e.runner.MergeBranch(c, alice, pc.ID, "chat-b")
	if contracts.Code(err) != compute.ErrWorkspaceConflict {
		t.Fatalf("overlapping merge: code=%v err=%v, want WORKSPACE_CONFLICT", contracts.Code(err), err)
	}
}

// ---------------------------------------------------------------------------
// B51: package install is an exclusive environment operation with REAL
// rollback on a throwaway prefix inside the workdir.
// ---------------------------------------------------------------------------

func TestB51PackageInstallRollback(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	pc := e.mustCreatePersonal(t, alice, "")
	_, _ = e.runner.Wake(c, alice, pc.ID)

	// Base environment (rev 1, applied).
	base, err := e.store.EnsureEnvironment(c, alice, pc.ID, "base-image")
	if err != nil || base.Revision != 1 || base.Status != "applied" {
		t.Fatalf("EnsureEnvironment = %+v err=%v", base, err)
	}

	// A first successful install materialises rev 2 with a real file.
	rev2, dir2, err := e.runner.BeginEnvironmentInstall(c, alice, pc.ID, "base+pkg-a", "installer-1")
	if err != nil {
		t.Fatalf("BeginEnvironmentInstall: %v", err)
	}
	if rev2.Revision != 2 || rev2.Status != "pending" || rev2.ParentID != base.ID {
		t.Fatalf("pending revision = %+v", rev2)
	}
	if _, err := os.Stat(dir2); err != nil {
		t.Fatalf("throwaway prefix %s missing: %v", dir2, err)
	}
	code, _, stderr, err := e.runner.RunEnvironmentInstall(c, alice, pc.ID, rev2.ID,
		"printf 'pkg-a-v1' > pkg-a.txt && echo installed")
	if err != nil || code != 0 {
		t.Fatalf("RunEnvironmentInstall success path: code=%d stderr=%q err=%v", code, stderr, err)
	}
	settled, err := e.runner.FinishEnvironmentInstall(c, alice, pc.ID, rev2.ID, "installer-1", true, "")
	if err != nil || settled.Status != "applied" {
		t.Fatalf("FinishEnvironmentInstall(success) = %+v err=%v", settled, err)
	}
	cur, files, err := e.runner.ListEnvironment(c, alice, pc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 2 || len(files) != 1 || files[0].Path != "pkg-a.txt" ||
		files[0].Digest != sha256HexOf("pkg-a-v1") {
		t.Fatalf("applied environment = rev %d files=%+v, want rev 2 with pkg-a.txt", cur, files)
	}

	// A failed install (rev 3) is rolled back for real: the throwaway prefix
	// is deleted and the applied environment is byte-for-byte rev 2.
	rev3, dir3, err := e.runner.BeginEnvironmentInstall(c, alice, pc.ID, "base+pkg-a+pkg-b", "installer-2")
	if err != nil {
		t.Fatalf("BeginEnvironmentInstall rev3: %v", err)
	}
	if _, err := os.Stat(dir3); err != nil {
		t.Fatalf("throwaway prefix rev3 %s missing: %v", dir3, err)
	}
	// The install corrupts the prefix and then fails.
	code, _, stderr, err = e.runner.RunEnvironmentInstall(c, alice, pc.ID, rev3.ID,
		"printf 'pkg-b-v2' > pkg-b.txt && rm pkg-a.txt && echo boom >&2 && exit 3")
	if err != nil || code != 3 {
		t.Fatalf("RunEnvironmentInstall failure path: code=%d stderr=%q err=%v", code, stderr, err)
	}
	settled, err = e.runner.FinishEnvironmentInstall(c, alice, pc.ID, rev3.ID, "installer-2", false, "install failed: exit 3")
	if err != nil || settled.Status != "rolled_back" {
		t.Fatalf("FinishEnvironmentInstall(failure) = %+v err=%v", settled, err)
	}
	if _, err := os.Stat(dir3); !os.IsNotExist(err) {
		t.Fatalf("throwaway prefix rev3 still present after rollback (err=%v)", err)
	}
	if b, err := os.ReadFile(filepath.Join(dir2, "pkg-a.txt")); err != nil || string(b) != "pkg-a-v1" {
		t.Fatalf("applied environment damaged by rollback: %q err=%v", b, err)
	}
	cur, files, err = e.runner.ListEnvironment(c, alice, pc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cur != 2 || len(files) != 1 || files[0].Digest != sha256HexOf("pkg-a-v1") {
		t.Fatalf("environment after rollback = rev %d files=%+v, want unchanged rev 2", cur, files)
	}

	// The failed revision is durably recorded with its error and parent.
	failed, err := e.store.EnvRevisionByID(c, alice, pc.ID, rev3.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != "rolled_back" || failed.ParentID != rev2.ID || failed.ErrorMsg == "" {
		t.Fatalf("failed revision record = %+v", failed)
	}

	// Exclusivity: a concurrent environment operation is refused while the
	// writer lease is held.
	rev4, _, err := e.runner.BeginEnvironmentInstall(c, alice, pc.ID, "base+pkg-a+pkg-c", "installer-3")
	if err != nil {
		t.Fatalf("BeginEnvironmentInstall rev4: %v", err)
	}
	_, _, err = e.runner.BeginEnvironmentInstall(c, alice, pc.ID, "base+pkg-a+pkg-d", "installer-4")
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("concurrent install: code=%v err=%v, want CONFLICT", contracts.Code(err), err)
	}
	_, _ = e.runner.FinishEnvironmentInstall(c, alice, pc.ID, rev4.ID, "installer-3", false, "cleaned up for test")
}

// ---------------------------------------------------------------------------
// B52: sleep — activity leases gate it, cost is accounted, and processes do
// NOT survive sleep (pids verified dead).
// ---------------------------------------------------------------------------

func TestB52SleepLeaseGateCostAndProcessDeath(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	pc := e.mustCreatePersonal(t, alice, "")

	// Wake: incarnation from the pool, cost window opens.
	inc, err := e.runner.Wake(c, alice, pc.ID)
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if inc.Seq != 1 {
		t.Fatalf("incarnation seq = %d, want 1", inc.Seq)
	}

	// A background process is started in the Computer; it outlives the
	// launching command (orphaned, same process group).
	rec := e.mustRun(t, alice, pc.ID, "sh -c 'sleep 120 > /dev/null 2>&1 & echo $! > bg.pid; cat bg.pid'")
	bgPID, err := parsePID(rec.Stdout)
	if err != nil {
		t.Fatalf("parse bg pid: %v (stdout=%q)", err, rec.Stdout)
	}
	if err := assertProcessAlive(bgPID); err != nil {
		t.Fatalf("background process not alive: %v", err)
	}
	t.Logf("background process %d alive before sleep", bgPID)

	// An activity lease (a background command) holds the Computer awake:
	// sleep must be refused while it is held.
	if _, err := e.leases.AcquireActivity(c, alice, pc.ID, compute.LeaseCommand, "bg-task", 5*time.Minute); err != nil {
		t.Fatalf("AcquireActivity: %v", err)
	}
	_, err = e.runner.Sleep(c, alice, pc.ID)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("Sleep with held lease: code=%v err=%v, want CONFLICT", contracts.Code(err), err)
	}

	// Release the lease: sleep proceeds, kills the process tree, verifies
	// death, suspends the Computer and closes the cost window.
	if err := e.leases.ReleaseActivity(c, alice, pc.ID, compute.LeaseCommand, "bg-task"); err != nil {
		t.Fatalf("ReleaseActivity: %v", err)
	}
	rep, err := e.runner.Sleep(c, alice, pc.ID)
	if err != nil {
		t.Fatalf("Sleep: %v", err)
	}
	if rep.Status != compute.ComputerSuspended {
		t.Fatalf("sleep status = %s, want suspended", rep.Status)
	}
	if len(rep.Processes.Processes) == 0 {
		t.Fatal("sleep report carries no process evidence")
	}
	for _, p := range rep.Processes.Processes {
		if !p.Dead {
			t.Fatalf("process %d not verified dead by sleep", p.PID)
		}
	}
	// Independent proof from the test process: the orphaned background pid is
	// gone from the kernel.
	if err := assertProcessDead(bgPID); err != nil {
		t.Fatalf("background process %d survived sleep: %v", bgPID, err)
	}
	t.Logf("background process %d verified dead after sleep", bgPID)

	// Cost accounting: the awake window was closed and counted.
	cost, err := e.store.CostReport(c, alice, pc.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if cost.AwakeNanos <= 0 || cost.Open {
		t.Fatalf("cost report = %+v, want closed positive window", cost)
	}
	t.Logf("awake cost accounted: %s", cost.AwakeHuman)

	// Wake again: a fresh incarnation from the pool (each incarnation is
	// disposable — the command's incarnation was already consumed), state
	// durable across the churn.
	inc2, err := e.runner.Wake(c, alice, pc.ID)
	if err != nil {
		t.Fatalf("second Wake: %v", err)
	}
	if inc2.Seq <= inc.Seq {
		t.Fatalf("second incarnation seq = %d, want > %d (fresh from pool)", inc2.Seq, inc.Seq)
	}
	st, err := e.store.GetComputer(c, alice, pc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != compute.ComputerActive || st.Incarnation != inc2.Seq {
		t.Fatalf("computer after wake = %+v, want active incarnation %d", st, inc2.Seq)
	}
}

func parsePID(s string) (int, error) {
	s = strings.TrimSpace(s)
	var pid int
	if _, err := fmt.Sscanf(s, "%d", &pid); err != nil {
		return 0, err
	}
	return pid, nil
}

func assertProcessAlive(pid int) error {
	return syscall.Kill(pid, 0)
}

func assertProcessDead(pid int) error {
	err := syscall.Kill(pid, 0)
	if err == nil {
		return fmt.Errorf("kill(pid,0) succeeded: process %d still alive", pid)
	}
	if err == syscall.ESRCH {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// B53: trust boundaries — realm, no employee mixing, custom agents refused.
// ---------------------------------------------------------------------------

func TestB53TrustBoundaries(t *testing.T) {
	e := newLiveEnv(t)
	c := ctx(t)
	alice := scope("alice")
	bob := scope("bob")

	// Custom agents (class X) and developer containers (class C) never get a
	// personal Computer.
	for _, class := range []compute.Class{compute.ClassX, compute.ClassC} {
		_, err := e.store.CreateComputer(c, alice, compute.NewComputerRequest{
			Class: class, Profile: compute.ProfilePersonal,
		}, "key-"+contracts.NewID())
		if contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("class %s personal: code=%v err=%v, want FORBIDDEN", class, contracts.Code(err), err)
		}
	}

	// Employee mixing: bob cannot see, let alone run on, alice's Computer.
	pc := e.mustCreatePersonal(t, alice, "")
	if _, err := e.store.GetComputer(c, bob, pc.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("bob GetComputer(alice): code=%v, want NOT_FOUND", contracts.Code(err))
	}
	_, err := e.runner.RunCommand(c, bob, pc.ID, "run-x", "id", "")
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("bob RunCommand on alice's computer: code=%v, want NOT_FOUND", contracts.Code(err))
	}

	// Alice's own Computer works for alice.
	if _, err := e.store.GetComputer(c, alice, pc.ID); err != nil {
		t.Fatalf("alice GetComputer(own): %v", err)
	}

	// A cross-realm access from a different deployment of the same tenant is
	// also not found (deployment is part of the isolation scope).
	otherDeploy := scope("carol")
	otherDeploy.Deployment = alice.Deployment + "-2"
	if _, err := e.store.GetComputer(c, otherDeploy, pc.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("other deployment GetComputer: code=%v, want NOT_FOUND", contracts.Code(err))
	}
}

// ---------------------------------------------------------------------------
// Store-level HTTP assertions over real mTLS + task tokens (the real-binary
// smoke test covers 127.0.0.1:54203 separately).
// ---------------------------------------------------------------------------

func TestLiveHTTPComputeSurface(t *testing.T) {
	h := newLiveHTTPEnv(t)
	tokAlice := signTask(t, h.signer, h.tenant, h.deployment, "alice", "task-alice")
	tokBob := signTask(t, h.signer, h.tenant, h.deployment, "bob", "task-bob")

	// Personal Computer creation for alice.
	st, body := h.call(t, "alice", "CreateComputer", tokAlice,
		`{"class":"S","profile":"personal","realm":"r1","key":"create-1"}`)
	mustStatus(t, st, 200, body)
	var pc struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &pc); err != nil || pc.ID == "" {
		t.Fatalf("CreateComputer body: %s (err=%v)", body, err)
	}

	// Custom agent (class X) is refused.
	st, body = h.call(t, "alice", "CreateComputer", tokAlice,
		`{"class":"X","profile":"personal","key":"create-x"}`)
	mustStatus(t, st, 403, body)

	// Bob cannot read alice's Computer.
	st, body = h.call(t, "bob", "GetComputer", tokBob, `{"computer_id":"`+pc.ID+`"}`)
	mustStatus(t, st, 404, body)

	// Writer lease contention over the wire.
	st, body = h.call(t, "alice", "TakeWriterLease", tokAlice,
		`{"computer_id":"`+pc.ID+`","holder":"chat-1","ttl_ms":300000}`)
	mustStatus(t, st, 200, body)
	st, body = h.call(t, "alice", "TakeWriterLease", tokAlice,
		`{"computer_id":"`+pc.ID+`","holder":"chat-2","ttl_ms":300000}`)
	mustStatus(t, st, 409, body)

	// Sleep is gated by the same lease, and the refusal is a CONTRACT answer:
	// 409 CONFLICT with the CONFLICT code, while the lease is still held by
	// chat-1. A Computer that cannot sleep because somebody is writing from it
	// is a conflict, not a server that broke, and on a host where the process
	// kill is unsupported it must still not arrive as 500 INTERNAL.
	st, body = h.call(t, "alice", "Sleep", tokAlice, `{"computer_id":"`+pc.ID+`"}`)
	mustStatus(t, st, 409, body)
	if !strings.Contains(body, "CONFLICT") {
		t.Fatalf("Sleep with a held writer lease: body=%s, want code CONFLICT", body)
	}
	st, body = h.call(t, "alice", "ReleaseWriterLease", tokAlice,
		`{"computer_id":"`+pc.ID+`","holder":"chat-1"}`)
	mustStatus(t, st, 200, body)

	// With the lease released the same call succeeds: the Computer suspends and
	// the answer names the state, on this host too.
	st, body = h.call(t, "alice", "Sleep", tokAlice, `{"computer_id":"`+pc.ID+`"}`)
	mustStatus(t, st, 200, body)
	var rep struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal([]byte(body), &rep); err != nil || rep.Status != "suspended" {
		t.Fatalf("Sleep body: %s (err=%v)", body, err)
	}
}
