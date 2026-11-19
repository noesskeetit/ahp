package compute

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// LeaseService exposes the Redis-backed lease layer (nil when not wired).
func (r *Runner) LeaseService() *LeaseService { return r.leases }

// WithLeaseService attaches the Redis-backed lease layer. Without it the
// lease-gated operations (typed patch, sleep) fail closed with Unavailable.
func (r *Runner) WithLeaseService(l *LeaseService) *Runner {
	r.leases = l
	return r
}

// WithEnvironmentManager overrides the environment materialiser (tests inject
// a manager rooted in a throwaway prefix inside the workdir).
func (r *Runner) WithEnvironmentManager(m *EnvironmentManager) *Runner {
	r.envMgr = m
	return r
}

func (r *Runner) envManager() *EnvironmentManager {
	if r.envMgr != nil {
		return r.envMgr
	}
	return NewEnvironmentManager(r.jailBase)
}

// HomeDir is the durable per-Computer working tree on this node.
func (r *Runner) HomeDir(computerID string) string {
	return filepath.Join(r.jailBase, "computers", computerID, "home")
}

const typedPatchLeaseHolder = "typed-patch"

// TypedPatch applies one typed patch to a personal Computer's working tree:
// the file's live hash is verified (against both the durable state tree and,
// when the Computer's home is mounted, the file on disk), the new content is
// written, and a derivative state revision is appended. A mismatch is refused
// with STALE_HASH and nothing changes. The operation holds the exclusive
// writer lease for its duration, per the Computer model.
func (r *Runner) TypedPatch(ctx context.Context, scope contracts.Scope, computerID, branch, path, expectedHash string, content []byte, mode int) (StateRevision, error) {
	if r.leases == nil {
		return StateRevision{}, Unavailable("redis", "lease service is not configured")
	}
	c, err := r.store.GetComputer(ctx, scope, computerID)
	if err != nil {
		return StateRevision{}, err
	}
	if c.Profile != ProfilePersonal {
		return StateRevision{}, contracts.NewError(contracts.ErrForbidden,
			"typed patches apply only to personal computers")
	}
	if len(content) > 1<<20 {
		return StateRevision{}, invalid("typed patch content exceeds 1 MiB")
	}
	if mode == 0 {
		mode = 0o600
	}
	hash := sha256Hex(content)

	// Verification is a live operation: the working tree must be mounted on
	// this node, otherwise the hash cannot be checked and the patch is
	// refused (fail closed, no unverified revision). A derivative branch has
	// its own worktree, so parallel chats never verify against each other's
	// files.
	home := r.HomeDir(computerID)
	if branch != "" && branch != "main" {
		home = r.branchDir(computerID, branch)
	}
	if st, err := os.Stat(home); err != nil || !st.IsDir() {
		return StateRevision{}, Unavailable("host",
			"the working tree is not mounted on this node (wake the computer, or the branch was never materialised)")
	}

	if _, err := r.leases.AcquireWriter(ctx, scope, computerID, typedPatchLeaseHolder, 2*time.Minute); err != nil {
		return StateRevision{}, err
	}
	defer func() {
		_ = r.leases.ReleaseWriter(context.WithoutCancel(ctx), scope, computerID, typedPatchLeaseHolder)
	}()

	head, err := r.store.StateHead(ctx, scope, computerID, branch)
	if err != nil {
		return StateRevision{}, err
	}
	// Durable tree check: a stale or conflicting revision is refused; a path
	// the tree has not recorded yet (written by a command) passes to the
	// live hash check, which is authoritative.
	if err := checkExpectedHash(head.Tree, path, expectedHash); err != nil {
		return StateRevision{}, err
	}
	if err := applyTypedPatch(home, path, expectedHash, content, hash, mode); err != nil {
		return StateRevision{}, err
	}

	rev, err := r.store.CommitStatePatch(ctx, scope, computerID, branch, head.ID, path,
		StateFile{Hash: hash, Size: int64(len(content)), Mode: mode})
	if err != nil {
		// The durable CAS failed (head moved under us): undo the disk write
		// we may have performed so disk and state never diverge.
		if st, err := os.Stat(home); err == nil && st.IsDir() {
			_ = os.Remove(filepath.Join(home, filepath.FromSlash(path)))
		}
		return StateRevision{}, err
	}
	return rev, nil
}

// checkExpectedHash enforces the typed-patch conflict rule against the
// durable tree: a recorded path must not be stale (hash mismatch) and a
// create-patch must not collide with a recorded path. Paths the tree has not
// recorded yet are verified against the live file instead.
func checkExpectedHash(tree StateTree, path, expectedHash string) error {
	if !hasLeadingSlash(path) {
		path = "/" + path
	}
	f, exists := tree[path]
	if !exists {
		return nil
	}
	if expectedHash == "" || f.Hash != expectedHash {
		return contracts.NewError(ErrStaleHash,
			fmt.Sprintf("file %s recorded with hash %s, expected %s", path, f.Hash, expectedHash))
	}
	return nil
}

func hasLeadingSlash(p string) bool {
	return len(p) > 0 && p[0] == '/'
}

// applyTypedPatch verifies the live file's hash, writes the new content and
// re-hashes it, so a torn write can never pass.
func applyTypedPatch(home, path, expectedHash string, content []byte, hash string, mode int) error {
	full := filepath.Join(home, filepath.FromSlash(path))
	if dir := filepath.Dir(full); dir != home {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("compute: create patch dir: %w", err)
		}
	}
	st, err := os.Stat(full)
	switch {
	case err == nil:
		if expectedHash == "" {
			return contracts.NewError(ErrStaleHash,
				fmt.Sprintf("file %s already exists on disk", path))
		}
		d, herr := fileSHA256(full)
		if herr != nil {
			return fmt.Errorf("compute: hash live file: %w", herr)
		}
		if d != expectedHash {
			return contracts.NewError(ErrStaleHash,
				fmt.Sprintf("live file %s hash is %s, expected %s", path, d, expectedHash))
		}
	case os.IsNotExist(err):
		if expectedHash != "" {
			return contracts.NewError(ErrStaleHash,
				fmt.Sprintf("live file %s is absent, expected hash %s", path, expectedHash))
		}
	default:
		return fmt.Errorf("compute: stat live file: %w", err)
	}
	if err := os.WriteFile(full, content, os.FileMode(mode)); err != nil {
		return fmt.Errorf("compute: write patched file: %w", err)
	}
	if d, err := fileSHA256(full); err != nil || d != hash {
		return fmt.Errorf("compute: patched file %s does not verify after write", path)
	}
	_ = st
	return nil
}

// Sleep puts a Computer to sleep: the Redis lease gate is consulted, the
// process tree is killed and verified dead, the durable status moves to
// suspended and the open cost window is closed. Processes never survive
// sleep (process snapshots are a separate PoC stage, out of scope).
func (r *Runner) Sleep(ctx context.Context, scope contracts.Scope, computerID string) (SleepReport, error) {
	if r.leases == nil {
		return SleepReport{}, Unavailable("redis", "lease service is not configured")
	}
	leasesHeld, err := r.leases.HoldingCount(ctx, scope, computerID)
	if err != nil {
		return SleepReport{}, err
	}
	// Gate before side effects: a refused sleep must not touch the process
	// tree.
	if leasesHeld > 0 {
		return SleepReport{}, contracts.NewError(contracts.ErrConflict,
			fmt.Sprintf("computer is awake: %d lease(s) held", leasesHeld))
	}
	var processes ProcessKillReport
	if r.jailBase != "" {
		processes, err = KillComputerProcesses(r.jailBase, computerID)
		if err != nil {
			return SleepReport{}, fmt.Errorf("compute: kill processes: %w", err)
		}
	}
	return r.store.SleepComputer(ctx, scope, computerID, leasesHeld, processes)
}

// Wake brings a sleeping (or fresh) Computer back: an incarnation is acquired
// from the pool and the cost window opens.
func (r *Runner) Wake(ctx context.Context, scope contracts.Scope, computerID string) (Incarnation, error) {
	computer, err := r.store.GetComputer(ctx, scope, computerID)
	if err != nil {
		return Incarnation{}, err
	}
	backend, err := r.registry.Require(ctx, computer.Class)
	if err != nil {
		return Incarnation{}, err
	}
	inc, err := r.store.AcquireIncarnation(ctx, scope, computerID, backend.Name(), r.node, "")
	if err != nil {
		return Incarnation{}, err
	}
	if err := r.store.OpenCostWindow(ctx, scope, computerID); err != nil {
		_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
		return Incarnation{}, err
	}
	return inc, nil
}

// BeginEnvironmentInstall opens an exclusive environment operation: the
// durable pending revision, the Redis writer lease and a real throwaway
// prefix materialised from the applied environment.
func (r *Runner) BeginEnvironmentInstall(ctx context.Context, scope contracts.Scope, computerID, spec, holder string) (EnvRevision, string, error) {
	if r.leases == nil {
		return EnvRevision{}, "", Unavailable("redis", "lease service is not configured")
	}
	rev, err := r.store.BeginEnvironmentInstall(ctx, scope, computerID, spec, holder)
	if err != nil {
		return EnvRevision{}, "", err
	}
	if _, err := r.leases.AcquireWriter(ctx, scope, computerID, holder, 30*time.Minute); err != nil {
		_, _ = r.store.FinishEnvironmentInstall(ctx, scope, computerID, rev.ID, holder, false, "redis writer lease: "+err.Error())
		return EnvRevision{}, "", err
	}
	parent := int64(0)
	if rev.Revision > 1 {
		parent = rev.Revision - 1
	}
	em := r.envManager()
	if err := em.Materialise(computerID, rev.Revision, parent); err != nil {
		_, _ = r.store.FinishEnvironmentInstall(ctx, scope, computerID, rev.ID, holder, false, "materialise: "+err.Error())
		_ = r.leases.ReleaseWriter(context.WithoutCancel(ctx), scope, computerID, holder)
		return EnvRevision{}, "", err
	}
	return rev, em.prefixDir(computerID, rev.Revision), nil
}

// RunEnvironmentInstall executes the install command as a real process inside
// the throwaway prefix. A non-zero exit is the failed install the rollback
// then undoes; the revision stays pending until FinishEnvironmentInstall.
func (r *Runner) RunEnvironmentInstall(ctx context.Context, scope contracts.Scope, computerID, revisionID, command string) (int, string, string, error) {
	if err := r.store.inTx(ctx, func(tx pgx.Tx) error { return r.store.checkOwnerWritable(ctx, tx, scope) }); err != nil {
		return 0, "", "", err
	}
	rev, err := r.store.EnvRevisionByID(ctx, scope, computerID, revisionID)
	if err != nil {
		return 0, "", "", err
	}
	if rev.Status != "pending" {
		return 0, "", "", contracts.NewError(contracts.ErrConflict, "environment revision is not pending")
	}
	execCtx, stopWatching := r.watchOwner(ctx, scope)
	defer stopWatching()
	return r.envManager().RunInPrefix(execCtx, computerID, rev.Revision, command, 60)
}

// FinishEnvironmentInstall settles the operation and performs the real
// filesystem outcome: success moves the applied cursor to the new prefix,
// failure deletes the throwaway prefix (the applied environment is byte-for-
// byte unchanged) and the revision is recorded rolled_back.
func (r *Runner) FinishEnvironmentInstall(ctx context.Context, scope contracts.Scope, computerID, revisionID, holder string, ok bool, errMsg string) (EnvRevision, error) {
	if r.leases == nil {
		return EnvRevision{}, Unavailable("redis", "lease service is not configured")
	}
	settlement, err := r.store.pool.Begin(ctx)
	if err != nil {
		return EnvRevision{}, err
	}
	defer settlement.Rollback(context.WithoutCancel(ctx))
	// Settlement remains legal after fencing, but must finish the filesystem
	// outcome before tenant cleanup can observe its terminal metadata.
	if _, err = settlement.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, "ahp_compute/tenant-erasure/"+scope.Tenant); err != nil {
		return EnvRevision{}, err
	}
	rev, err := r.store.FinishEnvironmentInstall(ctx, scope, computerID, revisionID, holder, ok, errMsg)
	if err != nil {
		return EnvRevision{}, err
	}
	em := r.envManager()
	var emErr error
	if ok {
		emErr = em.Activate(computerID, rev.Revision)
	} else {
		emErr = em.Rollback(computerID, rev.Revision)
	}
	relErr := r.leases.ReleaseWriter(context.WithoutCancel(ctx), scope, computerID, holder)
	if emErr != nil {
		return rev, fmt.Errorf("compute: environment settle: %w", emErr)
	}
	if relErr != nil {
		return rev, fmt.Errorf("compute: release writer lease: %w", relErr)
	}
	return rev, nil
}

// ListEnvironment returns the applied environment prefix with per-file
// digests — the observable proof of what a rollback restored.
func (r *Runner) ListEnvironment(ctx context.Context, scope contracts.Scope, computerID string) (int64, []EnvFile, error) {
	if _, err := r.store.GetComputer(ctx, scope, computerID); err != nil {
		return 0, nil, err
	}
	cur, err := r.envManager().CurrentRevision(computerID)
	if err != nil {
		return 0, nil, err
	}
	files, err := r.envManager().EnvFiles(computerID)
	if err != nil {
		return 0, nil, err
	}
	return cur, files, nil
}

// ---------------------------------------------------------------------------
// Derivative branches with their own live worktrees (PDF 3.6: for true
// parallel work the employee chooses a derivative branch).
// ---------------------------------------------------------------------------

// branchDirForTest exposes the branch worktree path to the live suite.
func (r *Runner) BranchDirForTest(computerID, branch string) string {
	return r.branchDir(computerID, branch)
}

// branchDir is a derivative branch's live worktree: a full copy of the main
// working tree taken at fork time. Patches on the branch verify and write
// inside this directory, so parallel chats never race on the same file.
func (r *Runner) branchDir(computerID, branch string) string {
	return filepath.Join(r.HomeDir(computerID), "branches", branch)
}

// DeriveBranch forks a derivative branch: the durable state branch is created
// and the live worktree is materialised as a copy of the main working tree.
func (r *Runner) DeriveBranch(ctx context.Context, scope contracts.Scope, computerID, name string) (StateRevision, error) {
	rev, err := r.store.DeriveStateBranch(ctx, scope, computerID, name)
	if err != nil {
		return StateRevision{}, err
	}
	mainHome := r.HomeDir(computerID)
	if _, err := os.Stat(mainHome); err != nil {
		// No live tree on this node yet: the durable branch exists; the
		// worktree materialises on first wake.
		return rev, nil
	}
	dir := r.branchDir(computerID, name)
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return StateRevision{}, fmt.Errorf("compute: create branch worktree dir: %w", err)
	}
	if err := copyTree(mainHome, dir); err != nil {
		return StateRevision{}, fmt.Errorf("compute: materialise branch worktree: %w", err)
	}
	return rev, nil
}

// MergeBranch merges a derivative branch into main: the durable merge runs
// first (conflicts are refused before any file moves), then the branch's
// changed files are applied to the main working tree and the branch worktree
// is consumed.
func (r *Runner) MergeBranch(ctx context.Context, scope contracts.Scope, computerID, name string) (StateRevision, error) {
	revs, err := r.store.ListStateRevisions(ctx, scope, computerID, name)
	if err != nil {
		return StateRevision{}, err
	}
	if len(revs) < 2 {
		return StateRevision{}, contracts.NewError(contracts.ErrConflict, "branch has no changes to merge")
	}
	fork, branchHead := revs[0], revs[len(revs)-1]
	changed := stateDiff(fork.Tree, branchHead.Tree)

	merged, err := r.store.MergeStateBranch(ctx, scope, computerID, name)
	if err != nil {
		return StateRevision{}, err
	}

	home := r.HomeDir(computerID)
	dir := r.branchDir(computerID, name)
	if st, err := os.Stat(home); err == nil && st.IsDir() {
		for _, p := range changed {
			f, ok := branchHead.Tree[p]
			full := filepath.Join(home, filepath.FromSlash(p))
			if !ok {
				_ = os.Remove(full)
				continue
			}
			if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
				return StateRevision{}, fmt.Errorf("compute: merge branch dir: %w", err)
			}
			content, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(p)))
			if err != nil {
				return StateRevision{}, fmt.Errorf("compute: merge branch file %s: %w", p, err)
			}
			if sha256Hex(content) != f.Hash {
				return StateRevision{}, fmt.Errorf("compute: branch file %s does not verify against its revision", p)
			}
			if err := os.WriteFile(full, content, os.FileMode(f.Mode)); err != nil {
				return StateRevision{}, fmt.Errorf("compute: merge branch file %s: %w", p, err)
			}
		}
		_ = os.RemoveAll(dir)
	}
	return merged, nil
}
