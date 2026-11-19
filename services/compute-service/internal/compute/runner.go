package compute

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Runner is the control-plane loop that turns an accepted command into a real
// execution on the correct substrate, honouring the PDF lifecycle
// (AcquireVM -> Mount -> Exec -> Save/Export -> DestroyVM). A fresh incarnation
// is taken with a volume fence, the backend runs the command, and the sandbox is
// destroyed on completion — it is never recycled under another identity.
//
// Durability rule (SBX-05): once a command is DISPATCHED its outcome is never
// blindly recomputed. If the transport to the backend fails after dispatch, the
// ledger is fenced to SIDE_EFFECT_UNKNOWN rather than retried.
type Runner struct {
	store    *Store
	registry *Registry
	jailBase string
	node     string
	exporter ArtifactExportClient
	leases   *LeaseService
	envMgr   *EnvironmentManager
}

// NewRunner wires a runner. jailBase is the on-node root that host jails and
// project/personal workspaces live under; it is also the boundary exports are
// confined to. exporter may be a nil ArtifactExportClient, in which case
// export fails closed with Unavailable.
func NewRunner(store *Store, registry *Registry, jailBase, node string, exporter ArtifactExportClient) *Runner {
	if node == "" {
		node = "node"
	}
	return &Runner{store: store, registry: registry, jailBase: jailBase, node: node, exporter: exporter}
}

// Store exposes the backing store so an API layer can serve reads directly.
func (r *Runner) Store() *Store { return r.store }

// Registry exposes the backend registry (used by the supervisor host).
func (r *Runner) Registry() *Registry { return r.registry }

// commandTimeout bounds a single dispatch: the smaller of the session TTL and
// the default command ceiling.
// DispatchCapability answers the question the record layer is asked to answer and must not
// guess at: could this plane, as it is configured right now, ever dispatch a command for
// this Computer? A "no" comes back as a reason rather than an error, because being told a
// command cannot be dispatched here is not a failed request - the record is valid and still
// waits for its substrate.
//
// Why this exists (DEF-COMPUTE-5). Exec records a command and answers with may_dispatch,
// which a caller reads as "go and run this". A plane whose backend for the class implements
// no execution door - which is exactly the MOCK backend the customer accepted - can never
// honour that; RunCommand refuses it by name, but only after the caller was told to try.
// Promising at record time and refusing at dispatch time is the overclaim. The record stays
// accepted, because a plane that does have a real substrate may legitimately claim it later;
// what is no longer allowed is asserting here that this one will.
func (r *Runner) DispatchCapability(ctx context.Context, scope contracts.Scope, computerID string) (bool, string) {
	computer, err := r.store.GetComputer(ctx, scope, computerID)
	if err != nil {
		return false, "the Computer could not be read: " + err.Error()
	}
	backend, err := r.registry.Require(ctx, computer.Class)
	if err != nil {
		return false, "no backend this plane can use is registered for class " + string(computer.Class) +
			": " + err.Error()
	}
	if _, ok := backend.(CommandBackend); !ok {
		return false, fmt.Sprintf("backend %q is bound to class %s and implements no execution door; "+
			"the command stays recorded until a plane with a real substrate claims it", backend.Name(), computer.Class)
	}
	return true, ""
}

func commandTimeout(q Quota) time.Duration {
	const def = 60 * time.Second
	if q.MaxSessionTTL > 0 && q.MaxSessionTTL < def {
		return q.MaxSessionTTL
	}
	return def
}

// RunCommand executes key/command on computerID, or returns the existing ledger
// record without any side effect when the same key/body was already dispatched
// (idempotent replay). Class X requires an available microVM backend; on a host
// without KVM this fails closed with Unavailable, never degrading to host.
func (r *Runner) RunCommand(ctx context.Context, scope contracts.Scope, computerID, key, command, cwd string) (CommandRecord, error) {
	computer, err := r.store.GetComputer(ctx, scope, computerID)
	if err != nil {
		return CommandRecord{}, err
	}
	rec, mayDispatch, err := r.store.Exec(ctx, scope, computerID, key, command, cwd)
	if err != nil {
		return CommandRecord{}, err
	}
	if !mayDispatch {
		// Replayed key whose prior outcome is known (or held): return it as-is.
		return rec, nil
	}
	backend, err := r.registry.Require(ctx, computer.Class)
	if err != nil {
		// Backend unavailable (e.g. class X on a Mac): the command stays
		// ACCEPTED so it can be restarted once the substrate is present.
		return rec, err
	}
	cb, ok := backend.(CommandBackend)
	if !ok {
		return rec, Unavailable(backend.Name(), "backend cannot run commands")
	}
	inc, err := r.store.AcquireIncarnation(ctx, scope, computerID, backend.Name(), r.node, "")
	if err != nil {
		return rec, err
	}
	if err := r.store.CheckFence(ctx, scope, computerID, inc.ID); err != nil {
		_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
		return rec, err
	}
	sb, err := backend.Acquire(ctx, AcquireSpec{
		ComputerID: computerID, Incarnation: inc.Seq, VolumeID: computer.VolumeID,
		Fence: inc.Fence, Class: computer.Class, Profile: computer.Profile,
		Quota: computer.Quota, JailRoots: JailRoots(computer.Profile, r.jailBase, scope.Subject, computer.WorkspaceID),
		Node: r.node,
	})
	if err != nil {
		_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
		return rec, err
	}
	// From this point the command is DISPATCHED: any transport failure must
	// fence it to SIDE_EFFECT_UNKNOWN rather than re-run it.
	defer func() {
		if backend.Release(context.WithoutCancel(ctx), sb.ID()) == nil {
			_ = r.store.StopIncarnation(context.WithoutCancel(ctx), scope, computerID, inc.ID)
		}
	}()
	dispatched, err := r.store.MarkDispatched(ctx, scope, computerID, rec.ID)
	if err != nil {
		return rec, err
	}
	execCtx, stopWatching := r.watchOwner(ctx, scope)
	defer stopWatching()
	// The dispatch is bounded from this point on. A holder that dies must not leave the
	// ledger saying "somebody is executing this" for good, so the heartbeat is the proof
	// that THIS process is alive; when the proof stops, reconciliation closes the row
	// with a terminal verdict (Store.ReconcileStaleDispatches).
	//
	// A heartbeat that comes back not-live is a FENCE, not information: the row was
	// closed without us, so this process stops executing and writes no outcome over a
	// verdict it no longer owns. A heartbeat that fails to reach the ledger at all is
	// neither: "the database did not answer" proves nothing about the command, so the
	// dispatch continues and the next tick decides.
	hbCtx, hbStop := context.WithCancel(context.WithoutCancel(ctx))
	defer hbStop()
	var reconciledUnderway atomic.Bool
	go func() {
		ticker := time.NewTicker(CommandHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
			}
			_, live, hbErr := r.store.HeartbeatCommand(hbCtx, scope, computerID, dispatched.ID, CommandDispatchBudget)
			if hbErr != nil {
				slog.Warn("compute: command heartbeat could not reach the ledger; the dispatch continues",
					"computer_id", computerID, "command_id", dispatched.ID, "err", hbErr.Error())
				continue
			}
			if live {
				continue
			}
			reconciledUnderway.Store(true)
			slog.Warn("compute: this command was closed while still executing; stopping without writing an outcome",
				"computer_id", computerID, "command_id", dispatched.ID)
			stopWatching()
			return
		}
	}()
	// The evidence boundary: record that a backend is about to be invoked BEFORE making
	// the call, so reconciliation can tell "no backend was ever reached" (LOST: nothing
	// on this path could have touched the world) from "a backend was reached and never
	// answered" (SIDE_EFFECT_UNKNOWN: an effect may have landed). A refusal to mark -
	// the row is no longer dispatched - means another verdict already owns this command,
	// so the backend is NOT invoked: that is the double-execution fence, and it holds
	// whether the earlier process crashed or is still running.
	if _, marked, markErr := r.store.MarkBackendStarted(ctx, scope, computerID, dispatched.ID); markErr != nil {
		return dispatched, markErr
	} else if !marked {
		closed, _ := r.store.GetCommand(ctx, scope, computerID, dispatched.ID)
		return closed, contracts.NewError(contracts.ErrConflict,
			"compute: command was closed by reconciliation before a backend was invoked; it is not executed a second time")
	}
	result, err := cb.Exec(execCtx, sb, ExecSpec{
		Command: dispatched.Command, Cwd: dispatched.Cwd, Timeout: commandTimeout(computer.Quota),
		Quota: computer.Quota, Egress: EgressPolicy(computer.Class).AllowedHosts,
		CommandID: dispatched.ID,
	})
	if err != nil {
		fenced, ferr := r.store.Fence(ctx, scope, computerID, rec.ID)
		if ferr != nil {
			return dispatched, err
		}
		return fenced, err
	}
	if reconciledUnderway.Load() {
		// The row is closed and the verdict in it was reached without this process. The
		// answer the caller gets is the ledger's, not ours, and the cause is named: an
		// outcome written here would overwrite a reconciliation with a story about a
		// command nobody was watching any more.
		closed, cerr := r.store.GetCommand(ctx, scope, computerID, rec.ID)
		if cerr != nil {
			return dispatched, err
		}
		return closed, contracts.NewError(contracts.ErrConflict,
			"compute: command was reconciled while executing, its answer is not recorded over that verdict")
	}
	completed, err := r.store.CompleteWithOutput(ctx, scope, computerID, rec.ID,
		result.ExitCode, result.Stdout, result.Stderr, result.Truncated)
	if err != nil {
		return dispatched, err
	}
	return completed, nil
}

// OpenPTY attaches an interactive terminal to computerID. On the Linux target
// runtime the host/microVM backend provides a real PTY; on a control host
// without one it fails closed with Unavailable. A pty activity lease keeps the
// computer awake and is released when the session is closed.
func (r *Runner) OpenPTY(ctx context.Context, scope contracts.Scope, computerID string, spec PTYSpec) (PTYSession, error) {
	computer, err := r.store.GetComputer(ctx, scope, computerID)
	if err != nil {
		return nil, err
	}
	backend, err := r.registry.Require(ctx, computer.Class)
	if err != nil {
		return nil, err
	}
	pb, ok := backend.(PTYBackend)
	if !ok {
		return nil, Unavailable(backend.Name(), "backend cannot open PTYs")
	}
	active, err := r.store.ActiveLeases(ctx, scope, computerID)
	if err != nil {
		return nil, err
	}
	if err := CheckPTYBudget(computer.Quota, active); err != nil {
		return nil, err
	}
	spec.Quota = computer.Quota
	holder := "pty-" + contracts.NewID()
	if _, err := r.store.AcquireLease(ctx, scope, computerID, LeasePTY, holder, ptyLeaseTTL(spec.Timeout)); err != nil {
		return nil, err
	}
	inc, err := r.store.AcquireIncarnation(ctx, scope, computerID, backend.Name(), r.node, "")
	if err != nil {
		_ = r.store.ReleaseLease(ctx, scope, computerID, LeasePTY, holder)
		return nil, err
	}
	if err := r.store.CheckFence(ctx, scope, computerID, inc.ID); err != nil {
		_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
		_ = r.store.ReleaseLease(ctx, scope, computerID, LeasePTY, holder)
		return nil, err
	}
	sb, err := backend.Acquire(ctx, AcquireSpec{
		ComputerID: computerID, Incarnation: inc.Seq, VolumeID: computer.VolumeID,
		Fence: inc.Fence, Class: computer.Class, Profile: computer.Profile,
		Quota: computer.Quota, JailRoots: JailRoots(computer.Profile, r.jailBase, scope.Subject, computer.WorkspaceID),
		Node: r.node,
	})
	if err != nil {
		_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
		_ = r.store.ReleaseLease(ctx, scope, computerID, LeasePTY, holder)
		return nil, err
	}
	ptyCtx, stopWatching := r.watchOwner(ctx, scope)
	session, err := pb.OpenPTY(ptyCtx, sb, spec)
	if err != nil {
		stopWatching()
		if backend.Release(context.WithoutCancel(ctx), sb.ID()) == nil {
			_ = r.store.StopIncarnation(context.WithoutCancel(ctx), scope, computerID, inc.ID)
		}
		_ = r.store.ReleaseLease(ctx, scope, computerID, LeasePTY, holder)
		return nil, err
	}
	leased := &leasedPTY{
		PTYSession: session,
		closeOnce:  sync.Once{},
		onClose: func() {
			stopWatching()
			ctx := context.WithoutCancel(ctx)
			if backend.Release(ctx, sb.ID()) == nil {
				_ = r.store.StopIncarnation(ctx, scope, computerID, inc.ID)
			}
			_ = r.store.ReleaseLease(ctx, scope, computerID, LeasePTY, holder)
		},
	}
	go func() {
		<-ptyCtx.Done()
		_ = leased.Close()
	}()
	return leased, nil
}

func ptyLeaseTTL(t time.Duration) time.Duration {
	if t <= 0 {
		return 30 * time.Minute
	}
	return t + time.Minute
}

// leasedPTY releases its incarnation and activity lease exactly once on Close.
type leasedPTY struct {
	PTYSession
	closeOnce sync.Once
	closeErr  error
	onClose   func()
}

func (p *leasedPTY) Close() error {
	p.closeOnce.Do(func() {
		p.closeErr = p.PTYSession.Close()
		p.onClose()
	})
	return p.closeErr
}

// ExportOptions carries the task binding and owner-facing trusted labels for an
// export. The API layer fills them from the VERIFIED principal (Claims.Task and
// scope-derived labels); they never originate from a request body, which is what
// makes them "trusted" from the Artifacts owner's point of view.
type ExportOptions struct {
	TaskID, ContextID, Key, Token string
	Labels                        map[string]string
	Check                         func(context.Context) error
}

// ExportPath publishes a private snapshot of a quiescent computer home.
// A scoped row guard fences physical lifecycle until owner Seal completes.
func (r *Runner) ExportPath(ctx context.Context, scope contracts.Scope, opts ExportOptions, computerID, path string) (ArtifactRef, error) {
	if err := validIdentifier(opts.Key, "export key"); err != nil {
		return ArtifactRef{}, err
	}
	if opts.TaskID == "" || opts.ContextID == "" || opts.Check == nil {
		return ArtifactRef{}, invalid("verified export task and current authority required")
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, exportTokenKey{}, opts.Token)
	if err := opts.Check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	_, err := r.store.exportComputerState(ctx, scope, computerID)
	if err != nil {
		return ArtifactRef{}, err
	}
	if r.exporter == nil {
		return ArtifactRef{}, Unavailable("artifacts", "no Artifacts service is configured on this host")
	}
	if r.leases == nil {
		return ArtifactRef{}, Unavailable("redis", "export requires current activity and writer leases")
	}
	home, err := filepath.Abs(r.HomeDir(computerID))
	if err != nil {
		return ArtifactRef{}, err
	}
	if path == "" {
		return ArtifactRef{}, invalid("export path required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(home, filepath.FromSlash(path))
	}
	target, err := filepath.Abs(path)
	if err != nil {
		return ArtifactRef{}, err
	}
	relative, err := filepath.Rel(home, target)
	if err != nil || !filepath.IsLocal(relative) {
		return ArtifactRef{}, contracts.NewError(contracts.ErrForbidden, "export path escapes its computer")
	}
	if err = r.store.bindExportRequest(ctx, scope, opts.Key, computerID, opts.TaskID, opts.ContextID, relative); err != nil {
		return ArtifactRef{}, err
	}
	// A unique lease holder is not the caller's operation key. Concurrent
	// retries must not release or refresh each other's temporary source lock.
	holder := "export-" + contracts.NewID()
	activity, err := r.leases.AcquireActivity(ctx, scope, computerID, LeaseExport, holder, time.Minute)
	if err != nil {
		return ArtifactRef{}, err
	}
	defer func() {
		_ = r.leases.ReleaseActivity(context.WithoutCancel(ctx), scope, computerID, LeaseExport, holder)
	}()
	if _, err = r.leases.AcquireWriter(ctx, scope, computerID, holder, time.Minute); err != nil {
		return ArtifactRef{}, err
	}
	defer func() { _ = r.leases.ReleaseWriter(context.WithoutCancel(ctx), scope, computerID, holder) }()
	guard, err := r.store.beginExportGuard(ctx, scope, computerID)
	if err != nil {
		return ArtifactRef{}, err
	}
	defer guard.close()
	check := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := opts.Check(ctx); err != nil {
			return err
		}
		if err := guard.check(ctx); err != nil {
			return err
		}
		active, err := r.leases.redis.Get(ctx, r.leases.activityKey(computerID, string(LeaseExport), holder))
		if err != nil {
			return err
		}
		writer, err := r.leases.redis.Get(ctx, r.leases.writerKey(computerID))
		if err != nil {
			return err
		}
		if active != activity || writer != holder {
			return staleEpoch()
		}
		return nil
	}
	if err = check(ctx); err != nil {
		return ArtifactRef{}, err
	}
	root, err := os.OpenRoot(home)
	if err != nil {
		return ArtifactRef{}, Unavailable("source", "computer home is not mounted")
	}
	defer root.Close()
	body, size, contentType, filename, cleanup, err := prepareExport(ctx, root, relative)
	if err != nil {
		return ArtifactRef{}, err
	}
	defer cleanup()
	return r.exporter.Export(ctx, ArtifactExportRequest{
		Scope:       scope,
		ComputerID:  computerID,
		TaskID:      opts.TaskID,
		ContextID:   opts.ContextID,
		Key:         opts.Key,
		SourcePath:  filepath.ToSlash(relative),
		Check:       check,
		Labels:      trustedExportLabels(scope, opts.TaskID, computerID, opts.Labels),
		Filename:    filename,
		ContentType: contentType,
		Body:        body,
		SizeHint:    size,
	})
}

// trustedExportLabels namespaces caller-supplied labels under ahp: and binds the
// verified scope and computer. Callers must pass only labels derived from a
// verified principal; user-controlled values are never placed here.
func trustedExportLabels(scope contracts.Scope, taskID, computerID string, extra map[string]string) map[string]string {
	labels := make(map[string]string, len(extra)+5)
	for k, v := range extra {
		labels[k] = v
	}
	labels["ahp:tenant"] = scope.Tenant
	labels["ahp:deployment"] = scope.Deployment
	labels["ahp:subject"] = scope.Subject
	labels["ahp:computer"] = computerID
	if taskID != "" {
		labels["ahp:task"] = taskID
	}
	return labels
}

// within reports whether target is root or a descendant of it.
func within(root, target string) bool {
	if target == root {
		return true
	}
	sep := string(filepath.Separator)
	if !strings.HasSuffix(root, sep) {
		root += sep
	}
	return strings.HasPrefix(target, root)
}
