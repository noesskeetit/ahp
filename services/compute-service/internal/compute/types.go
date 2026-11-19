// Package compute is the Compute service core: security classes (S/C/X),
// Computer lifecycles (ephemeral / project / personal), disposable VM
// incarnations, volume fencing, activity leases, the command ledger with
// SIDE_EFFECT_UNKNOWN semantics, snapshots, package/environment revisions with
// rollback and project copy-on-write.
//
// Trust model (non-negotiable, from the architecture spec):
//   - Class S is the trusted multi-tenant Agents Space runtime. Only S may
//     reach an employee's personal Computer, and only on that employee's
//     behalf.
//   - Class C is developer code running per deployment.
//   - Class X runs arbitrary code inside a microVM and NEVER has access to a
//     personal Computer, personal memory or the coordinator.
//
// The isolation scope is always derived from verified identity upstream (see
// internal/compute/computeapi); nothing in this package trusts scope supplied
// by a request body.
package compute

import (
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Class is the platform security class of a workload. The strings are part of
// the durable wire format.
type Class string

const (
	// ClassS is the trusted Agents Space runtime with its own node pool and
	// the only class allowed to touch employee personal Computers.
	ClassS Class = "S"
	// ClassC is developer code running in a per-deployment container.
	ClassC Class = "C"
	// ClassX is arbitrary untrusted code confined to a microVM with no access
	// to Computers, personal memory or the coordinator.
	ClassX Class = "X"
)

// Valid reports whether c is one of the three defined classes.
func (c Class) Valid() bool {
	switch c {
	case ClassS, ClassC, ClassX:
		return true
	default:
		return false
	}
}

// Profile selects how a Computer's state behaves across sessions.
type Profile string

const (
	// ProfileEphemeral is a clean VM per session; state is thrown away on
	// Destroy and never reused.
	ProfileEphemeral Profile = "ephemeral"
	// ProfileProject pins a read-only base revision plus a copy-on-write
	// delta committed with an optimistic-expected head.
	ProfileProject Profile = "project"
	// ProfilePersonal is an employee's Computer: a persistent HOME volume plus
	// versioned environment layers, shared by that employee's chats. Only class
	// S may acquire it.
	ProfilePersonal Profile = "personal"
)

// Valid reports whether p is a defined profile.
func (p Profile) Valid() bool {
	switch p {
	case ProfileEphemeral, ProfileProject, ProfilePersonal:
		return true
	default:
		return false
	}
}

// PersonalRequiresTrustedClass encodes the invariant that a personal Computer
// is only ever class S. Custom agents (class X) and developer containers
// (class C) may use ephemeral and project profiles but never a Computer.
func PersonalRequiresTrustedClass(p Profile, c Class) bool {
	return p != ProfilePersonal || c == ClassS
}

// ComputerStatus is the lifecycle state of a Computer.
type ComputerStatus string

const (
	// ComputerProvisioning means an incarnation is being acquired.
	ComputerProvisioning ComputerStatus = "provisioning"
	// ComputerActive means an incarnation is running and serving commands.
	ComputerActive ComputerStatus = "active"
	// ComputerSuspended means no incarnation is running; state is durable and
	// the next acquire re-attaches the same volumes.
	ComputerSuspended ComputerStatus = "suspended"
	// ComputerDestroyed means the Computer and its scratch state are gone.
	ComputerDestroyed ComputerStatus = "destroyed"
)

// IncarnationStatus is the lifecycle state of one disposable VM.
type IncarnationStatus string

const (
	// IncarnationAcquiring means a VM is being started from the golden
	// snapshot / template.
	IncarnationAcquiring IncarnationStatus = "acquiring"
	// IncarnationRunning means the VM is up with volumes fenced to it.
	IncarnationRunning IncarnationStatus = "running"
	// IncarnationStopped means the VM was destroyed and will never return to
	// the pool.
	IncarnationStopped IncarnationStatus = "stopped"
)

// CommandStatus is the state of one entry in the durable command ledger. The
// state machine is:
//
//	ACCEPTED -> DISPATCHED -> {SUCCEEDED, FAILED, CANCELLED, LOST, SIDE_EFFECT_UNKNOWN}
//	ACCEPTED -> FENCED          (never dispatched; key closed forever)
//
// Retries are only legal while a command is still ACCEPTED. Once DISPATCHED the
// outcome is either a terminal status or SIDE_EFFECT_UNKNOWN; it is never
// blindly re-run.
type CommandStatus string

const (
	// CommandAccepted means the command is durably recorded but not yet sent
	// to a backend.
	CommandAccepted CommandStatus = "accepted"
	// CommandDispatched means the command was handed to a backend; a response
	// has not yet been durably recorded.
	CommandDispatched CommandStatus = "dispatched"
	// CommandSucceeded means the command completed with exit code 0.
	CommandSucceeded CommandStatus = "succeeded"
	// CommandFailed means the command completed with a non-zero exit code.
	CommandFailed CommandStatus = "failed"
	// CommandCancelled means the command was cancelled before it ran.
	CommandCancelled CommandStatus = "cancelled"
	// CommandLost means the backend vanished before reporting; the state of the
	// world may or may not have changed but the run is unrecoverable.
	CommandLost CommandStatus = "lost"
	// CommandSideEffectUnknown means the command reached a backend that then
	// failed to answer: the external side effect may have happened. A guard is
	// held until reconciliation or a human resolves it. Never auto-retried.
	CommandSideEffectUnknown CommandStatus = "side_effect_unknown"
	// CommandFenced means an ACCEPTED command was fenced (closed) before it was
	// ever dispatched; its idempotency key is closed forever and a fresh key
	// must be used to retry.
	CommandFenced CommandStatus = "fenced"
)

// Terminal reports whether no further status transition is legal for a command.
func (s CommandStatus) Terminal() bool {
	switch s {
	case CommandSucceeded, CommandFailed, CommandCancelled, CommandLost,
		CommandSideEffectUnknown, CommandFenced:
		return true
	default:
		return false
	}
}

// TerminalReason values recorded on a ledger row that was closed by reconciliation
// instead of by an answer. They exist because a verdict that arrived on its own and a
// verdict the platform had to infer are different facts for an operator, and the
// ledger must not flatten them into the same status change.
const (
	// TerminalReasonDispatchDeadline means the dispatch budget expired with no
	// answer and no heartbeat: the holder of the command is gone.
	TerminalReasonDispatchDeadline = "dispatch_deadline_exceeded"
	// TerminalReasonBackendUnanswered qualifies that verdict with the one thing
	// still knowable from the ledger: a backend HAD been invoked (or had already
	// written output), so an effect may have landed. The row closes as
	// side_effect_unknown and the export guard keeps holding it.
	TerminalReasonBackendUnanswered = "backend_invoked_without_answer"
)

// ReconciledCommand is one ledger row reconciliation closed, with the verdict and the
// evidence that produced it. It is returned rather than only logged so the operator
// (and the next test) can see exactly which dispatched rows stopped being open-ended.
type ReconciledCommand struct {
	ComputerID string
	CommandID  string
	Status     CommandStatus
	Reason     string
	// BackendInvoked is the evidence the verdict was made from: output already
	// arrived, or a backend had been invoked for this command.
	BackendInvoked bool
}

// ReconcileReport is the outcome of one reconciliation sweep.
type ReconcileReport struct {
	Examined int
	Closed   []ReconciledCommand
}

// CancelPhase is the observable outcome of a cancellation request.
type CancelPhase string

const (
	// CancelCancelled means the command was closed before it ran.
	CancelCancelled CancelPhase = "cancelled"
	// CancelUnderReview means N dispatched commands could not be confirmed as
	// not-run and are held for reconciliation ("cancelled, N operations under
	// review").
	CancelUnderReview CancelPhase = "under_review"
)

// VolumeFence is the monotonically increasing attachment epoch of a Computer's
// state volume. Every attach increments the fence; a stale holder carrying an
// older fence is rejected with STALE_EPOCH, which is what stops two incarnations
// writing the same volume.
type VolumeFence struct {
	ComputerID  string
	VolumeID    string
	Fence       int64
	Incarnation string
	UpdatedAt   time.Time
}

// ActivityLeaseKind is the category of work that keeps a Computer awake.
type ActivityLeaseKind string

const (
	// LeaseCommand is a running command.
	LeaseCommand ActivityLeaseKind = "command"
	// LeasePTY is an attached interactive terminal.
	LeasePTY ActivityLeaseKind = "pty"
	// LeaseBrowser is an attached browser worker.
	LeaseBrowser ActivityLeaseKind = "browser"
	// LeaseExport is a running export.
	LeaseExport ActivityLeaseKind = "export"
	// LeaseWriter is an exclusive writer lease on the working tree, taken by a
	// command whose writable file set is unknown.
	LeaseWriter ActivityLeaseKind = "writer"
)

// Valid reports whether k is a defined lease kind.
func (k ActivityLeaseKind) Valid() bool {
	switch k {
	case LeaseCommand, LeasePTY, LeaseBrowser, LeaseExport, LeaseWriter:
		return true
	default:
		return false
	}
}

// Quota is the per-Computer resource envelope enforced at acquire, exec and pty
// time. Zero means "use the class default". PTYs are bounded per workspace
// (SBX-03: at most 64 PTYs).
type Quota struct {
	MaxCPUMillisPerSec int32
	MaxMemoryMB        int32
	MaxPIDs            int32
	MaxPTYs            int32
	MaxOpenFiles       int32
	MaxStorageMB       int32
	MaxSessionTTL      time.Duration
}

// SnapshotKind distinguishes a filesystem (directory) snapshot from an in-guest
// memory snapshot (SBX-08 warm-boot, PoC).
type SnapshotKind string

const (
	// SnapshotDirectory is a filesystem snapshot of a path.
	SnapshotDirectory SnapshotKind = "directory"
	// SnapshotMemory is a guest memory snapshot used to warm a pool.
	SnapshotMemory SnapshotKind = "memory"
)

// Snapshot is a durable reference to a point-in-time snapshot.
type Snapshot struct {
	ID         string
	ComputerID string
	Kind       SnapshotKind
	Path       string
	Digest     string
	SizeBytes  int64
	CreatedAt  time.Time
}

// EnvRevision is one immutable environment/package layer. Installing packages
// is an exclusive environment operation that produces a new revision; a failed
// install rolls back to the previous revision.
type EnvRevision struct {
	ID         string
	ComputerID string
	Revision   int64
	Spec       string
	Status     string
	ParentID   string
	ErrorMsg   string
	CreatedAt  time.Time
}

// WorkspaceRevision is one committed copy-on-write delta of a project Computer.
// CommitWorkspace performs optimistic concurrency against expectedHead; a
// mismatch is WORKSPACE_CONFLICT and produces a conflict branch rather than a
// silent overwrite.
type WorkspaceRevision struct {
	ID         string
	ComputerID string
	Revision   int64
	BaseRev    int64
	Digest     string
	Branch     string
	CreatedAt  time.Time
}

// ArtifactRef is the addressable, sealed version returned by the Artifacts
// service after Compute exports bytes. It is the owner-minted logical
// reference (id+version+verified digest+size); it never exposes a bucket or
// storage key, so no consumer can bypass the Artifacts access proxy.
type ArtifactRef struct {
	ArtifactID string
	Version    string
	Digest     string
	SizeBytes  int64
	SealedAt   time.Time
}

// Computer is the durable logical compute unit. For personal and project
// profiles it survives incarnation churn; for ephemeral it exists only to
// drive one session.
type Computer struct {
	ID          string
	Scope       contracts.Scope
	Class       Class
	Profile     Profile
	Status      ComputerStatus
	VolumeID    string
	WorkspaceID string
	Realm       string
	Incarnation int64
	Quota       Quota
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Incarnation is a single disposable VM. It never returns to the pool.
type Incarnation struct {
	ID         string
	ComputerID string
	Seq        int64
	Status     IncarnationStatus
	Backend    string
	Node       string
	VMGenID    string
	Fence      int64
	AcquiredAt time.Time
	StoppedAt  *time.Time
}

// CommandRecord is one row of the durable command ledger. ExecutionID is the
// SBX-05 buffering/reconnect key.
type CommandRecord struct {
	ID          string
	ComputerID  string
	Incarnation int64
	Key         string
	ExecutionID string
	Status      CommandStatus
	Command     string
	Cwd         string
	Stdout      string
	Stderr      string
	ExitCode    int
	HasExit     bool
	// OutputTruncated reports that at least one output stream hit the capture cap, so
	// Stdout/Stderr here are a prefix of what the command wrote. Without it a bounded
	// log is indistinguishable from a complete one, and an observer reading "the
	// command's logs" would be reading a silently shortened version of them.
	OutputTruncated bool
	OutputSeq       int64
	// DispatchDeadline is the bound on a dispatch: the moment after which no
	// answer is expected any more and reconciliation may close the row. It is
	// zero while the command is still only ACCEPTED (nothing was dispatched).
	DispatchDeadline time.Time
	// HeartbeatAt is the last proof of life the holder renewed while the command
	// ran. It is what makes "the runner died" an observed fact rather than a guess.
	HeartbeatAt time.Time
	// BackendStartedAt records that a backend was actually invoked for this
	// command. It is the ledger's evidence boundary: before it nothing on this
	// dispatch path could have touched the world, after it an effect is possible.
	BackendStartedAt time.Time
	// TerminalReason names, for an operator, who closed a command that never
	// answered and on what evidence (see TerminalReason*). It is empty for every
	// command that reached its own answer.
	TerminalReason string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ImageVersionStatus is the lifecycle state of a published image version.
type ImageVersionStatus string

const (
	// ImageVersionActive means this is the version new tasks route to.
	ImageVersionActive ImageVersionStatus = "active"
	// ImageVersionDrain means existing tasks complete here but new tasks
	// route to the active version. After drain_deadline, pending tasks are
	// migrated (if state_compat) or failed.
	ImageVersionDrain ImageVersionStatus = "drain"
	// ImageVersionRetired means all tasks have migrated or completed; the
	// version is no longer serviceable.
	ImageVersionRetired ImageVersionStatus = "retired"
	// ImageVersionRolledBack means a rollback was performed; new tasks route
	// to the previous version. Tasks already started on this version stay.
	ImageVersionRolledBack ImageVersionStatus = "rolled_back"
)

// ImageVersion is one published image@sha256 for an agent within a
// (tenant, deployment) scope. The version is pinned to tasks at creation;
// publishing a new version never migrates existing tasks.
type ImageVersion struct {
	ID            string
	Scope         contracts.Scope
	Agent         string
	Digest        string
	Status        ImageVersionStatus
	DrainDeadline *time.Time
	StateCompat   bool
	PublishedAt   time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
