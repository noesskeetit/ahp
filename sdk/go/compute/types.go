package compute

import "time"

// Public wire types mirror the Compute API JSON contract (snake_case). The
// isolation scope is never a field: it is derived server-side from the bound
// task token, so these shapes expose tenant/subject only as read-back echo,
// never as caller-controlled input.

// Class is a compute isolation class.
const (
	ClassS = "S" // shared kernel, strongest density
	ClassC = "C" // container isolation
	ClassX = "X" // hardware-virtualised microVM
)

// Profile is a computer lifecycle profile.
const (
	ProfileEphemeral = "ephemeral"
	ProfileProject   = "project"
	ProfilePersonal  = "personal"
)

// Computer lifecycle statuses.
const (
	StatusProvisioning = "provisioning"
	StatusActive       = "active"
	StatusSuspended    = "suspended"
	StatusDestroyed    = "destroyed"
)

// Command lifecycle statuses.
const (
	CmdAccepted          = "accepted"
	CmdDispatched        = "dispatched"
	CmdSucceeded         = "succeeded"
	CmdFailed            = "failed"
	CmdCancelled         = "cancelled"
	CmdLost              = "lost"
	CmdSideEffectUnknown = "side_effect_unknown"
)

// Quota bounds a computer's resource envelope.
type Quota struct {
	CPUMillisPerSec int32 `json:"cpu_millis_per_sec,omitempty"`
	MemoryMB        int32 `json:"memory_mb,omitempty"`
	MaxPIDs         int32 `json:"max_pids,omitempty"`
	MaxPTYs         int32 `json:"max_ptys,omitempty"`
	MaxOpenFiles    int32 `json:"max_open_files,omitempty"`
	MaxStorageMB    int32 `json:"max_storage_mb,omitempty"`
	MaxSessionTTLMS int64 `json:"max_session_ttl_ms,omitempty"`
}

// Computer is a durable compute instance record.
type Computer struct {
	ID          string    `json:"id"`
	Tenant      string    `json:"tenant"`
	Subject     string    `json:"subject"`
	Deployment  string    `json:"deployment,omitempty"`
	Class       string    `json:"class"`
	Profile     string    `json:"profile"`
	Status      string    `json:"status"`
	VolumeID    string    `json:"volume_id"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	Realm       string    `json:"realm,omitempty"`
	Incarnation int64     `json:"incarnation"`
	Quota       Quota     `json:"quota"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Incarnation is one running epoch of a computer.
type Incarnation struct {
	ID         string     `json:"id"`
	ComputerID string     `json:"computer_id"`
	Seq        int64      `json:"seq"`
	Status     string     `json:"status"`
	Backend    string     `json:"backend"`
	Node       string     `json:"node"`
	VMGenID    string     `json:"vm_gen_id,omitempty"`
	Fence      int64      `json:"fence"`
	AcquiredAt time.Time  `json:"acquired_at"`
	StoppedAt  *time.Time `json:"stopped_at,omitempty"`
}

// Command is one command-ledger entry.
type Command struct {
	ID              string    `json:"id"`
	ComputerID      string    `json:"computer_id"`
	Incarnation     int64     `json:"incarnation"`
	Key             string    `json:"key"`
	ExecutionID     string    `json:"execution_id,omitempty"`
	Status          string    `json:"status"`
	Command         string    `json:"command"`
	Cwd             string    `json:"cwd,omitempty"`
	Stdout          string    `json:"stdout,omitempty"`
	Stderr          string    `json:"stderr,omitempty"`
	ExitCode        int       `json:"exit_code"`
	HasExit         bool      `json:"has_exit"`
	OutputTruncated bool      `json:"output_truncated,omitempty"`
	OutputSeq       int64     `json:"output_seq"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// EnvRevision is one environment (package set) revision.
type EnvRevision struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Revision   int64     `json:"revision"`
	Spec       string    `json:"spec"`
	Status     string    `json:"status"`
	ParentID   string    `json:"parent_id,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// WorkspaceRevision is one copy-on-write project workspace commit.
type WorkspaceRevision struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Revision   int64     `json:"revision"`
	BaseRev    int64     `json:"base_rev"`
	Digest     string    `json:"digest"`
	Branch     string    `json:"branch"`
	CreatedAt  time.Time `json:"created_at"`
}

// Snapshot is a durable directory/memory snapshot record.
type Snapshot struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Kind       string    `json:"kind"`
	Path       string    `json:"path"`
	Digest     string    `json:"digest"`
	SizeBytes  int64     `json:"size_bytes"`
	CreatedAt  time.Time `json:"created_at"`
}

// ArtifactRef is the sealed reference returned by the Artifacts service after a
// successful export. Compute never mints it; it relays what the owner seals.
type ArtifactRef struct {
	ArtifactID string    `json:"artifact_id"`
	Version    string    `json:"version"`
	Digest     string    `json:"digest"`
	SizeBytes  int64     `json:"size_bytes"`
	SealedAt   time.Time `json:"sealed_at"`
}

// VolumeFence is the monotonic fencing token for a computer's volume.
type VolumeFence struct {
	ComputerID  string    `json:"computer_id"`
	VolumeID    string    `json:"volume_id"`
	Fence       int64     `json:"fence"`
	Incarnation string    `json:"incarnation,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// response envelopes
// ---------------------------------------------------------------------------

type execResult struct {
	Command     Command `json:"command"`
	MayDispatch bool    `json:"may_dispatch"`
	// DispatchReason explains a false MayDispatch, so a caller can tell "this key is
	// already resolved" from "nothing here can run it" (DEF-COMPUTE-5).
	DispatchReason string `json:"dispatch_reason,omitempty"`
}

type cancelResult struct {
	Command     Command `json:"command"`
	CancelPhase string  `json:"cancel_phase"`
}

type leaseResult struct {
	LeaseID string `json:"lease_id"`
}

type okResult struct {
	OK bool `json:"ok"`
}

// ---------------------------------------------------------------------------
// request shapes (strict; scope is never a field)
// ---------------------------------------------------------------------------

type createComputerRequest struct {
	Class       string `json:"class"`
	Profile     string `json:"profile"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Realm       string `json:"realm,omitempty"`
	Key         string `json:"key"`
	Quota       *Quota `json:"quota,omitempty"`
}

type computerRefRequest struct {
	ComputerID string `json:"computer_id"`
}

type acquireIncarnationRequest struct {
	ComputerID string `json:"computer_id"`
	Backend    string `json:"backend,omitempty"`
	Node       string `json:"node,omitempty"`
	VMGenID    string `json:"vm_gen_id,omitempty"`
}

type stopIncarnationRequest struct {
	ComputerID    string `json:"computer_id"`
	IncarnationID string `json:"incarnation_id"`
}

type execRequest struct {
	ComputerID string `json:"computer_id"`
	Key        string `json:"key"`
	Command    string `json:"command"`
	Cwd        string `json:"cwd,omitempty"`
}

type commandRefRequest struct {
	ComputerID string `json:"computer_id"`
	CommandID  string `json:"command_id"`
}

type completeRequest struct {
	ComputerID      string `json:"computer_id"`
	CommandID       string `json:"command_id"`
	ExitCode        int    `json:"exit_code"`
	Stdout          string `json:"stdout,omitempty"`
	Stderr          string `json:"stderr,omitempty"`
	OutputTruncated bool   `json:"output_truncated,omitempty"`
}

type commitWorkspaceRequest struct {
	ComputerID   string `json:"computer_id"`
	ExpectedHead int64  `json:"expected_head"`
	Digest       string `json:"digest"`
}

type ensureEnvironmentRequest struct {
	ComputerID string `json:"computer_id"`
	Spec       string `json:"spec"`
}

type beginInstallRequest struct {
	ComputerID string `json:"computer_id"`
	Spec       string `json:"spec"`
	Holder     string `json:"holder"`
}

type finishInstallRequest struct {
	ComputerID string `json:"computer_id"`
	RevisionID string `json:"revision_id"`
	Holder     string `json:"holder"`
	OK         bool   `json:"ok"`
	ErrorMsg   string `json:"error_msg,omitempty"`
}

type snapshotRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	SizeBytes  int64  `json:"size_bytes"`
}

type exportRequest struct {
	Key        string `json:"key"`
	ComputerID string `json:"computer_id"`
	Path       string `json:"path"`
}

type acquireLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms,omitempty"`
}

type releaseLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
}
