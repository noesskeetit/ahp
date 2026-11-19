package compute

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// All methods derive isolation scope from the bound task token; none of them
// accept tenant/subject/deployment. Each performs at most one HTTP request.

func invalid(msg string) error {
	return contracts.NewError(contracts.ErrInvalid, "compute sdk: "+msg)
}

// CreateComputer creates (or idempotently replays under key) a computer of the
// given class and lifecycle profile. Personal computers must be class S; that
// invariant is enforced server-side and surfaced as FORBIDDEN.
func (c *Client) CreateComputer(ctx context.Context, class, profile, workspaceID, realm, key string, quota *Quota) (Computer, error) {
	if !validClass(class) {
		return Computer{}, invalid("class must be one of S, C, X")
	}
	if !validProfile(profile) {
		return Computer{}, invalid("profile must be one of ephemeral, project, personal")
	}
	if key == "" {
		return Computer{}, invalid("key must not be empty")
	}
	var out Computer
	err := c.callTask(ctx, "CreateComputer", createComputerRequest{
		Class: class, Profile: profile, WorkspaceID: workspaceID, Realm: realm, Key: key, Quota: quota,
	}, &out)
	return out, err
}

// GetComputer fetches a computer by id.
func (c *Client) GetComputer(ctx context.Context, computerID string) (Computer, error) {
	if computerID == "" {
		return Computer{}, invalid("computer_id must not be empty")
	}
	var out Computer
	err := c.callTask(ctx, "GetComputer", computerRefRequest{ComputerID: computerID}, &out)
	return out, err
}

// AcquireIncarnation starts a new running epoch (bounded epoch) on a computer.
func (c *Client) AcquireIncarnation(ctx context.Context, computerID, backend, node, vmGenID string) (Incarnation, error) {
	if computerID == "" {
		return Incarnation{}, invalid("computer_id must not be empty")
	}
	var out Incarnation
	err := c.callTask(ctx, "AcquireIncarnation", acquireIncarnationRequest{
		ComputerID: computerID, Backend: backend, Node: node, VMGenID: vmGenID,
	}, &out)
	return out, err
}

// StopIncarnation ends a running epoch, advancing the volume fence.
func (c *Client) StopIncarnation(ctx context.Context, computerID, incarnationID string) (bool, error) {
	if computerID == "" {
		return false, invalid("computer_id must not be empty")
	}
	if incarnationID == "" {
		return false, invalid("incarnation_id must not be empty")
	}
	var out okResult
	err := c.callTask(ctx, "StopIncarnation", stopIncarnationRequest{ComputerID: computerID, IncarnationID: incarnationID}, &out)
	return out.OK, err
}

// Destroy marks a computer destroyed.
func (c *Client) Destroy(ctx context.Context, computerID string) (Computer, error) {
	if computerID == "" {
		return Computer{}, invalid("computer_id must not be empty")
	}
	var out Computer
	err := c.callTask(ctx, "Destroy", computerRefRequest{ComputerID: computerID}, &out)
	return out, err
}

// Exec records a command in the ledger without dispatching it.
//
// mayDispatch is false in two different situations, and reason says which one this is:
// the ledger already resolved this key (a replay, so the caller must act on the returned
// record rather than dispatch), or this plane cannot dispatch it at all (the backend bound
// to the class has no execution door, DEF-COMPUTE-5 - the record still stands, and a plane
// with a real substrate may claim it). Callers that need to tell the two apart use
// ExecWithReason; a plain false here would otherwise be a guess.
func (c *Client) Exec(ctx context.Context, computerID, key, command, cwd string) (Command, bool, error) {
	out, mayDispatch, _, err := c.ExecWithReason(ctx, computerID, key, command, cwd)
	return out, mayDispatch, err
}

// ExecWithReason is Exec with the plane's own explanation of a false mayDispatch. An empty
// reason means the plane raised no objection; it is not evidence about the other plane a
// worker might run on.
func (c *Client) ExecWithReason(ctx context.Context, computerID, key, command, cwd string) (Command, bool, string, error) {
	if computerID == "" {
		return Command{}, false, "", invalid("computer_id must not be empty")
	}
	if key == "" {
		return Command{}, false, "", invalid("key must not be empty")
	}
	if command == "" {
		return Command{}, false, "", invalid("command must not be empty")
	}
	var out execResult
	err := c.callTask(ctx, "Exec", execRequest{ComputerID: computerID, Key: key, Command: command, Cwd: cwd}, &out)
	return out.Command, out.MayDispatch, out.DispatchReason, err
}

// Run executes a command end-to-end on an available backend.
func (c *Client) Run(ctx context.Context, computerID, key, command, cwd string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if key == "" {
		return Command{}, invalid("key must not be empty")
	}
	if command == "" {
		return Command{}, invalid("command must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "Run", execRequest{ComputerID: computerID, Key: key, Command: command, Cwd: cwd}, &out)
	return out, err
}

// MarkDispatched transitions an accepted command to dispatched.
func (c *Client) MarkDispatched(ctx context.Context, computerID, commandID string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, invalid("command_id must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "MarkDispatched", commandRefRequest{ComputerID: computerID, CommandID: commandID}, &out)
	return out, err
}

// Complete records a command's terminal result.
func (c *Client) Complete(ctx context.Context, computerID, commandID string, exitCode int, stdout, stderr string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, invalid("command_id must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "Complete", completeRequest{
		ComputerID: computerID, CommandID: commandID, ExitCode: exitCode, Stdout: stdout, Stderr: stderr,
	}, &out)
	return out, err
}

// GetCommand reads a command by id.
func (c *Client) GetCommand(ctx context.Context, computerID, commandID string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, invalid("command_id must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "GetCommand", commandRefRequest{ComputerID: computerID, CommandID: commandID}, &out)
	return out, err
}

// Fence marks a dispatched command's side effects unknown after a lost epoch.
func (c *Client) Fence(ctx context.Context, computerID, commandID string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, invalid("command_id must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "Fence", commandRefRequest{ComputerID: computerID, CommandID: commandID}, &out)
	return out, err
}

// Cancel requests cancellation of a command; cancelPhase reports how far the
// command progressed before cancellation was decided.
func (c *Client) Cancel(ctx context.Context, computerID, commandID string) (Command, string, error) {
	if computerID == "" {
		return Command{}, "", invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, "", invalid("command_id must not be empty")
	}
	var out cancelResult
	err := c.callTask(ctx, "Cancel", commandRefRequest{ComputerID: computerID, CommandID: commandID}, &out)
	return out.Command, out.CancelPhase, err
}

// Restart re-dispatches a lost command for a fresh attempt.
func (c *Client) Restart(ctx context.Context, computerID, commandID string) (Command, error) {
	if computerID == "" {
		return Command{}, invalid("computer_id must not be empty")
	}
	if commandID == "" {
		return Command{}, invalid("command_id must not be empty")
	}
	var out Command
	err := c.callTask(ctx, "Restart", commandRefRequest{ComputerID: computerID, CommandID: commandID}, &out)
	return out, err
}

// CommitWorkspace commits a copy-on-write workspace revision, guarded by the
// expected head revision (a stale head yields CONFLICT).
func (c *Client) CommitWorkspace(ctx context.Context, computerID string, expectedHead int64, digest string) (WorkspaceRevision, error) {
	if computerID == "" {
		return WorkspaceRevision{}, invalid("computer_id must not be empty")
	}
	if digest == "" {
		return WorkspaceRevision{}, invalid("digest must not be empty")
	}
	var out WorkspaceRevision
	err := c.callTask(ctx, "CommitWorkspace", commitWorkspaceRequest{
		ComputerID: computerID, ExpectedHead: expectedHead, Digest: digest,
	}, &out)
	return out, err
}

// WorkspaceHead reads the current workspace revision head.
func (c *Client) WorkspaceHead(ctx context.Context, computerID string) (WorkspaceRevision, error) {
	if computerID == "" {
		return WorkspaceRevision{}, invalid("computer_id must not be empty")
	}
	var out WorkspaceRevision
	err := c.callTask(ctx, "WorkspaceHead", computerRefRequest{ComputerID: computerID}, &out)
	return out, err
}

// EnsureEnvironment resolves (or idempotently replays) an environment revision.
func (c *Client) EnsureEnvironment(ctx context.Context, computerID, spec string) (EnvRevision, error) {
	if computerID == "" {
		return EnvRevision{}, invalid("computer_id must not be empty")
	}
	if spec == "" {
		return EnvRevision{}, invalid("spec must not be empty")
	}
	var out EnvRevision
	err := c.callTask(ctx, "EnsureEnvironment", ensureEnvironmentRequest{ComputerID: computerID, Spec: spec}, &out)
	return out, err
}

// BeginInstall starts installing an environment revision under a holder lease.
func (c *Client) BeginInstall(ctx context.Context, computerID, spec, holder string) (EnvRevision, error) {
	if computerID == "" {
		return EnvRevision{}, invalid("computer_id must not be empty")
	}
	if spec == "" {
		return EnvRevision{}, invalid("spec must not be empty")
	}
	if holder == "" {
		return EnvRevision{}, invalid("holder must not be empty")
	}
	out, err := c.BeginInstallDetails(ctx, computerID, spec, holder)
	return out.Revision, err
}

// FinishInstall records the outcome of an environment install.
func (c *Client) FinishInstall(ctx context.Context, computerID, revisionID, holder string, ok bool, errorMsg string) (EnvRevision, error) {
	if computerID == "" {
		return EnvRevision{}, invalid("computer_id must not be empty")
	}
	if revisionID == "" {
		return EnvRevision{}, invalid("revision_id must not be empty")
	}
	if holder == "" {
		return EnvRevision{}, invalid("holder must not be empty")
	}
	var out EnvRevision
	err := c.callTask(ctx, "FinishInstall", finishInstallRequest{
		ComputerID: computerID, RevisionID: revisionID, Holder: holder, OK: ok, ErrorMsg: errorMsg,
	}, &out)
	return out, err
}

// CurrentEnvironment reads the active environment revision.
func (c *Client) CurrentEnvironment(ctx context.Context, computerID string) (EnvRevision, error) {
	if computerID == "" {
		return EnvRevision{}, invalid("computer_id must not be empty")
	}
	var out EnvRevision
	err := c.callTask(ctx, "CurrentEnvironment", computerRefRequest{ComputerID: computerID}, &out)
	return out, err
}

// Snapshot records a durable directory or memory snapshot.
func (c *Client) Snapshot(ctx context.Context, computerID, kind, path, digest string, sizeBytes int64) (Snapshot, error) {
	if computerID == "" {
		return Snapshot{}, invalid("computer_id must not be empty")
	}
	if kind == "" {
		return Snapshot{}, invalid("kind must not be empty")
	}
	if path == "" {
		return Snapshot{}, invalid("path must not be empty")
	}
	var out Snapshot
	err := c.callTask(ctx, "Snapshot", snapshotRequest{
		ComputerID: computerID, Kind: kind, Path: path, Digest: digest, SizeBytes: sizeBytes,
	}, &out)
	return out, err
}

// Export hands a jailed path to the Artifacts service and returns its sealed
// reference. Compute never mints the reference; a service that does not seal
// one yields UNAVAILABLE, not a fabricated ref.
func (c *Client) Export(ctx context.Context, computerID, path, key string) (ArtifactRef, error) {
	if computerID == "" {
		return ArtifactRef{}, invalid("computer_id must not be empty")
	}
	if path == "" {
		return ArtifactRef{}, invalid("path must not be empty")
	}
	if key == "" {
		return ArtifactRef{}, invalid("stable export key required")
	}
	var out ArtifactRef
	err := c.callTask(ctx, "Export", exportRequest{ComputerID: computerID, Path: path, Key: key}, &out)
	return out, err
}

// AcquireLease takes an activity lease (command/PTY/browser/export/writer).
func (c *Client) AcquireLease(ctx context.Context, computerID, kind, holder string, ttlMS int64) (string, error) {
	if computerID == "" {
		return "", invalid("computer_id must not be empty")
	}
	if kind == "" {
		return "", invalid("kind must not be empty")
	}
	if holder == "" {
		return "", invalid("holder must not be empty")
	}
	var out leaseResult
	err := c.callTask(ctx, "AcquireLease", acquireLeaseRequest{
		ComputerID: computerID, Kind: kind, Holder: holder, TTLms: ttlMS,
	}, &out)
	return out.LeaseID, err
}

// ReleaseLease releases an activity lease.
func (c *Client) ReleaseLease(ctx context.Context, computerID, kind, holder string) (bool, error) {
	if computerID == "" {
		return false, invalid("computer_id must not be empty")
	}
	if kind == "" {
		return false, invalid("kind must not be empty")
	}
	if holder == "" {
		return false, invalid("holder must not be empty")
	}
	var out okResult
	err := c.callTask(ctx, "ReleaseLease", releaseLeaseRequest{ComputerID: computerID, Kind: kind, Holder: holder}, &out)
	return out.OK, err
}

// CurrentFence reads the current volume fence for a computer.
func (c *Client) CurrentFence(ctx context.Context, computerID string) (VolumeFence, error) {
	if computerID == "" {
		return VolumeFence{}, invalid("computer_id must not be empty")
	}
	var out VolumeFence
	err := c.callTask(ctx, "CurrentFence", computerRefRequest{ComputerID: computerID}, &out)
	return out, err
}

func validClass(class string) bool {
	switch class {
	case ClassS, ClassC, ClassX:
		return true
	}
	return false
}

func validProfile(profile string) bool {
	switch profile {
	case ProfileEphemeral, ProfileProject, ProfilePersonal:
		return true
	}
	return false
}
