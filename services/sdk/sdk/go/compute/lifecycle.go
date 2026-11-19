package compute

import (
	"context"
	"encoding/json"
	"iter"
	"time"
)

// CompleteWithOutput preserves the backend's explicit capture-truncation flag.
func (c *Client) CompleteWithOutput(ctx context.Context, computerID, commandID string, exitCode int, stdout, stderr string, truncated bool) (Command, error) {
	var out Command
	if computerID == "" || commandID == "" {
		return out, invalid("computer_id and command_id required")
	}
	err := c.callTask(ctx, "Complete", completeRequest{ComputerID: computerID, CommandID: commandID, ExitCode: exitCode, Stdout: stdout, Stderr: stderr, OutputTruncated: truncated}, &out)
	return out, err
}

// TypedPatch calls the mounted Compute owner operation once.
func (c *Client) TypedPatch(ctx context.Context, request PatchRequest) (StateRevision, error) {
	var out StateRevision
	e := c.callTask(ctx, "TypedPatch", request, &out)
	return out, e
}

// StateHead calls the mounted Compute owner operation once.
func (c *Client) StateHead(ctx context.Context, request BranchRequest) (StateRevision, error) {
	var out StateRevision
	e := c.callTask(ctx, "StateHead", request, &out)
	return out, e
}

// StateRevisions calls the mounted Compute owner operation once.
func (c *Client) StateRevisions(ctx context.Context, request BranchRequest) ([]StateRevision, error) {
	var out struct {
		Revisions []StateRevision `json:"revisions"`
	}
	e := c.callTask(ctx, "StateRevisions", request, &out)
	return out.Revisions, e
}

// DeriveBranch calls the mounted Compute owner operation once.
func (c *Client) DeriveBranch(ctx context.Context, request BranchRequest) (StateRevision, error) {
	var out StateRevision
	e := c.callTask(ctx, "DeriveBranch", request, &out)
	return out, e
}

// MergeBranch calls the mounted Compute owner operation once.
func (c *Client) MergeBranch(ctx context.Context, request BranchRequest) (StateRevision, error) {
	var out StateRevision
	e := c.callTask(ctx, "MergeBranch", request, &out)
	return out, e
}

// Sleep calls the mounted Compute owner operation once.
func (c *Client) Sleep(ctx context.Context, computerID string) (SleepReport, error) {
	var out SleepReport
	e := c.callTask(ctx, "Sleep", computerRefRequest{ComputerID: computerID}, &out)
	return out, e
}

// Wake calls the mounted Compute owner operation once.
func (c *Client) Wake(ctx context.Context, computerID string) (Incarnation, error) {
	var out Incarnation
	e := c.callTask(ctx, "Wake", computerRefRequest{ComputerID: computerID}, &out)
	return out, e
}

// CostReport calls the mounted Compute owner operation once.
func (c *Client) CostReport(ctx context.Context, computerID string) (CostReport, error) {
	var out CostReport
	e := c.callTask(ctx, "CostReport", computerRefRequest{ComputerID: computerID}, &out)
	return out, e
}

// TakeWriterLease calls the mounted Compute owner operation once.
func (c *Client) TakeWriterLease(ctx context.Context, request WriterLeaseRequest) (string, error) {
	var out leaseResult
	e := c.callTask(ctx, "TakeWriterLease", request, &out)
	return out.LeaseID, e
}

// ReleaseWriterLease calls the mounted Compute owner operation once.
func (c *Client) ReleaseWriterLease(ctx context.Context, request WriterLeaseRequest) (bool, error) {
	var out okResult
	e := c.callTask(ctx, "ReleaseWriterLease", request, &out)
	return out.OK, e
}

// AcquireActivityLease calls the mounted Compute owner operation once.
func (c *Client) AcquireActivityLease(ctx context.Context, request ActivityLeaseRequest) (string, error) {
	var out leaseResult
	e := c.callTask(ctx, "AcquireActivityLease", request, &out)
	return out.LeaseID, e
}

// ReleaseActivityLease calls the mounted Compute owner operation once.
func (c *Client) ReleaseActivityLease(ctx context.Context, request ActivityLeaseRequest) (bool, error) {
	var out okResult
	e := c.callTask(ctx, "ReleaseActivityLease", request, &out)
	return out.OK, e
}

// ActivityLeases calls the mounted Compute owner operation once.
func (c *Client) ActivityLeases(ctx context.Context, computerID string) (ActivityLeases, error) {
	var out ActivityLeases
	e := c.callTask(ctx, "ActivityLeases", computerRefRequest{ComputerID: computerID}, &out)
	return out, e
}

// RunInstall calls the mounted Compute owner operation once.
func (c *Client) RunInstall(ctx context.Context, request InstallCommandRequest) (InstallResult, error) {
	var out InstallResult
	e := c.callTask(ctx, "RunInstall", request, &out)
	return out, e
}

// ListEnvironment calls the mounted Compute owner operation once.
func (c *Client) ListEnvironment(ctx context.Context, computerID string) (EnvironmentFiles, error) {
	var out EnvironmentFiles
	e := c.callTask(ctx, "ListEnvironment", computerRefRequest{ComputerID: computerID}, &out)
	return out, e
}

// PublishVersion calls the mounted Compute owner operation once.
func (c *Client) PublishVersion(ctx context.Context, request PublishVersionRequest) (Version, error) {
	var out Version
	e := c.callTask(ctx, "PublishVersion", request, &out)
	return out, e
}

// DrainVersion calls the mounted Compute owner operation once.
func (c *Client) DrainVersion(ctx context.Context, request DrainVersionRequest) (Version, error) {
	var out Version
	e := c.callTask(ctx, "DrainVersion", request, &out)
	return out, e
}

// RollbackVersion calls the mounted Compute owner operation once.
func (c *Client) RollbackVersion(ctx context.Context, agent string) (Version, error) {
	var out Version
	e := c.callTask(ctx, "RollbackVersion", map[string]string{"agent": agent}, &out)
	return out, e
}

// GetActiveVersion calls the mounted Compute owner operation once.
func (c *Client) GetActiveVersion(ctx context.Context, agent string) (Version, error) {
	var out Version
	e := c.callTask(ctx, "GetActiveVersion", map[string]string{"agent": agent}, &out)
	return out, e
}

// BeginInstallDetails preserves the full runner response and accepts the store-only shape.
func (c *Client) BeginInstallDetails(ctx context.Context, computerID, spec, holder string) (InstallSession, error) {
	var raw json.RawMessage
	if computerID == "" || spec == "" || holder == "" {
		return InstallSession{}, invalid("computer_id, spec and holder required")
	}
	e := c.callTask(ctx, "BeginInstall", beginInstallRequest{ComputerID: computerID, Spec: spec, Holder: holder}, &raw)
	if e != nil {
		return InstallSession{}, e
	}
	var out InstallSession
	if json.Unmarshal(raw, &out) != nil {
		return out, invalid("invalid install response")
	}
	if out.Revision.ID == "" {
		if json.Unmarshal(raw, &out.Revision) != nil || out.Revision.ID == "" {
			return out, invalid("missing install revision")
		}
	}
	return out, nil
}

// AttachCommand polls durable snapshots, yielding status/output changes. It is not a PTY
// stream. UNKNOWN/LOST are terminal for observation and never trigger Restart.
func (c *Client) AttachCommand(ctx context.Context, computerID, commandID string, interval time.Duration) iter.Seq2[Command, error] {
	return func(yield func(Command, error) bool) {
		if interval <= 0 {
			interval = 200 * time.Millisecond
		}
		last := Command{}
		first := true
		for {
			v, e := c.GetCommand(ctx, computerID, commandID)
			if e != nil {
				yield(Command{}, e)
				return
			}
			if first || v.Status != last.Status || v.OutputSeq != last.OutputSeq || !v.UpdatedAt.Equal(last.UpdatedAt) {
				if !yield(v, nil) {
					return
				}
				last = v
				first = false
			}
			switch v.Status {
			case CmdSucceeded, CmdFailed, CmdCancelled, CmdLost, CmdSideEffectUnknown:
				return
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				yield(Command{}, ctx.Err())
				return
			case <-timer.C:
			}
		}
	}
}
