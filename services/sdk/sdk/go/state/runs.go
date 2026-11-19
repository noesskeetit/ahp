package state

import (
	"context"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// CreateThread creates (or idempotently replays) a thread.
func (c *Client) CreateThread(ctx context.Context, contextID, key string) (Thread, error) {
	if contextID == "" {
		return Thread{}, newInvalid("context_id must not be empty")
	}
	if key == "" {
		return Thread{}, newInvalid("key must not be empty")
	}
	var out Thread
	err := c.callTask(ctx, "CreateThread", createThreadRequest{ContextID: contextID, Key: key}, &out)
	return out, err
}

// GetThread fetches a thread by id.
func (c *Client) GetThread(ctx context.Context, id string) (Thread, error) {
	if id == "" {
		return Thread{}, newInvalid("thread_id must not be empty")
	}
	var out Thread
	err := c.callTask(ctx, "GetThread", getThreadRequest{ThreadID: id}, &out)
	return out, err
}

// GetThreadByContext fetches the thread bound to contextID.
func (c *Client) GetThreadByContext(ctx context.Context, contextID string) (Thread, error) {
	if contextID == "" {
		return Thread{}, newInvalid("context_id must not be empty")
	}
	var out Thread
	err := c.callTask(ctx, "GetThreadByContext", getThreadByContextRequest{ContextID: contextID}, &out)
	return out, err
}

// CreateRun appends a run to a thread.
func (c *Client) CreateRun(ctx context.Context, threadID, key string, payload json.RawMessage) (Run, error) {
	if threadID == "" {
		return Run{}, newInvalid("thread_id must not be empty")
	}
	if key == "" {
		return Run{}, newInvalid("key must not be empty")
	}
	if len(payload) == 0 {
		return Run{}, newInvalid("payload must not be empty")
	}
	var out Run
	err := c.callTask(ctx, "CreateRun", createRunRequest{ThreadID: threadID, Key: key, Payload: payload}, &out)
	return out, err
}

// GetRun fetches a run by id.
func (c *Client) GetRun(ctx context.Context, runID string) (Run, error) {
	if runID == "" {
		return Run{}, newInvalid("run_id must not be empty")
	}
	var out Run
	err := c.callTask(ctx, "GetRun", getRunRequest{RunID: runID}, &out)
	return out, err
}

// ListRuns lists the runs of a thread.
func (c *Client) ListRuns(ctx context.Context, threadID string) ([]Run, error) {
	if threadID == "" {
		return nil, newInvalid("thread_id must not be empty")
	}
	var out listRunsResponse
	if err := c.callTask(ctx, "ListRuns", listRunsRequest{ThreadID: threadID}, &out); err != nil {
		return nil, err
	}
	return out.Runs, nil
}

// ClaimRun claims the next claimable run (certificate-only, no bearer token).
func (c *Client) ClaimRun(ctx context.Context, worker string, ttlMS int64) (*Claimed, error) {
	return c.ClaimRunWithFilter(ctx, worker, ttlMS, ClaimFilter{})
}

// ClaimRunFor claims only runID when it is the eligible FIFO head of its thread.
func (c *Client) ClaimRunFor(ctx context.Context, worker string, ttlMS int64, runID string) (*Claimed, error) {
	if runID == "" {
		return nil, newInvalid("run_id must not be empty")
	}
	return c.ClaimRunWithFilter(ctx, worker, ttlMS, ClaimFilter{RunID: runID})
}

// ClaimRunWithFilter claims a FIFO head matching both optional selectors.
// PayloadType matches payload.type; it does not authorize a different scope or
// allow overtaking another unfinished run in the same thread.
func (c *Client) ClaimRunWithFilter(ctx context.Context, worker string, ttlMS int64, filter ClaimFilter) (*Claimed, error) {
	if len(filter.PayloadType) > 128 || !utf8.ValidString(filter.PayloadType) {
		return nil, newInvalid("payload_type must be valid UTF-8 of at most 128 bytes")
	}
	if worker == "" {
		return nil, newInvalid("worker must not be empty")
	}
	if err := validateTTL(ttlMS); err != nil {
		return nil, err
	}
	var out struct {
		Lease     *Lease `json:"lease"`
		TaskToken string `json:"task_token"`
	}
	if err := c.call(ctx, "ClaimRun", claimRunRequest{Worker: worker, TTLms: ttlMS, RunID: filter.RunID, PayloadType: filter.PayloadType}, &out); err != nil {
		return nil, err
	}
	if out.Lease == nil {
		return nil, nil
	}
	return &Claimed{Lease: *out.Lease, TaskToken: out.TaskToken}, nil
}

// Heartbeat extends the lease.
func (c *Client) Heartbeat(ctx context.Context, f Fence, ttlMS int64) (time.Time, error) {
	if err := validateFence(f); err != nil {
		return time.Time{}, err
	}
	if err := validateTTL(ttlMS); err != nil {
		return time.Time{}, err
	}
	var out heartbeatResponse
	err := c.callTask(ctx, "Heartbeat", heartbeatRequest{
		RunID: f.RunID, Worker: f.Worker, Epoch: f.Epoch, TTLms: ttlMS,
	}, &out)
	return out.ExpiresAt, err
}

// Release ends the lease without stating a reason. It is exactly
// ReleaseWithReason with an empty reason: no key is sent, so a peer that does not
// know the field answers exactly as it did before this field existed.
func (c *Client) Release(ctx context.Context, f Fence, status string) error {
	return c.ReleaseWithReason(ctx, f, status, "")
}

// ReleaseWithReason ends the lease and names why the run ended, so the run's
// terminal row carries the same terminal fact the execution plane journals for
// its task. The caller's words are sent verbatim; an empty reason sends nothing.
func (c *Client) ReleaseWithReason(ctx context.Context, f Fence, status, closeReason string) error {
	return c.ReleaseWithReasonCause(ctx, f, status, closeReason, "")
}

// ReleaseWithReasonCause ends the lease with both halves of a reason: the execution
// plane's dictionary word (closeReason) and the plane's own sentence (failureCause).
// Either half alone is a stated reason; DEF-RELEASE-REASON-1 requires at least one of
// them on a failed or canceled status, and an empty half sends no key at all, which
// keeps every existing caller's wire form byte-identical.
func (c *Client) ReleaseWithReasonCause(ctx context.Context, f Fence, status, closeReason, failureCause string) error {
	if err := validateFence(f); err != nil {
		return err
	}
	if !IsTerminalStatus(status) {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: release status must be terminal")
	}
	var out okResponse
	return c.callTask(ctx, "Release", releaseRequest{
		RunID: f.RunID, Worker: f.Worker, Epoch: f.Epoch, Status: status,
		CloseReason: closeReason, FailureCause: failureCause,
	}, &out)
}

// Suspend parks the run in waiting.
func (c *Client) Suspend(ctx context.Context, f Fence, waitFor string, deadline *time.Time) error {
	if err := validateFence(f); err != nil {
		return err
	}
	if waitFor == "" {
		return newInvalid("wait_for must not be empty")
	}
	var out okResponse
	return c.callTask(ctx, "Suspend", suspendRequest{
		RunID: f.RunID, Worker: f.Worker, Epoch: f.Epoch, WaitFor: waitFor, Deadline: deadline,
	}, &out)
}

// DeleteThread deletes a thread and all its sub-resources (cascade).
// Server contract: body is {"thread_id"} only, no fence.
func (c *Client) DeleteThread(ctx context.Context, threadID string) error {
	if threadID == "" {
		return newInvalid("thread_id must not be empty")
	}
	var out okResponse
	req := struct {
		ThreadID string `json:"thread_id"`
	}{threadID}
	if err := c.callTask(ctx, "DeleteThread", req, &out); err != nil {
		return err
	}
	if !out.OK {
		return contracts.NewError(contracts.ErrConflict, "state sdk: delete was not acknowledged")
	}
	return nil
}
