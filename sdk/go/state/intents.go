package state

import (
	"context"
	"encoding/json"
)

// Intent lifecycle statuses (wire contract).
const (
	IntentRecorded   = "recorded"
	IntentDispatched = "dispatched"
	IntentSucceeded  = "succeeded"
	IntentFailed     = "failed"
	IntentUnknown    = "unknown"
	IntentFenced     = "fenced"
)

// Intent is a journaled side-effect of one node call inside a run.
type Intent struct {
	ID     string          `json:"id"`
	RunID  string          `json:"run_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
}

// RecordIntent journals an intent for one node call under the current lease.
// The (run, node, call) triple is the idempotency key.
func (c *Client) RecordIntent(ctx context.Context, f Fence, node, call, kind, target string, args json.RawMessage) (Intent, error) {
	if err := validateFence(f); err != nil {
		return Intent{}, err
	}
	if node == "" {
		return Intent{}, newInvalid("node must not be empty")
	}
	if call == "" {
		return Intent{}, newInvalid("call must not be empty")
	}
	var out Intent
	req := struct {
		Fence  fenceDTO        `json:"fence"`
		Node   string          `json:"node"`
		Call   string          `json:"call"`
		Kind   string          `json:"kind"`
		Target string          `json:"target"`
		Args   json.RawMessage `json:"args,omitempty"`
	}{toFenceDTO(f), node, call, kind, target, args}
	if err := c.callTask(ctx, "RecordIntent", req, &out); err != nil {
		return Intent{}, err
	}
	return out, nil
}

// UpdateIntent transitions an intent to a new status under the current lease.
func (c *Client) UpdateIntent(ctx context.Context, f Fence, node, call, status string, result json.RawMessage) (Intent, error) {
	if err := validateFence(f); err != nil {
		return Intent{}, err
	}
	if node == "" {
		return Intent{}, newInvalid("node must not be empty")
	}
	if call == "" {
		return Intent{}, newInvalid("call must not be empty")
	}
	var out Intent
	req := struct {
		Fence  fenceDTO        `json:"fence"`
		Node   string          `json:"node"`
		Call   string          `json:"call"`
		Status string          `json:"status"`
		Result json.RawMessage `json:"result,omitempty"`
	}{toFenceDTO(f), node, call, status, result}
	if err := c.callTask(ctx, "UpdateIntent", req, &out); err != nil {
		return Intent{}, err
	}
	return out, nil
}
