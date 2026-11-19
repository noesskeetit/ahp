package emil

// intents.go — T1 Table A completion: the run-store INTENT lifecycle (workflow.py
// records each tool call as an intent before executing it, then advances its status,
// so a crash between "decided" and "executed" is recoverable and never double-run).
// This forwards to the authoritative StateClient intent ops (RecordIntent/UpdateIntent/
// GetIntent/ReconcileIntent) — fence {run_id,worker,epoch} in the body of the writes,
// GetIntent is a fenceless read, ReconcileIntent is the privileged UNKNOWN exit gated
// by the exact original digest. Intents are addressed by (run_id, node, call), not an
// opaque id, per the published State contract.

import (
	"context"
	"encoding/json"
)

// Intent is a tool-call intent record (workflow.py intent row -> State intent).
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

// RecordIntent parks an intent before the tool executes it (fenced write).
func (s *StateRunStore) RecordIntent(ctx context.Context, l Lease, node, call, kind, target string, args json.RawMessage) (*Intent, error) {
	ir, err := s.state.RecordIntent(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, node, call, kind, target, args)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return &Intent{ID: ir.ID, RunID: ir.RunID, Node: ir.Node, Call: ir.Call, Kind: ir.Kind, Target: ir.Target, Digest: ir.Digest, Status: ir.Status, Result: ir.Result}, nil
}

// UpdateIntent advances an intent's lifecycle (fenced write).
func (s *StateRunStore) UpdateIntent(ctx context.Context, l Lease, node, call, status string, result json.RawMessage) (*Intent, error) {
	ir, err := s.state.UpdateIntent(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, node, call, status, result)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return &Intent{ID: ir.ID, RunID: ir.RunID, Node: ir.Node, Call: ir.Call, Kind: ir.Kind, Target: ir.Target, Digest: ir.Digest, Status: ir.Status, Result: ir.Result}, nil
}

// GetIntent reads an intent by its (run_id, node, call) coordinates (fenceless read,
// authorized by the task-token scope).
func (s *StateRunStore) GetIntent(ctx context.Context, token, runID, node, call string) (*Intent, error) {
	ir, err := s.state.GetIntent(ctx, token, runID, node, call)
	if err != nil {
		return nil, err
	}
	return &Intent{ID: ir.ID, RunID: ir.RunID, Node: ir.Node, Call: ir.Call, Kind: ir.Kind, Target: ir.Target, Digest: ir.Digest, Status: ir.Status, Result: ir.Result}, nil
}

// ReconcileIntent is the privileged UNKNOWN->terminal transition, gated by the exact
// original digest so a reconciler cannot overwrite an intent it did not observe.
func (s *StateRunStore) ReconcileIntent(ctx context.Context, l Lease, node, call, status, digest string, result json.RawMessage) (*Intent, error) {
	ir, err := s.state.ReconcileIntent(ctx, l.TaskToken, l.Fence(), node, call, status, digest, result)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return &Intent{ID: ir.ID, RunID: ir.RunID, Node: ir.Node, Call: ir.Call, Kind: ir.Kind, Target: ir.Target, Digest: ir.Digest, Status: ir.Status, Result: ir.Result}, nil
}
