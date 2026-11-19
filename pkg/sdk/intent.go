package sdk

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// OperationKey derives a deterministic idempotency key for a tool-call side
// effect from the run, node, and call index (per PDF section 6.3).
// The key is unique within a deployment: hash(task_id, node_id, call_index).
func OperationKey(runID, nodeID string, callIndex int) string {
	d, _ := contracts.Digest([]any{runID, nodeID, callIndex})
	return d
}

// IntentRecorder wraps RecordIntent and UpdateIntent for the current lease.
type IntentRecorder struct {
	client *sdkstate.Client
	fence  sdkstate.Fence
	runID  string
}

// NewIntentRecorder creates a recorder scoped to the given lease.
func NewIntentRecorder(client *sdkstate.Client, fence sdkstate.Fence, runID string) *IntentRecorder {
	return &IntentRecorder{client: client, fence: fence, runID: runID}
}

// Record journals an intent for one node call. The (node, call) pair is the
// idempotency key. Returns the intent with its operation key (Digest field).
func (r *IntentRecorder) Record(ctx context.Context, node, call, kind, target string, args json.RawMessage) (sdkstate.Intent, error) {
	intent, err := r.client.RecordIntent(ctx, r.fence, node, call, kind, target, args)
	if err != nil {
		return sdkstate.Intent{}, fmt.Errorf("sdk: RecordIntent: %w", err)
	}
	return intent, nil
}

// Dispatched transitions the intent to DISPATCHED (the effect was sent).
func (r *IntentRecorder) Dispatched(ctx context.Context, node, call string) (sdkstate.Intent, error) {
	return r.update(ctx, node, call, sdkstate.IntentDispatched, nil)
}

// Succeeded transitions the intent to SUCCEEDED with the result.
func (r *IntentRecorder) Succeeded(ctx context.Context, node, call string, result json.RawMessage) (sdkstate.Intent, error) {
	return r.update(ctx, node, call, sdkstate.IntentSucceeded, result)
}

// Failed transitions the intent to FAILED with the error result.
func (r *IntentRecorder) Failed(ctx context.Context, node, call string, result json.RawMessage) (sdkstate.Intent, error) {
	return r.update(ctx, node, call, sdkstate.IntentFailed, result)
}

func (r *IntentRecorder) update(ctx context.Context, node, call, status string, result json.RawMessage) (sdkstate.Intent, error) {
	intent, err := r.client.UpdateIntent(ctx, r.fence, node, call, status, result)
	if err != nil {
		return sdkstate.Intent{}, fmt.Errorf("sdk: UpdateIntent: %w", err)
	}
	return intent, nil
}

// RunID returns the run this recorder is scoped to.
func (r *IntentRecorder) RunID() string { return r.runID }
