//go:build state_integration

package space_test

// runtime_durability_seam_realstate_test.go — proves the PRODUCTION durability wiring
// end-to-end: a real core.Runtime.Run, configured with cfg.Durable = the Emil seam
// RunStoreDurabilitySink, persists its emitted steps into the REAL State API over
// PostgreSQL THROUGH the seam (Runtime.Run injects the per-run fence+task-token from
// the request; the sink forwards each step via StateRunStore.AppendEvent -> the
// existing StateClient.AppendStream). This is the "stateless Core Agent pool stores
// steps in the Agent State Store by State API contract" path (PDF rev3.2 л.250/430),
// exercised against the authoritative State service (NOT a fake server, NOT in-memory).
//
// The model here is a DETERMINISTIC in-process final-text double used only to drive a
// fixed, reproducible number of steps so the WIRING + idempotency can be asserted; this
// is NOT a model-quality proof (that is the live_real suite). Nothing fakes State.
//
// Author-side candidate evidence; independent live verification is iv's.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// finalTextModel answers with text and NO tool calls, so the Core loop emits exactly
// two recorded steps (StepModel + StepDone) and terminates.
type finalTextModel struct{}

func (finalTextModel) Generate(_ context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	return &core.ModelResponse{Content: json.RawMessage(`"ok"`), Stop: "stop"}, nil
}

func TestEngineDurabilityThroughSeam_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "rtd-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "rtd-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease: %v", err)
	}

	sink := emil.NewRunStoreDurabilitySink(rs)
	rt := core.NewRuntime(core.RuntimeConfig{
		Model:   finalTextModel{},
		Tools:   core.NewToolRegistry(nil),
		Durable: sink,
	})

	req := &space.CoreRequest{
		RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch, TaskToken: l.TaskToken,
		ThreadID: l.ThreadID, Input: json.RawMessage(`"go"`), MaxSteps: 8,
	}
	ch := make(chan space.StepResult, 16)
	if err := rt.Run(ctx, req, ch); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Run pushes every step synchronously before returning but never closes the
	// channel, so drain the buffered steps by count (not via range, which waits for close).
	emitted := len(ch)
	for i := 0; i < emitted; i++ {
		<-ch
	}
	if emitted < 2 {
		t.Fatalf("expected >=2 emitted steps, got %d", emitted)
	}

	// The steps must be durably present in the REAL State thread stream under the
	// canonical per-step keys (space.StepKey(runID, seq)).
	evs, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	want := map[string]bool{
		space.StepKey(l.RunID, 0): false,
		space.StepKey(l.RunID, 1): false,
	}
	for _, ev := range evs {
		if _, ok := want[ev.Key]; ok {
			want[ev.Key] = true
		}
	}
	for k, ok := range want {
		if !ok {
			t.Fatalf("step key %q not durably recorded to real State via seam; total events=%d", k, len(evs))
		}
	}
	before := len(evs)

	// Re-run through the SAME production path must NOT add State rows (idempotent step keys).
	if err := rt.Run(ctx, req, make(chan space.StepResult, 16)); err != nil {
		t.Fatalf("re-Run: %v", err)
	}
	evs2, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil {
		t.Fatalf("ReadEvents(2): %v", err)
	}
	if len(evs2) != before {
		t.Fatalf("idempotent re-run must not add rows: before=%d after=%d", before, len(evs2))
	}

	if err := rs.Release(ctx, *l, "done"); err != nil {
		t.Logf("Release(non-fatal): %v", err)
	}
	t.Logf("production Runtime.Run durably recorded steps to REAL State through the seam: steps=%d state_events=%d idempotent_reRun_no_new_rows=true", emitted, len(evs2))
}

// variantModel answers with a body derived from its tag. Two lives of the SAME request
// therefore write DIFFERENT bytes for the same logical step — the nondeterministic step the
// key contract must never swallow.
type variantModel struct{ tag string }

func (v variantModel) Generate(_ context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	return &core.ModelResponse{Content: json.RawMessage(`"body-` + v.tag + `"`), Stop: "stop"}, nil
}

func durablePayloadOf(evs []emil.Event, key string) json.RawMessage {
	for _, ev := range evs {
		if ev.Key == key {
			return ev.Payload
		}
	}
	return nil
}

// The OTHER half of the durable contract, on the same live seam: idempotency may not become
// silent acceptance. A re-delivered run whose step body differs must be REFUSED by real
// State (KEY_REUSE_MISMATCH), must not grow the stream, must not overwrite the body an
// earlier life wrote, and must not change the run's own outcome — a durability problem is
// never a verdict. Renumbering such a step to make the write land would erase the only
// signal that the step is not reproducible.
func TestEngineDurabilityReDeliveryWithDifferentBodyIsRejected_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "rtd2-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "rtd2-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease: %v", err)
	}

	// A fresh sink per life: a re-delivery arrives in another life of the stateless pool, so
	// no in-process cache may answer for it — real State has to decide.
	runtimeFor := func(tag string) *core.Runtime {
		return core.NewRuntime(core.RuntimeConfig{
			Model:   variantModel{tag: tag},
			Tools:   core.NewToolRegistry(nil),
			Durable: emil.NewRunStoreDurabilitySink(rs),
		})
	}
	req := &space.CoreRequest{
		RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch, TaskToken: l.TaskToken,
		ThreadID: l.ThreadID, Input: json.RawMessage(`"go"`), MaxSteps: 8,
	}

	if err := runtimeFor("first").Run(ctx, req, make(chan space.StepResult, 16)); err != nil {
		t.Fatalf("Run(life 1): %v", err)
	}
	evs, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	before := len(evs)
	step0 := space.StepKey(l.RunID, 0)
	original := durablePayloadOf(evs, step0)
	if len(original) == 0 {
		t.Fatalf("step key %q was not durably recorded by life 1; events=%d", step0, before)
	}

	// Life 2: the same request, a body that does not match what is already durable.
	if err := runtimeFor("different").Run(ctx, req, make(chan space.StepResult, 16)); err != nil {
		t.Fatalf("a durable mismatch must not change the run outcome: %v", err)
	}
	evs2, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil {
		t.Fatalf("ReadEvents(2): %v", err)
	}
	if len(evs2) != before {
		t.Fatalf("a refused step must not add rows: before=%d after=%d", before, len(evs2))
	}
	if got := durablePayloadOf(evs2, step0); string(got) != string(original) {
		t.Fatalf("a refused step overwrote the durable body:\n got %s\nwant %s", got, original)
	}

	// And the refusal is a REPORTED code at the seam, not a quiet nil.
	streamCtx := space.WithRunContext(ctx, space.RunContext{
		Fence:     space.Fence{RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch},
		ThreadID:  l.ThreadID,
		TaskToken: l.TaskToken,
	})
	conflicting := json.RawMessage(`{"step_type":"model","content":"body-different","done":false}`)
	fp, err := space.StepFingerprint(string(space.StepModel), conflicting)
	if err != nil {
		t.Fatalf("StepFingerprint: %v", err)
	}
	_, err = emil.NewRunStoreDurabilitySink(rs).AppendDurable(streamCtx, step0, string(space.StepModel), fp, conflicting)
	// Measured, not assumed: over the State HTTP edge the refusal arrives wrapped as
	// INVALID_ARGUMENT with the code inside the message ("KEY_REUSE_MISMATCH: key reused
	// with a different body"); the unwrapped code is what the in-process State tests assert.
	// What this seam must guarantee is a NON-NIL refusal that names the reuse.
	if err == nil || !strings.Contains(err.Error(), "KEY_REUSE_MISMATCH") {
		t.Fatalf("same key + different body: want a refusal naming KEY_REUSE_MISMATCH, got err=%v code=%s", err, contracts.Code(err))
	}

	if err := rs.Release(ctx, *l, "done"); err != nil {
		t.Logf("Release(non-fatal): %v", err)
	}
	t.Logf("re-delivery with a different body refused by real State: rows_unchanged=%d body_preserved=true outcome_unaffected=true", before)
}
