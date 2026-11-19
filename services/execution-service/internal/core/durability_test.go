package core

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// These tests close iv#74 / iv-evidence-74 ("CoreLoop durability & idempotency") as
// a Core-loop PROPERTY, owned and exercised in internal/core:
//   - the canonical per-step key (space.StepKey) is applied to the PRODUCTION Core
//     loop (Runtime.Run with cfg.Durable), not only to a test harness;
//   - a repeated durable step is idempotent (same key + same fingerprint -> existing
//     unchanged, no growth), and a reused key with a different fingerprint is
//     PERMANENTLY rejected (ErrKeyReuse).
//
// The store-side KEY_REUSE_MISMATCH that the State service enforces over PostgreSQL
// is validated separately against the REAL State service in
// tests/space/coordinator_idempotency_realstate_test.go; space.MemDurabilitySink
// models that exact contract here so the loop's keying and the port semantics are
// proven deterministically without a DB or the stochastic model.

type fixedExecutor struct{ res json.RawMessage }

func (f fixedExecutor) Execute(ctx context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	return f.res, nil
}

// TestStepKeyAndFingerprint pins the canonical generators: StepKey is deterministic
// and distinct per step (matching the Coordinator's production key shape exactly),
// and StepFingerprint is deterministic for a body and differs across bodies.
func TestStepKeyAndFingerprint(t *testing.T) {
	if got := space.StepKey("run-1", 7); got != "run-1:7" {
		t.Fatalf("StepKey stable per (loopID,step): got %q want run-1:7", got)
	}
	// Matches the Coordinator's emitted key shape (turnID:seq) byte-for-byte, so the
	// canonical generator is a faithful owner, not a format change.
	for i := 0; i < 5; i++ {
		want := "turn:" + strconv.Itoa(i)
		if got := space.StepKey("turn", i); got != want {
			t.Fatalf("StepKey(%q,%d)=%q want %q", "turn", i, got, want)
		}
	}
	if space.StepKey("run", 1) == space.StepKey("run", 2) {
		t.Fatal("distinct steps must yield distinct keys")
	}

	p := json.RawMessage(`{"a":1}`)
	fp1, err := space.StepFingerprint("core.step", p)
	if err != nil {
		t.Fatalf("StepFingerprint: %v", err)
	}
	fp2, _ := space.StepFingerprint("core.step", p)
	if fp1 != fp2 {
		t.Fatalf("fingerprint must be deterministic for the same (kind,payload): %q vs %q", fp1, fp2)
	}
	fpDiff, _ := space.StepFingerprint("core.step", json.RawMessage(`{"a":2}`))
	if fpDiff == fp1 {
		t.Fatal("fingerprint must differ for a different payload (drives KEY_REUSE_MISMATCH)")
	}
}

// TestDurabilitySinkIdempotentReplayAndPermanentReuse is the director's exact
// contract against a DurabilitySink: a retried step (same key + same fingerprint) is
// deduped with the existing record left unchanged; a reused key with a different
// fingerprint is rejected (ErrKeyReuse) and the rejection is PERMANENT — it still
// fails after an intervening distinct-key success.
func TestDurabilitySinkIdempotentReplayAndPermanentReuse(t *testing.T) {
	ctx := context.Background()
	sink := space.NewMemDurabilitySink()
	key := space.StepKey("run-1", 0)
	payloadA := json.RawMessage(`{"step":0,"v":"A"}`)
	fpA, err := space.StepFingerprint("core.step", payloadA)
	if err != nil {
		t.Fatalf("fingerprint: %v", err)
	}

	// First durable write of the step: stored, not a replay.
	replayed, err := sink.AppendDurable(ctx, key, "core.step", fpA, payloadA)
	if err != nil || replayed {
		t.Fatalf("first append must store (replayed=false, nil err): replayed=%v err=%v", replayed, err)
	}
	if sink.Len() != 1 {
		t.Fatalf("first append must add exactly one record, Len=%d", sink.Len())
	}

	// Idempotent replay: SAME key + SAME fingerprint -> replayed=true, existing
	// unchanged, NO growth. A retried durable emit is safe.
	replayed, err = sink.AppendDurable(ctx, key, "core.step", fpA, payloadA)
	if err != nil || !replayed {
		t.Fatalf("idempotent replay must succeed with replayed=true: replayed=%v err=%v", replayed, err)
	}
	if sink.Len() != 1 {
		t.Fatalf("idempotent replay must not grow the store, Len=%d", sink.Len())
	}
	if _, got, ok := sink.Get(key); !ok || string(got) != string(payloadA) {
		t.Fatalf("idempotent replay must leave the existing payload unchanged: got %q ok=%v", got, ok)
	}

	// Reused key with a DIFFERENT fingerprint -> permanent reject; existing untouched.
	payloadB := json.RawMessage(`{"step":0,"v":"B-different"}`)
	fpB, _ := space.StepFingerprint("core.step", payloadB)
	if fpB == fpA {
		t.Fatal("test bug: payloads must produce different fingerprints")
	}
	if replayed, err := sink.AppendDurable(ctx, key, "core.step", fpB, payloadB); err == nil || !errors.Is(err, space.ErrKeyReuse) {
		t.Fatalf("reused key with different fingerprint must be ErrKeyReuse, got replayed=%v err=%v", replayed, err)
	}
	if _, got, _ := sink.Get(key); string(got) != string(payloadA) {
		t.Fatalf("a rejected reuse must NOT overwrite the stored record: got %q", got)
	}

	// PERMANENT: after an intervening DISTINCT-key success, the same conflicting
	// key is STILL rejected (the binding is durable, not a one-shot guard).
	if _, err := sink.AppendDurable(ctx, space.StepKey("run-1", 1), "core.step", fpA, payloadA); err != nil {
		t.Fatalf("unrelated distinct key must succeed: %v", err)
	}
	if _, err := sink.AppendDurable(ctx, key, "core.step", fpB, payloadB); err == nil || !errors.Is(err, space.ErrKeyReuse) {
		t.Fatalf("key reuse must be PERMANENT after an intervening success, got: %v", err)
	}
}

// TestCoreLoopAppliesPerStepKeysInProductionPath drives the REAL Runtime.Run loop
// (recording model -> tool call -> final answer) with cfg.Durable wired, proving the
// Core loop keys every emitted step distinctly (no constant-key iv#74 defect) and
// never triggers ErrKeyReuse on a correctly-keyed multi-step loop.
func TestCoreLoopAppliesPerStepKeysInProductionPath(t *testing.T) {
	ctx := context.Background()
	sink := space.NewMemDurabilitySink()

	reg := NewToolRegistry(fixedExecutor{res: json.RawMessage(`{"tool":"ok"}`)})
	reg.Register(ToolDef{Name: "echo", Description: "echo"})

	m := &recordingModel{respond: func(call int) *ModelResponse {
		switch call {
		case 0:
			return &ModelResponse{
				Content:   json.RawMessage(`"thinking"`),
				ToolCalls: []ToolCall{{ID: "tc1", Name: "echo", Args: json.RawMessage(`{}`)}},
			}
		default:
			return &ModelResponse{Content: json.RawMessage(`"final answer"`)}
		}
	}}

	rt := NewRuntime(RuntimeConfig{Model: m, Tools: reg, Durable: sink})
	results := make(chan space.StepResult, 64)
	if err := rt.Run(ctx, &space.CoreRequest{RunID: "loop-1", Input: json.RawMessage(`"go"`)}, results); err != nil {
		t.Fatalf("Run: %v", err)
	}
	steps := drain(results)

	// The loop emitted a model, a tool, a model and a done step.
	if len(steps) < 3 {
		t.Fatalf("expected a multi-step loop, got %d steps: %+v", len(steps), steps)
	}
	// Every emitted step must have been recorded durably under a DISTINCT canonical
	// key run of the form "loop-1:<n>" (Core loop owns the per-step key).
	if sink.Len() == 0 {
		t.Fatal("Core loop must drive durable per-step appends when cfg.Durable is set")
	}
	seen := map[string]bool{}
	for i := 0; i < sink.Len(); i++ {
		k := space.StepKey("loop-1", i)
		if _, _, ok := sink.Get(k); !ok {
			t.Fatalf("Core loop must emit step %d under canonical key %q (iv#74: keys must vary per step)", i, k)
		}
		if seen[k] {
			t.Fatalf("duplicate durable key %q produced by the loop", k)
		}
		seen[k] = true
	}
	// The recorded payload must decode back to a StepResult (mirror of what the
	// Coordinator persists to the thread stream).
	if k0, p0, ok := sink.Get(space.StepKey("loop-1", 0)); !ok || k0 == "" {
		t.Fatal("step 0 record missing")
	} else if err := json.Unmarshal(p0, &space.StepResult{}); err != nil {
		t.Fatalf("durable payload must be a marshaled StepResult: %v", err)
	}
	t.Logf("Core loop drove %d durable per-step appends under distinct canonical keys, no key reuse", sink.Len())
}

// TestCoreLoopNilDurableIsBackCompat proves a Runtime without cfg.Durable still runs
// the emit loop (zero risk to existing wiring; the Coordinator remains the emitter).
func TestCoreLoopNilDurableIsBackCompat(t *testing.T) {
	m := &recordingModel{respond: func(call int) *ModelResponse {
		return &ModelResponse{Content: json.RawMessage(`"done"`)}
	}}
	rt := NewRuntime(RuntimeConfig{Model: m}) // Durable nil
	results := make(chan space.StepResult, 16)
	if err := rt.Run(context.Background(), &space.CoreRequest{RunID: "loop-nil", Input: json.RawMessage(`"go"`)}, results); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(drain(results)) == 0 {
		t.Fatal("nil-Durable runtime must still emit steps")
	}
}
