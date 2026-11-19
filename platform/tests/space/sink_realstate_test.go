//go:build state_integration

package space_test

// sink_realstate_test.go — drives RunStoreDurabilitySink (the Core-loop durable sink
// that translates the engine's per-step emission to the State API THROUGH the seam,
// with NO new wire) against the REAL State API over PostgreSQL. It proves the
// AUTHORITATIVE idempotency + fail-closed guarantees that the offline conformance
// (fake State server) can only approximate:
//   * a fresh step is written once;
//   * an identical (key,digest) re-append is deduped by State to the SAME row
//     (no double write) — across a brand-new sink instance, i.e. a "restart";
//   * a reused key with DIFFERENT content is rejected by State (KEY_REUSE surfaces);
//   * a stale fence (resumed higher epoch) is rejected and surfaced as emil.ErrStaleEpoch.
//
// Author-side candidate evidence; independent live verification is iv's. Nothing here
// fakes State — it runs against the real stateapi + PG from cNewEnv.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func sinkRunCtx(l *emil.Lease) (context.Context, space.RunContext) {
	rc := space.RunContext{
		Fence:     space.Fence{RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch},
		ThreadID:  l.ThreadID,
		TaskToken: l.TaskToken,
	}
	return space.WithRunContext(context.Background(), rc), rc
}

func TestRunStoreDurabilitySink_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "sink-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "sink-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease: %v", err)
	}
	sctx, _ := sinkRunCtx(l)
	sink := emil.NewRunStoreDurabilitySink(rs)

	// 1) fresh step -> replayed=false, one row in the real State stream.
	p := json.RawMessage(`{"step":0,"artifact":"a"}`)
	rp, err := sink.AppendDurable(sctx, "step-0", "tool", "fp-0", p)
	if err != nil || rp {
		t.Fatalf("fresh: replayed=%v err=%v want false/nil", rp, err)
	}
	evs, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil || len(evs) != 1 {
		t.Fatalf("after fresh append want 1 row, got n=%d err=%v", len(evs), err)
	}

	// 2) same-process identical re-append -> replayed=true AND no extra row (State dedup).
	rp, err = sink.AppendDurable(sctx, "step-0", "tool", "fp-0", p)
	if err != nil || !rp {
		t.Fatalf("same-process replay: replayed=%v err=%v want true/nil", rp, err)
	}
	evs, _ = rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if len(evs) != 1 {
		t.Fatalf("identical re-append must NOT double-write; rows=%d", len(evs))
	}

	// 3) AUTHORITATIVE (cross-process) idempotency: a brand-new sink (empty cache, i.e. a
	//    "restart") re-appends the IDENTICAL step -> State dedups to the same row, no error.
	sinkRestart := emil.NewRunStoreDurabilitySink(rs)
	if _, err := sinkRestart.AppendDurable(sctx, "step-0", "tool", "fp-0", p); err != nil {
		t.Fatalf("cross-process identical replay must succeed (State dedup): %v", err)
	}
	evs, _ = rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if len(evs) != 1 {
		t.Fatalf("cross-process identical replay must NOT double-write; rows=%d", len(evs))
	}

	// 4) AUTHORITATIVE fail-closed: a brand-new sink reuses step-0 with DIFFERENT content
	//    -> State rejects with KEY_REUSE (surfaces through the seam, never swallowed).
	sinkBad := emil.NewRunStoreDurabilitySink(rs)
	_, err = sinkBad.AppendDurable(sctx, "step-0", "tool", "fp-DIFF", json.RawMessage(`{"step":0,"artifact":"tampered"}`))
	if err == nil || !strings.Contains(err.Error(), "KEY_REUSE") {
		t.Fatalf("reused key w/ different content must surface KEY_REUSE, got %v", err)
	}
	// tamper must not have changed the stored row
	evs, _ = rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if len(evs) != 1 || string(evs[0].Payload) != string(p) {
		t.Fatalf("KEY_REUSE must leave original row untouched; rows=%d payload=%s", len(evs), evs[0].Payload)
	}

	if err := rs.Release(ctx, *l, "done"); err != nil {
		// Release status string must match a terminal State status; tolerate if enum differs.
		t.Logf("Release(non-fatal) err=%v", err)
	}
	t.Logf("sink REAL State: idempotent replay (no double-write across restart), KEY_REUSE+untouched row proven")
}

func TestRunStoreDurabilitySink_RealState_StaleFence(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "sinkstale-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "sinkstale-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)
	l1, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease w1: %v", err)
	}
	sctx1, _ := sinkRunCtx(l1)
	sink := emil.NewRunStoreDurabilitySink(rs)
	if _, err := sink.AppendDurable(sctx1, "step-0", "tool", "fp-0", json.RawMessage(`{"x":1}`)); err != nil {
		t.Fatalf("w1 append should succeed: %v", err)
	}

	// Crash/resume: suspend w1 -> claim w2 at a HIGHER epoch on the SAME run.
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(ctx, l1.TaskToken, l1.RunID, "w1", l1.Epoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend w1: %v", err)
	}
	l2, err := rs.ClaimLease(ctx, "", "w2", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease w2: %v", err)
	}
	if l2.RunID != l1.RunID || l2.Epoch <= l1.Epoch {
		t.Fatalf("resume must be same run, higher epoch: w1=%d w2=%d", l1.Epoch, l2.Epoch)
	}

	// A brand-new sink driven with the STALE (w1/old-epoch) run context must be fenced by
	// the real State and surface as emil.ErrStaleEpoch (fail-closed, not a silent write).
	sinkOld := emil.NewRunStoreDurabilitySink(rs)
	_, err = sinkOld.AppendDurable(sctx1, "step-1", "tool", "fp-1", json.RawMessage(`{"x":2}`))
	if err == nil || !errors.Is(err, emil.ErrStaleEpoch) {
		t.Fatalf("stale-fence sink write must map to emil.ErrStaleEpoch, got %v", err)
	}

	// The new (w2/higher-epoch) writer is accepted through its own sink.
	sctx2, _ := sinkRunCtx(l2)
	sinkNew := emil.NewRunStoreDurabilitySink(rs)
	if _, err := sinkNew.AppendDurable(sctx2, "step-1", "tool", "fp-1", json.RawMessage(`{"x":2}`)); err != nil {
		t.Fatalf("w2 sink append should succeed: %v", err)
	}
	t.Logf("sink REAL State stale-fence: w1_epoch=%d rejected(ErrStaleEpoch) w2_epoch=%d accepted", l1.Epoch, l2.Epoch)
}
