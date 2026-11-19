//go:build state_integration

package space_test

// seam_realstate_test.go — drives OUR Emil workflow run-store SEAM
// (adapters/emil.StateRunStore) against the REAL State API service
// (stateapi over PostgreSQL), NOT a fake server. This is the real-API upgrade of
// the seam's offline client-conformance (fake State SERVER over our client). It
// proves, over the authoritative State service, that the Table A translation is
// faithful end-to-end: lease/claim, intent record/update/get, stream append/read,
// checkpoint save/head, inbox enqueue/read/ack — and the director's reliability
// gate: same-run resume raises the epoch and a stale writer's fenced write is
// rejected, surfaced as the seam's typed ErrStaleEpoch.
//
// Author-side candidate evidence; the independent live-seam verification over real
// State is iv's (base 012a849). Emil name/port are NOT a gate; nothing here fakes
// State — it runs against the real stateapi + PG from cNewEnv.

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestSeamStateRunStore_RealState_TableA(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())

	// Set up a run with the real State API (the seam forwards to the same client).
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "seam-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "seam-run", json.RawMessage(`{"type":"turn","input":"go"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	rs := emil.NewStateRunStore(e.client)

	// Claim a lease through the seam (cert-only; the seam carries the minted token).
	l, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil || l.RunID == "" || l.TaskToken == "" {
		t.Fatalf("seam ClaimLease: %v lease=%+v", err, l)
	}
	if l.ThreadID != th.ID {
		t.Fatalf("seam lease thread %q != created thread %q", l.ThreadID, th.ID)
	}
	if err := rs.Heartbeat(ctx, *l, 120*time.Second); err != nil {
		t.Fatalf("seam Heartbeat: %v", err)
	}

	// Intent lifecycle (workflow.py pre-execution intent) over the REAL service.
	it, err := rs.RecordIntent(ctx, *l, "agent-1", "save_artifact", "tool", "artifact", json.RawMessage(`{"name":"r.txt"}`))
	if err != nil {
		t.Fatalf("seam RecordIntent: %v", err)
	}
	if it.Status == "" || it.Call != "save_artifact" {
		t.Fatalf("seam RecordIntent bad row: %+v", it)
	}
	// Real State enforces recorded -> dispatched -> succeeded; the seam must forward both.
	if _, err := rs.UpdateIntent(ctx, *l, "agent-1", "save_artifact", state.IntentDispatched, nil); err != nil {
		t.Fatalf("seam UpdateIntent(dispatched): %v", err)
	}
	done, err := rs.UpdateIntent(ctx, *l, "agent-1", "save_artifact", state.IntentSucceeded, json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatalf("seam UpdateIntent(succeeded): %v", err)
	}
	if done.Status != state.IntentSucceeded {
		t.Fatalf("seam intent must advance to succeeded, got %q", done.Status)
	}
	gotIntent, err := rs.GetIntent(ctx, l.TaskToken, l.RunID, "agent-1", "save_artifact")
	if err != nil || gotIntent.Status != state.IntentSucceeded {
		t.Fatalf("seam GetIntent over real State: err=%v row=%+v", err, gotIntent)
	}

	// Stream append/read.
	if _, err := rs.AppendEvent(ctx, *l, "step-0", "tool", json.RawMessage(`{"step":0}`)); err != nil {
		t.Fatalf("seam AppendEvent: %v", err)
	}
	evs, err := rs.ReadEvents(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil || len(evs) == 0 {
		t.Fatalf("seam ReadEvents over real State: err=%v n=%d", err, len(evs))
	}

	// Checkpoint save/head (namespace = space/core via the seam).
	cp, err := rs.SaveCheckpoint(ctx, *l, "cp-seam-1", "ckpt-key", 1, json.RawMessage(`{"messages":[]}`), nil)
	if err != nil || cp.ID == "" {
		t.Fatalf("seam SaveCheckpoint: %v cp=%+v", err, cp)
	}
	head, err := rs.HeadCheckpoint(ctx, l.TaskToken, l.ThreadID)
	if err != nil || head == nil || head.ID != cp.ID {
		t.Fatalf("seam HeadCheckpoint over real State: err=%v head=%+v want id=%s", err, head, cp.ID)
	}

	// Inbox enqueue/read/ack (steering/HITL inbox).
	ib, err := rs.EnqueueInbound(ctx, l.TaskToken, l.ThreadID, "steer-1", "steering", json.RawMessage(`{"msg":"focus"}`))
	if err != nil || ib.ID == "" {
		t.Fatalf("seam EnqueueInbound: %v ib=%+v", err, ib)
	}
	inbox, err := rs.ReadInbox(ctx, l.TaskToken, l.ThreadID, 0, 10)
	if err != nil || len(inbox) == 0 {
		t.Fatalf("seam ReadInbox over real State: err=%v n=%d", err, len(inbox))
	}
	if err := rs.AckInbound(ctx, *l, []string{ib.ID}); err != nil {
		t.Fatalf("seam AckInbound: %v", err)
	}
	ri, err := rs.GetRun(ctx, l.TaskToken, l.RunID)
	if err != nil || ri.ID != l.RunID {
		t.Fatalf("seam GetRun over real State: err=%v run=%+v", err, ri)
	}

	if err := rs.Release(ctx, *l, state.RunDone); err != nil {
		t.Fatalf("seam Release: %v", err)
	}
	t.Logf("seam StateRunStore Table A round-trip OK over REAL State: run=%s intent=%s stream=%d cp=%s inbox=%d", l.RunID, gotIntent.Status, len(evs), cp.ID, len(inbox))
}

// TestSeamStateRunStore_RealState_StaleWriterRejected proves the director's reliability
// gate THROUGH the seam: a crash requeues the run, a new claim resumes the SAME run at a
// HIGHER epoch, and the stale (old-epoch) writer's fenced checkpoint is rejected — and the
// seam surfaces that rejection as its typed ErrStaleEpoch (mapping real STALE_EPOCH).
func TestSeamStateRunStore_RealState_StaleWriterRejected(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "seam-cr-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "seam-cr-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)

	l1, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease w1: %v", err)
	}
	if _, err := rs.SaveCheckpoint(ctx, *l1, "cp-w1", "cr-ckpt", 1, nil, nil); err != nil {
		t.Fatalf("w1 SaveCheckpoint should succeed: %v", err)
	}

	// Crash: suspend w1 with a past deadline -> requeue for another worker.
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(ctx, l1.TaskToken, l1.RunID, "w1", l1.Epoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend w1: %v", err)
	}
	l2, err := rs.ClaimLease(ctx, "", "w2", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease w2: %v", err)
	}
	if l2.RunID != l1.RunID {
		t.Fatalf("resume must be SAME run: %s vs %s", l2.RunID, l1.RunID)
	}
	if l2.Epoch <= l1.Epoch {
		t.Fatalf("resume must raise epoch: w1=%d w2=%d", l1.Epoch, l2.Epoch)
	}

	// Old writer (w1, old epoch) replaying a fenced write through the seam is rejected
	// and MUST surface as emil.ErrStaleEpoch (real STALE_EPOCH mapped by the seam).
	_, err = rs.SaveCheckpoint(ctx, *l1, "cp-w1-late", "cr-ckpt", 2, nil, nil)
	if err == nil {
		t.Fatal("stale writer must be rejected through the seam")
	}
	if !errors.Is(err, emil.ErrStaleEpoch) {
		t.Fatalf("stale writer must map to emil.ErrStaleEpoch, got %v", err)
	}

	// New writer (w2, higher epoch) succeeds on the same run.
	if _, err := rs.SaveCheckpoint(ctx, *l2, "cp-w2", "cr-ckpt-resume", 2, nil, nil); err != nil {
		t.Fatalf("w2 SaveCheckpoint should succeed: %v", err)
	}
	t.Logf("seam crash/resume fenced over REAL State: run=%s w1_epoch=%d rejected(ErrStaleEpoch) w2_epoch=%d accepted", l1.RunID, l1.Epoch, l2.Epoch)
}
