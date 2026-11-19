package stateint

import (
	"bytes"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// newTaskThread starts a task, creates a thread and returns its id.
func newTaskThread(e *env, t *testing.T) sdk.Thread {
	t.Helper()
	e.setToken(e.signTask(t, "task-"+contracts.NewID()))
	ctxID := "ctx-" + contracts.NewID()
	th, err := e.client.CreateThread(e.ctx, ctxID, "k-"+contracts.NewID())
	requireNoErr(t, err, "CreateThread")
	byCtx, err := e.client.GetThreadByContext(e.ctx, ctxID)
	requireNoErr(t, err, "GetThreadByContext")
	if byCtx.ID != th.ID {
		t.Fatalf("GetThreadByContext id %q != created id %q", byCtx.ID, th.ID)
	}
	return th
}

func TestThreadRunCheckpointStreamLifecycle(t *testing.T) {
	e := newEnv(t)
	th := newTaskThread(e, t)

	run, err := e.client.CreateRun(e.ctx, th.ID, "k-"+contracts.NewID(), pj(map[string]int{"n": 1}))
	requireNoErr(t, err, "CreateRun")
	if run.Sequence != 1 || run.Status != sdk.RunPending {
		t.Fatalf("fresh run: seq=%d status=%s", run.Sequence, run.Status)
	}

	claimed, err := e.client.ClaimRun(e.ctx, "w1", 60000)
	requireNoErr(t, err, "ClaimRun")
	if claimed == nil {
		t.Fatal("expected a claimable run")
	}
	e.setToken(claimed.TaskToken)
	f := sdk.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}

	exp, err := e.client.Heartbeat(e.ctx, f, 60000)
	requireNoErr(t, err, "Heartbeat")
	if !exp.After(time.Now()) {
		t.Fatalf("heartbeat expiry %v not in the future", exp)
	}

	// Opaque, non-UTF8 bytes must round-trip byte-for-byte.
	secret := []byte{0x00, 0x01, 0xff, 0xfe, 'a', 0x00}
	saved, err := e.client.PutCheckpoint(e.ctx, f, cp("cp1", 1, secret), "ck-cp1")
	requireNoErr(t, err, "PutCheckpoint")
	if saved.ID != "cp1" || saved.Version != 1 {
		t.Fatalf("saved checkpoint: %+v", saved)
	}
	back, err := e.client.GetCheckpoint(e.ctx, th.ID, "lg", "cp1")
	requireNoErr(t, err, "GetCheckpoint")
	if back.Version != 1 {
		t.Fatalf("loaded checkpoint version %d", back.Version)
	}
	part, err := e.client.GetPart(e.ctx, th.ID, "lg", "messages", "v-cp1")
	requireNoErr(t, err, "GetPart")
	if !bytes.Equal(part, secret) {
		t.Fatalf("part bytes differ: got %v want %v", part, secret)
	}
	cps, err := e.client.ListCheckpoints(e.ctx, th.ID, "lg", 10)
	requireNoErr(t, err, "ListCheckpoints")
	if len(cps) < 1 {
		t.Fatalf("ListCheckpoints empty")
	}

	writes := []sdk.PendingWrite{{Task: "t1", Index: 0, Channel: "messages", Data: secret}}
	requireNoErr(t, e.client.PutWrites(e.ctx, f, "lg", "cp1", writes), "PutWrites")
	gw, err := e.client.GetWrites(e.ctx, th.ID, "lg", "cp1")
	requireNoErr(t, err, "GetWrites")
	if len(gw) != 1 || !bytes.Equal(gw[0].Data, secret) {
		t.Fatalf("pending writes: %+v", gw)
	}

	for i := 1; i <= 3; i++ {
		_, err := e.client.AppendStream(e.ctx, f, "sk-"+string(rune('a'+i)), "note", pj(map[string]int{"i": i}))
		requireNoErr(t, err, "AppendStream")
	}
	evs, err := e.client.ReadStream(e.ctx, th.ID, 0, 100)
	requireNoErr(t, err, "ReadStream")
	if len(evs) != 3 {
		t.Fatalf("stream length %d", len(evs))
	}
	for i := 1; i < len(evs); i++ {
		if !(evs[i-1].Sequence < evs[i].Sequence) {
			t.Fatalf("stream not ordered at %d: %d !< %d", i, evs[i-1].Sequence, evs[i].Sequence)
		}
	}

	requireNoErr(t, e.client.Release(e.ctx, f, sdk.RunDone), "Release")
	gr, err := e.client.GetRun(e.ctx, run.ID)
	requireNoErr(t, err, "GetRun after release")
	if gr.Status != sdk.RunDone {
		t.Fatalf("released run status %s", gr.Status)
	}
	none, err := e.client.ClaimRun(e.ctx, "w1", 60000)
	requireNoErr(t, err, "ClaimRun after terminal")
	if none != nil {
		t.Fatalf("expected no claimable run, got %+v", none)
	}
}

func TestFIFOOrderingPerThread(t *testing.T) {
	e := newEnv(t)
	th := newTaskThread(e, t)

	ids := make([]string, 0, 3)
	for i := 1; i <= 3; i++ {
		e.setToken(e.signTask(t, "task-run-"+contracts.NewID()))
		r, err := e.client.CreateRun(e.ctx, th.ID, "k-"+contracts.NewID(), pj(map[string]int{"n": i}))
		requireNoErr(t, err, "CreateRun")
		ids = append(ids, r.ID)
	}

	runs, err := e.client.ListRuns(e.ctx, th.ID)
	requireNoErr(t, err, "ListRuns")
	if len(runs) != 3 {
		t.Fatalf("ListRuns len %d", len(runs))
	}
	for i, r := range runs {
		if r.Sequence != int64(i+1) {
			t.Fatalf("run[%d].sequence=%d", i, r.Sequence)
		}
		if r.ID != ids[i] {
			t.Fatalf("run[%d].id=%s want %s", i, r.ID, ids[i])
		}
	}

	for i := 0; i < 3; i++ {
		claimed, err := e.client.ClaimRun(e.ctx, "w", 60000)
		requireNoErr(t, err, "FIFO claim")
		if claimed == nil {
			t.Fatalf("FIFO: expected claimable head at %d", i)
		}
		if claimed.Lease.Run.ID != ids[i] {
			t.Fatalf("FIFO violation: claimed %s want head %s", claimed.Lease.Run.ID, ids[i])
		}
		e.setToken(claimed.TaskToken)
		f := sdk.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}
		requireNoErr(t, e.client.Release(e.ctx, f, sdk.RunDone), "Release FIFO head")
	}

	tail, err := e.client.ClaimRun(e.ctx, "w", 60000)
	requireNoErr(t, err, "ClaimRun tail")
	if tail != nil {
		t.Fatalf("expected drained thread, got %+v", tail)
	}
}

func TestSuspendInboxResumeAndTakeover(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w1")
	f := lp.fence

	fut := time.Now().Add(time.Hour)
	requireNoErr(t, e.client.Suspend(e.ctx, f, "approval", &fut), "Suspend")
	gr, err := e.client.GetRun(e.ctx, lp.run.ID)
	requireNoErr(t, err, "GetRun waiting")
	if gr.Status != sdk.RunWaiting || gr.WaitFor != "approval" {
		t.Fatalf("after suspend: status=%s wait_for=%s", gr.Status, gr.WaitFor)
	}

	// A waiting run with a future deadline is not claimable.
	if c, err := e.client.ClaimRun(e.ctx, "w", 60000); err != nil || c != nil {
		t.Fatalf("waiting run should not be claimable: %+v %v", c, err)
	}

	// A non-matching event does not wake it.
	_, err = e.client.Enqueue(e.ctx, lp.thread.ID, "k-no-"+contracts.NewID(), "something-else", pj(map[string]bool{"ok": true}))
	requireNoErr(t, err, "Enqueue non-matching")
	if c, err := e.client.ClaimRun(e.ctx, "w", 60000); err != nil || c != nil {
		t.Fatalf("non-matching event must not wake run: %+v %v", c, err)
	}

	// The matching kind wakes the earliest waiting run back to pending.
	_, err = e.client.Enqueue(e.ctx, lp.thread.ID, "k-yes-"+contracts.NewID(), "approval", pj(map[string]bool{"approved": true}))
	requireNoErr(t, err, "Enqueue matching")
	gr3, err := e.client.GetRun(e.ctx, lp.run.ID)
	requireNoErr(t, err, "GetRun after wake")
	if gr3.Status != sdk.RunPending {
		t.Fatalf("wake should reset to pending, got %s", gr3.Status)
	}

	// Takeover by a different worker bumps the epoch.
	claimed, err := e.client.ClaimRun(e.ctx, "w2", 60000)
	requireNoErr(t, err, "reclaim after wake")
	if claimed == nil {
		t.Fatal("woken run must be claimable")
	}
	if claimed.Lease.Epoch <= f.Epoch {
		t.Fatalf("epoch did not grow: old=%d new=%d", f.Epoch, claimed.Lease.Epoch)
	}
	e.setToken(claimed.TaskToken)
	nf := sdk.Fence{RunID: claimed.Lease.Run.ID, Worker: claimed.Lease.Worker, Epoch: claimed.Lease.Epoch}
	_, err = e.client.PutCheckpoint(e.ctx, nf, cp("cp-resume", 1, []byte("resumed")), "ck-resume")
	requireNoErr(t, err, "PutCheckpoint on new lease")

	// The stale fence from before the takeover must be refused.
	_, err = e.client.PutCheckpoint(e.ctx, f, cp("cp-stale", 2, []byte("stale")), "ck-stale")
	mustCode(t, err, contracts.ErrStaleEpoch, "stale fence after takeover")
}

func TestIntentsLifecycleAndUnknownNoAutoReplay(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w")
	f := lp.fence
	args := pj(map[string]string{"url": "https://x"})

	it, err := e.client.RecordIntent(e.ctx, f, "node1", "call1", "http", "target-1", args)
	requireNoErr(t, err, "RecordIntent")
	if it.Status != "recorded" {
		t.Fatalf("intent status %s", it.Status)
	}
	// Idempotent re-record with the same fingerprint returns the same record.
	it2, err := e.client.RecordIntent(e.ctx, f, "node1", "call1", "http", "target-1", args)
	requireNoErr(t, err, "RecordIntent replay")
	if it2.ID != it.ID {
		t.Fatalf("intent replay produced a new record: %s vs %s", it2.ID, it.ID)
	}

	_, err = e.client.UpdateIntent(e.ctx, f, "node1", "call1", "dispatched", nil)
	requireNoErr(t, err, "UpdateIntent dispatched")
	_, err = e.client.UpdateIntent(e.ctx, f, "node1", "call1", "unknown", nil)
	requireNoErr(t, err, "UpdateIntent unknown")

	// UNKNOWN must never auto-replay to a terminal state.
	_, err = e.client.UpdateIntent(e.ctx, f, "node1", "call1", "succeeded", nil)
	if err == nil {
		t.Fatal("unknown intent must not be forceable to succeeded via UpdateIntent")
	}
	gi, err := e.client.GetIntent(e.ctx, lp.run.ID, "node1", "call1")
	requireNoErr(t, err, "GetIntent after refused update")
	if gi.Status != "unknown" {
		t.Fatalf("unknown intent status drifted to %s", gi.Status)
	}

	// Only explicit ReconcileIntent with the exact digest may close UNKNOWN.
	_, err = e.client.ReconcileIntent(e.ctx, f, "node1", "call1", "succeeded", "00000000", nil)
	if err == nil {
		t.Fatal("ReconcileIntent must reject a mismatched digest")
	}
	rec, err := e.client.ReconcileIntent(e.ctx, f, "node1", "call1", "succeeded", gi.Digest, pj(map[string]int{"code": 200}))
	requireNoErr(t, err, "ReconcileIntent with correct digest")
	if rec.Status != "succeeded" {
		t.Fatalf("reconciled status %s", rec.Status)
	}
}

func TestCheckpointIdempotencyAndKeyReuse(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w")
	f := lp.fence
	data := []byte("checkpoint-bytes")

	_, err := e.client.PutCheckpoint(e.ctx, f, cp("cp1", 1, data), "ck1")
	requireNoErr(t, err, "PutCheckpoint")
	// Same key + same body is an idempotent replay.
	replay, err := e.client.PutCheckpoint(e.ctx, f, cp("cp1", 1, data), "ck1")
	requireNoErr(t, err, "PutCheckpoint replay")
	if replay.ID != "cp1" {
		t.Fatalf("replay id %s", replay.ID)
	}
	// Same key + different body: KEY_REUSE surfaces as INVALID over the wire.
	_, err = e.client.PutCheckpoint(e.ctx, f, cp("cp1", 2, []byte("different")), "ck1")
	mustCode(t, err, contracts.ErrInvalid, "checkpoint key reuse")
	// Existing id + different body + new key: CONFLICT.
	_, err = e.client.PutCheckpoint(e.ctx, f, cp("cp1", 3, []byte("third")), "ck-new")
	mustCode(t, err, contracts.ErrConflict, "checkpoint id conflict")

}
