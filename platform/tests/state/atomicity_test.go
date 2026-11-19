package stateint

import (
	"bytes"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// TestBatchIsAtomicAndSurvivesRestart verifies that a checkpoint, a stream
// append and a run-status transition commit as ONE transaction and that the
// committed result is durable across a full process crash/restart. It also
// proves two failure properties: a replayed batch (same key + same bodies) is
// idempotent and does not double-append the stream, and a batch that fails on a
// conflicting checkpoint leaves NO partial artifacts (no orphan stream event,
// no run-status drift).
func TestBatchIsAtomicAndSurvivesRestart(t *testing.T) {
	// cpPtr wraps the value-returning cp helper so a checkpoint can be placed
	// in BatchRequest.Checkpoint, which is a pointer field.
	cpPtr := func(id string, version int64, data []byte) *sdk.Checkpoint {
		c := cp(id, version, data)
		return &c
	}
	e := newEnv(t)
	lp := e.claimFirst(t, "w")
	f := lp.fence
	cpID := "cp-batch-1"
	secret := []byte{0x00, 0xde, 0xad, 0xbe, 0xef, 0x00}
	batchKey := "batch-" + contracts.NewID()
	streamKey := "evt-" + contracts.NewID()

	res, err := e.client.PutBatch(e.ctx, f, sdk.BatchRequest{
		Checkpoint: cpPtr(cpID, 1, secret),
		Key:        batchKey,
		Stream:     &sdk.StreamOp{Key: streamKey, Kind: "token", Payload: pj(map[string]int{"i": 1})},
		RunStatus:  &sdk.RunStatusOp{Status: sdk.RunRunning},
	})
	requireNoErr(t, err, "PutBatch commit")
	if !res.OK || res.Checkpoint == nil || res.StreamEvent == nil {
		t.Fatalf("batch commit result incomplete: %+v", res)
	}

	evs, err := e.client.ReadStream(e.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream after batch")
	if len(evs) != 1 || evs[0].Key != streamKey {
		t.Fatalf("stream after batch: %+v", evs)
	}
	gotRun, err := e.client.GetRun(e.ctx, lp.run.ID)
	requireNoErr(t, err, "GetRun after batch")
	if gotRun.Status != sdk.RunRunning {
		t.Fatalf("run status after batch = %s, want running", gotRun.Status)
	}
	gotCP, err := e.client.GetCheckpoint(e.ctx, lp.thread.ID, "lg", cpID)
	requireNoErr(t, err, "GetCheckpoint after batch")
	if gotCP.Version != 1 {
		t.Fatalf("checkpoint version = %d, want 1", gotCP.Version)
	}

	// Replay: same key + identical bodies must be a no-op that returns the
	// committed result, leaving the stream at a single event.
	replay, err := e.client.PutBatch(e.ctx, f, sdk.BatchRequest{
		Checkpoint: cpPtr(cpID, 1, secret),
		Key:        batchKey,
		Stream:     &sdk.StreamOp{Key: streamKey, Kind: "token", Payload: pj(map[string]int{"i": 1})},
		RunStatus:  &sdk.RunStatusOp{Status: sdk.RunRunning},
	})
	requireNoErr(t, err, "PutBatch replay")
	if !replay.OK {
		t.Fatalf("replayed batch not OK: %+v", replay)
	}
	evsAfterReplay, err := e.client.ReadStream(e.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream after replay")
	if len(evsAfterReplay) != 1 {
		t.Fatalf("replay double-appended stream: %d events", len(evsAfterReplay))
	}

	// Failing batch: a checkpoint whose id already exists under a different
	// batch key must abort, and the stream append + run-status transition it
	// carried must not be applied.
	if _, err := e.client.PutBatch(e.ctx, f, sdk.BatchRequest{
		Checkpoint: cpPtr(cpID, 99, []byte("conflicting body")),
		Key:        "batch-" + contracts.NewID(),
		Stream:     &sdk.StreamOp{Key: "ghost-" + contracts.NewID(), Kind: "token", Payload: pj(map[string]bool{"ghost": true})},
		RunStatus:  &sdk.RunStatusOp{Status: sdk.RunDone},
	}); err == nil {
		t.Fatal("expected the conflicting batch to fail")
	}

	evsAfterFail, err := e.client.ReadStream(e.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream after failed batch")
	if len(evsAfterFail) != 1 {
		t.Fatalf("failed batch leaked a stream event: %d events", len(evsAfterFail))
	}
	runAfterFail, err := e.client.GetRun(e.ctx, lp.run.ID)
	requireNoErr(t, err, "GetRun after failed batch")
	if runAfterFail.Status != sdk.RunRunning {
		t.Fatalf("failed batch drifted run status to %s", runAfterFail.Status)
	}

	// Crash/restart: a brand-new process on the same durable database must read
	// the exact committed bytes, the single stream event, and the running status
	// with no divergence between state and the ordered stream (outbox).
	e2 := e.reopen(t)
	e2.setToken(e2.signTask(t, "task-"+contracts.NewID()))
	cpBack, err := e2.client.GetCheckpoint(e2.ctx, lp.thread.ID, "lg", cpID)
	requireNoErr(t, err, "GetCheckpoint after restart")
	if cpBack.Version != 1 {
		t.Fatalf("checkpoint after restart version = %d", cpBack.Version)
	}
	part, err := e2.client.GetPart(e2.ctx, lp.thread.ID, "lg", "messages", "v-"+cpID)
	requireNoErr(t, err, "GetPart after restart")
	if !bytes.Equal(part, secret) {
		t.Fatalf("opaque checkpoint bytes diverged across restart: got %x want %x", part, secret)
	}
	evs2, err := e2.client.ReadStream(e2.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream after restart")
	if len(evs2) != 1 {
		t.Fatalf("stream diverged after restart: %d events", len(evs2))
	}
	run2, err := e2.client.GetRun(e2.ctx, lp.run.ID)
	requireNoErr(t, err, "GetRun after restart")
	if run2.Status != sdk.RunRunning {
		t.Fatalf("run status after restart = %s, want running", run2.Status)
	}
}
