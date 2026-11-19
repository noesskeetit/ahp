package space_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// claimOneRun accepts a fresh turn via the state client, creates its run, and
// claims it so the thread's fencing epoch advances to 2. It returns the run id
// and the post-claim epoch. This mirrors what Coordinator.DispatchTurn does at
// the wire level, letting these tests exercise State's takeover and privacy
// guarantees directly through Space's published client.
func claimOneRun(t *testing.T, sc *space.StateClient, contextID string) (runID, threadID string, epoch int64) {
	t.Helper()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	th, err := sc.CreateThread(context.Background(), "token", scope, contextID, contextID+":k")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := sc.CreateRun(context.Background(), "token", th.ID, contextID+":run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := sc.ClaimRun(context.Background(), "token", "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim.Lease == nil {
		t.Fatal("expected a lease after claiming the only pending run")
	}
	if claim.Lease.Run.ID != run.ID {
		t.Fatalf("claim handed wrong run: got %s want %s", claim.Lease.Run.ID, run.ID)
	}
	return run.ID, claim.Lease.Thread.ID, claim.Lease.Epoch
}

// TestStaleEpochFencingRejectsCheckpoint verifies takeover safety: once a run
// is claimed the thread epoch advances, and a checkpoint written with the
// pre-takeover (stale) epoch is rejected with STALE_EPOCH. The same write at
// the current epoch must succeed, proving the rejection is specifically about
// the fence and not the payload.
func TestStaleEpochFencingRejectsCheckpoint(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	runID, _, epoch := claimOneRun(t, sc, "stale-cp-"+contracts.NewID())
	if epoch != 2 {
		t.Fatalf("expected post-claim epoch 2, got %d", epoch)
	}

	// A stale writer (still holding epoch 1 from before takeover) is rejected.
	stale := space.Fence{RunID: runID, Worker: "worker-1", Epoch: 1}
	_, err := sc.PutCheckpoint(context.Background(), "token", stale, space.CheckpointNamespace,
		contracts.NewID(), "", "ckpt", 1, json.RawMessage(`{}`), nil)
	if err == nil || !strings.Contains(err.Error(), "STALE_EPOCH") {
		t.Fatalf("expected STALE_EPOCH for stale-epoch checkpoint, got %v", err)
	}

	// The current lease holder (epoch 2) writes successfully: the rejection was
	// specifically about the fence, not the operation.
	current := space.Fence{RunID: runID, Worker: "worker-1", Epoch: epoch}
	if _, err := sc.PutCheckpoint(context.Background(), "token", current, space.CheckpointNamespace,
		contracts.NewID(), "", "ckpt", 1, json.RawMessage(`{}`), nil); err != nil {
		t.Fatalf("current-epoch checkpoint should succeed, got %v", err)
	}
}

// TestStaleEpochFencingRejectsStream verifies the same fencing guarantee on the
// append-only stream: a stale-epoch append is rejected, the current epoch is
// accepted.
func TestStaleEpochFencingRejectsStream(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	runID, threadID, epoch := claimOneRun(t, sc, "stale-stream-"+contracts.NewID())

	if _, err := sc.AppendStream(context.Background(), "token", "", runID, "worker-1", 1,
		"step", "model", json.RawMessage(`{"t":"stale"}`)); err == nil || !strings.Contains(err.Error(), "STALE_EPOCH") {
		t.Fatalf("expected STALE_EPOCH for stale stream append, got %v", err)
	}

	if _, err := sc.AppendStream(context.Background(), "token", threadID, runID, "worker-1", epoch,
		"step", "model", json.RawMessage(`{"t":"current"}`)); err != nil {
		t.Fatalf("current-epoch append should succeed, got %v", err)
	}
}

// TestPrivateContextDoesNotLeakAcrossThreads verifies that a thread's
// checkpoints and stream events are invisible when the caller is scoped to a
// different thread. This is the wire-level privacy guarantee: turn A's private
// context never surfaces in turn B, even though both share one State backend.
func TestPrivateContextDoesNotLeakAcrossThreads(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	// Thread A carries a checkpoint and a stream event.
	runA, threadA, epochA := claimOneRun(t, sc, "priv-a-"+contracts.NewID())
	fenceA := space.Fence{RunID: runA, Worker: "worker-1", Epoch: epochA}
	if _, err := sc.PutCheckpoint(context.Background(), "token", fenceA, space.CheckpointNamespace,
		contracts.NewID(), "", "ckpt", 1, json.RawMessage(`{"secret":"turn-a-private"}`), nil); err != nil {
		t.Fatalf("checkpoint A: %v", err)
	}
	if _, err := sc.AppendStream(context.Background(), "token", threadA, runA, "worker-1", epochA,
		"step", "model", json.RawMessage(`{"secret":"turn-a-stream"}`)); err != nil {
		t.Fatalf("append stream A: %v", err)
	}

	// Positive control: scoped to A, both are visible.
	cpsA, err := sc.ListCheckpoints(context.Background(), "token", threadA, space.CheckpointNamespace, 10)
	if err != nil || len(cpsA) != 1 {
		t.Fatalf("expected 1 checkpoint visible in thread A, got %d (err %v)", len(cpsA), err)
	}
	streamA, err := sc.ReadStream(context.Background(), "token", threadA, 0, 100)
	if err != nil || len(streamA) != 1 {
		t.Fatalf("expected 1 stream event in thread A, got %d (err %v)", len(streamA), err)
	}

	// Thread B is an unrelated turn in the same State instance.
	_, threadB, _ := claimOneRun(t, sc, "priv-b-"+contracts.NewID())
	if threadB == threadA {
		t.Fatal("test setup error: threads must be distinct")
	}

	cpsB, err := sc.ListCheckpoints(context.Background(), "token", threadB, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("list checkpoints B: %v", err)
	}
	if len(cpsB) != 0 {
		t.Fatalf("private-context leak: thread B sees %d of A's checkpoints", len(cpsB))
	}
	streamB, err := sc.ReadStream(context.Background(), "token", threadB, 0, 100)
	if err != nil {
		t.Fatalf("read stream B: %v", err)
	}
	if len(streamB) != 0 {
		t.Fatalf("private-context leak: thread B sees %d of A's stream events", len(streamB))
	}
}
