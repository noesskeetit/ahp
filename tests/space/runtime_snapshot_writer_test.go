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

// The HTTP SaveCheckpoint face is NOT the only snapshot writer and, in the composed
// stand, it is not even the LEGITIMATE one: the run lease is held by the execution
// process, which has no SaveCheckpoint HTTP face. These tests pin the real writer -
// the coordinator's runtime, writing at the durable step boundary with the exact
// worker/epoch State granted to it - and the recovery that writer makes possible.

// durableSnapshot is the test's own projection of a State checkpoint row, so the
// assertions can be written against a named type (State's client returns an
// unexported response struct).
type durableSnapshot struct {
	ID        string
	Namespace string
	Version   int64
	Metadata  json.RawMessage
}

// awaitCheckpoints blocks until the thread has at least `want` snapshots in Space's
// own namespace (or the deadline). Polling a durable read - not a wall-clock sleep -
// keeps the test deterministic and fast.
func awaitCheckpoints(t *testing.T, sc *space.StateClient, tok, threadID string, want int) []durableSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cps, err := sc.ListCheckpoints(context.Background(), tok, threadID, space.CheckpointNamespace, 10)
		if err != nil {
			t.Fatalf("ListCheckpoints: %v", err)
		}
		if len(cps) >= want {
			out := make([]durableSnapshot, 0, len(cps))
			for _, cp := range cps {
				out = append(out, durableSnapshot{ID: cp.ID, Namespace: cp.Namespace, Version: cp.Version, Metadata: cp.Metadata})
			}
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d checkpoints after 5s, want >= %d", len(cps), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRuntimeWriterPersistsSnapshotUnderLease is the positive leg the finding was
// missing: after a real turn executes, the runtime itself leaves a snapshot in State,
// fenced by the GRANTED lease. Before this, the only writer was the ingress HTTP face
// and a completed journey left ListCheckpoints empty.
func TestRuntimeWriterPersistsSnapshotUnderLease(t *testing.T) {
	spy := newSpyState(t)
	sc := space.NewStateClient(spy.srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	// Ingress accepts the turn and never executes it, so it holds no lease and writes
	// nothing.
	ingress := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w-ingress",
	})
	accepted, err := ingress.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "rtsnap-" + contracts.NewID(), ContextID: "ctx-rtsnap-" + contracts.NewID(),
		Input: json.RawMessage(`"hello runtime snapshot"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if n := len(spy.calls()); n != 0 {
		t.Fatalf("an accepting-but-not-executing process wrote %d snapshots, want 0", n)
	}

	// The execution process claims the run and runs a Core that produces steps; each
	// step boundary must yield a snapshot written under THIS process's granted lease.
	owner := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{stepsBeforeDone: 2}, WorkerID: "w-exec",
	})
	if err := owner.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn(exec): %v", err)
	}
	held := waitLeaseHeld(t, owner, accepted.TurnID, "w-exec")

	cps := awaitCheckpoints(t, sc, "token", accepted.ThreadID, 1)
	for _, cp := range cps {
		if cp.Namespace != space.CheckpointNamespace {
			t.Fatalf("snapshot namespace = %q, want %q", cp.Namespace, space.CheckpointNamespace)
		}
	}
	// The snapshot must actually carry the conversation (input present), not an empty shell.
	if !strings.Contains(string(cps[0].Metadata), "hello runtime snapshot") {
		t.Fatalf("snapshot metadata does not carry the turn input: %s", cps[0].Metadata)
	}

	// Every snapshot the runtime wrote is fenced by the GRANTED lease - the executor's
	// worker and the epoch State issued it - never the coordinator default.
	calls := spy.calls()
	if len(calls) < 1 {
		t.Fatalf("runtime wrote no PutCheckpoint, want >=1")
	}
	for _, c := range calls {
		if c.fence.Worker != "w-exec" || c.fence.Epoch != held.LeaseEpoch || c.fence.RunID != accepted.TurnID {
			t.Fatalf("runtime snapshot fence = %+v, want run %s worker w-exec epoch %d (the granted lease)",
				c.fence, accepted.TurnID, held.LeaseEpoch)
		}
	}
}

// stepThenBlockCore emits `steps` model steps (so the runtime writer produces snapshots)
// and then blocks, simulating a worker that is executing and then dies before finishing.
type stepThenBlockCore struct {
	steps   int
	release chan struct{}
}

func (c *stepThenBlockCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	for i := 0; i < c.steps; i++ {
		select {
		case <-ctx.Done():
			results <- space.StepResult{StepType: space.StepError, Done: true}
			return ctx.Err()
		default:
		}
		results <- space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`"partial work"`)}
	}
	select {
	case <-c.release:
	case <-ctx.Done():
		results <- space.StepResult{StepType: space.StepError, Done: true}
		return ctx.Err()
	}
	results <- space.StepResult{StepType: space.StepDone, Done: true}
	return nil
}

// TestRuntimeSnapshotResumesColdDispatch proves the runtime snapshot is not decorative:
// when the executing worker dies mid-run, a fresh worker cold-resumes from the snapshot
// the runtime wrote - it attaches a checkpoint and the snapshot's messages to Core,
// instead of replaying the turn from the raw input.
func TestRuntimeSnapshotResumesColdDispatch(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	release := make(chan struct{})
	coord1 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stepThenBlockCore{steps: 1, release: release}, WorkerID: "w1",
	})
	accept, err := coord1.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "rtresume-" + contracts.NewID(), ContextID: "ctx-rtresume-" + contracts.NewID(),
		Input: json.RawMessage(`"resume me from the runtime snapshot"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord1.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	waitLeaseHeld(t, coord1, accept.TurnID, "w1")
	// Wait until the runtime snapshot is durable - this is the state the next worker resumes from.
	awaitCheckpoints(t, sc, "token", accept.ThreadID, 1)

	// w1 dies before finishing: no Release ran, so a takeover sees the run back in the
	// queue with no lease (exactly a lapsed lease in real State).
	ms.crashRun(accept.TurnID)

	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{capture: captured}, WorkerID: "w2",
	})
	if err := coord2.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("cold DispatchTurn: %v", err)
	}

	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("cold dispatch did not reach Core")
	}
	if req.Checkpoint == nil {
		t.Fatal("takeover did NOT resume from the runtime-written checkpoint (req.Checkpoint is nil)")
	}
	if !strings.Contains(string(req.Messages), "resume me from the runtime snapshot") {
		t.Fatalf("resumed messages did not come from the snapshot: %s", req.Messages)
	}
	close(release) // release w1's now-stale execution at the very end
}
