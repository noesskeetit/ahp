package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// D14: Steer must not claim a status the durable layer has not accepted.
//
// By the platform's own contract steering records an inbox event and NEVER parks the run
// (internal/space/turn_history.go:154 - a cold read of a steered turn says `running`). The
// warm Steer path, however, flipped the turn's in-memory status to `awaiting_steering`
// BEFORE the durable Enqueue (coordinator.go), and a refused Enqueue - the exact transport
// timeout the acceptance stand lives on (evidence 132: PutCheckpoint/AppendStream "context
// deadline exceeded") - returned an error while leaving the turn mislabeled. GetTurn on this
// worker then reports a status no durable row backs, for the rest of the run's life. A refusal
// must mutate nothing, exactly as Recover was fixed to (D15).
func TestSteerRefusalDoesNotChangeTurnStatus(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "test-worker",
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "steer-fail-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"steer"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	setTurnStatus(coord, accept.TurnID, space.TurnRunning)

	// Every steering enqueue now dies with a transient transport failure and writes nothing.
	ms.mu.Lock()
	ms.failEnqueue = func(op, key, kind string) bool { return kind == "steering" }
	ms.mu.Unlock()

	if err := coord.Steer(context.Background(), "token", space.SteerRequest{
		TurnID: accept.TurnID, Instruction: "use a different approach",
	}); err == nil {
		t.Fatal("a refused steering enqueue must return an error")
	}

	// The refusal changed nothing: the run is still running, not parked on a gate nothing asked for.
	snap, ok := coord.TurnSnapshot(accept.TurnID)
	if !ok {
		t.Fatal("turn missing after a failed steer")
	}
	if snap.Status != space.TurnRunning {
		t.Fatalf("a failed Steer left the turn at %q; a refusal must leave it running because the "+
			"durable enqueue never happened and no steering row exists to back a steering status", snap.Status)
	}
}

// Control: a steering instruction the durable inbox DID accept is still recorded and still marks
// the turn steering. This pins that the fix is not "stop recording steering" or "never set the
// status" - the durable write and the status claim must both happen, in that order.
func TestSteerAfterAcceptedEnqueueRecordsAndMarksTurn(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "test-worker",
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "steer-ok-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"go"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	setTurnStatus(coord, accept.TurnID, space.TurnRunning)

	if err := coord.Steer(context.Background(), "token", space.SteerRequest{
		TurnID: accept.TurnID, Instruction: "turn left",
	}); err != nil {
		t.Fatalf("Steer: %v", err)
	}

	ms.mu.Lock()
	var found bool
	for _, it := range ms.inbox {
		if it.Kind == "steering" {
			found = true
		}
	}
	ms.mu.Unlock()
	if !found {
		t.Fatal("an accepted steer must be durably enqueued")
	}
	if snap, _ := coord.TurnSnapshot(accept.TurnID); snap.Status != space.TurnSteering {
		t.Fatalf("an accepted steer must mark the turn steering, got %q", snap.Status)
	}
}
