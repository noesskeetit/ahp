package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func newHitlCoord(t *testing.T) (*space.Coordinator, *mockState, contracts.Scope, string) {
	return newHitlCoordWithCore(t, &stubCore{})
}

// newHitlCoordWithCore is the same fixture around a chosen Core, so a test can model an
// EXECUTOR that is inside the run - and therefore holds its lease - instead of a process
// that never claimed it. Only a claimed run has a fence worth presenting.
func newHitlCoordWithCore(t *testing.T, core space.CoreRunner) (*space.Coordinator, *mockState, contracts.Scope, string) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: core, WorkerID: "w1"})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	acc, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "hitl-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"needs approval"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	return coord, ms, scope, acc.TurnID
}

// TestSubmitHITLSuspendsExecutorRun: when SubmitHITL is called from the execution
// path with a live run fence (non-empty worker), the coordinator must durably
// park the run in State (PDF "Suspend на interrupt()") so it stops advancing and
// is later woken by the approval answer in the inbox (State wakes waiting->pending
// on a matching inbox event; verified by the real-state bridge test).
func TestSubmitHITLSuspendsExecutorRun(t *testing.T) {
	// A HITL gate interrupts a run that is EXECUTING, so the executor first takes the run
	// and stays inside Core with it. SubmitHITL then parks it under the lease State granted
	// to THIS worker: an epoch it was not given is refused STALE_EPOCH and parks nothing,
	// which is exactly why the fence here is taken from the claim and not written by hand.
	coord, ms, scope, turnID := newHitlCoordWithCore(t, &stubCore{blockForever: true})
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, coord, turnID, "w1")
	if _, err := coord.SubmitHITL(context.Background(), "token", space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"q":"Proceed?"}`),
	}, turnID, "w1", held.LeaseEpoch); err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	ms.mu.Lock()
	status := ms.runs[turnID].Status
	ms.mu.Unlock()
	if status != "waiting" {
		t.Fatalf("executor-path SubmitHITL did not park run: status=%q want waiting", status)
	}
}

// TestSubmitHITLHumanPathDoesNotPark: the human-facing HTTP SubmitHITL presents
// no run fence (empty worker) — it records the gate but must NOT attempt to park
// the run (it does not hold the lease; State would reject with STALE_EPOCH). This
// keeps the human request path compatible while the executor owns run parking.
func TestSubmitHITLHumanPathDoesNotPark(t *testing.T) {
	coord, ms, _, turnID := newHitlCoord(t)
	if _, err := coord.SubmitHITL(context.Background(), "token", space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"q":"Proceed?"}`),
	}, turnID, "", 1); err != nil {
		t.Fatalf("human-path SubmitHITL: %v", err)
	}
	ms.mu.Lock()
	status := ms.runs[turnID].Status
	ms.mu.Unlock()
	if status == "waiting" {
		t.Fatalf("human-path SubmitHITL must not park the run (no fence); got waiting")
	}
}

// TestSubmitHITLUnknownTurnFailsClosed: SubmitHITL for a turn the coordinator does
// not track returns NOT_FOUND rather than dereferencing a nil turn.
func TestSubmitHITLUnknownTurnFailsClosed(t *testing.T) {
	coord, _, _, _ := newHitlCoord(t)
	_, err := coord.SubmitHITL(context.Background(), "token", space.HITLSubmitRequest{
		TurnID: "no-such-turn", Kind: "approval", Prompt: json.RawMessage(`{}`),
	}, "no-such-run", "w1", 1)
	if err == nil {
		t.Fatal("SubmitHITL for unknown turn must fail closed")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("SubmitHITL unknown turn code=%v want NOT_FOUND", contracts.Code(err))
	}
}
