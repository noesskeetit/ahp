package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// step_budget_statedness_test.go — Space must carry the CALLER's step budget, not its own
// default, all the way to Core.
//
// internal/core/run_binding_budget_test.go pins that a tool sees the stated budget on its
// RunBinding. That is only possible if the number reaching Core still says who stated it,
// and it did not: AcceptTurn replaced an absent max_steps with 64 before persisting the run,
// and a cold rehydrate filled 64 again before reading the durable payload. By the time Core
// saw the request, "the caller asked for 64" and "nobody said anything" were the same bytes,
// so a run delegating to a SECOND DEPLOYMENT could only repeat a will it had invented -
// step_budget_source=caller on a number the caller never named, which is the exact
// substitution internal/gateway/budget.go exists to prevent.
//
// The bound itself does not change: Core bounds the loop with the same 64 steps
// (internal/core/runtime.go), so an unstated budget still runs 64 steps. What changes is that
// the platform stops claiming the caller chose that number.

func dispatchAndCapture(t *testing.T, coord *space.Coordinator, captured <-chan *space.CoreRequest, scope contracts.Scope) *space.CoreRequest {
	t.Helper()
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case req := <-captured:
		return req
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch never reached Core")
	}
	return nil
}

func budgetCoord(sc *space.StateClient, workerID string) (*space.Coordinator, chan *space.CoreRequest) {
	captured := make(chan *space.CoreRequest, 1)
	return space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{capture: captured}, WorkerID: workerID,
	}), captured
}

// TestStatedStepBudgetReachesCoreExactly: a caller that names a number gets that number.
func TestStatedStepBudgetReachesCoreExactly(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	coord, captured := budgetCoord(sc, "w1")
	if _, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:      "budget-stated-" + contracts.NewID(),
		Input:    json.RawMessage(`{"role":"user","content":"hi"}`),
		MaxSteps: 5,
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	req := dispatchAndCapture(t, coord, captured, scope)
	if req.MaxSteps != 5 {
		t.Fatalf("Core got MaxSteps %d, want the 5 the caller stated", req.MaxSteps)
	}
}

// TestUnstatedStepBudgetReachesCoreAsUnstated is the defect: an absent max_steps must arrive
// as "nobody stated one" (0), not as the ingress process default 64.
func TestUnstatedStepBudgetReachesCoreAsUnstated(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	coord, captured := budgetCoord(sc, "w1")
	if _, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:   "budget-unstated-" + contracts.NewID(),
		Input: json.RawMessage(`{"role":"user","content":"hi"}`),
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	req := dispatchAndCapture(t, coord, captured, scope)
	if req.MaxSteps != 0 {
		t.Fatalf("Core got MaxSteps %d for a turn whose caller stated nothing; the durable run "+
			"payload has to keep an unstated budget unstated, because Core's own 64-step "+
			"fallback is not the caller's number and a second deployment would be told the "+
			"caller chose it", req.MaxSteps)
	}
}

// TestUnstatedStepBudgetStaysUnstatedThroughColdRestart is the resume leg, and the reason
// statedness has to be durable rather than recomputed: a delegating run is exactly the run
// that outlives its first process, and the cold path filled 64 a second time.
func TestUnstatedStepBudgetStaysUnstatedThroughColdRestart(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	// Process 1: accept with no stated budget, then die before any checkpoint exists.
	coord1, _ := budgetCoord(sc, "w1")
	if _, err := coord1.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:   "budget-cold-unstated-" + contracts.NewID(),
		Input: json.RawMessage(`{"role":"user","content":"hi"}`),
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Process 2 (post-restart): a fresh coordinator over the SAME durable State.
	coord2, captured := budgetCoord(sc, "w2")
	req := dispatchAndCapture(t, coord2, captured, scope)
	if req.MaxSteps != 0 {
		t.Fatalf("cold-resumed Core got MaxSteps %d for a turn whose caller stated nothing; a "+
			"restart must not manufacture a caller will", req.MaxSteps)
	}
}

// TestStatedStepBudgetSurvivesColdRestart is the other half of the resume leg: a stated
// number must still be the SAME stated number after a restart, or the budget the user set
// silently becomes the deployment's after one crash.
func TestStatedStepBudgetSurvivesColdRestart(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	coord1, _ := budgetCoord(sc, "w1")
	if _, err := coord1.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:      "budget-cold-stated-" + contracts.NewID(),
		Input:    json.RawMessage(`{"role":"user","content":"hi"}`),
		MaxSteps: 6,
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	coord2, captured := budgetCoord(sc, "w2")
	req := dispatchAndCapture(t, coord2, captured, scope)
	if req.MaxSteps != 6 {
		t.Fatalf("cold-resumed Core got MaxSteps %d, want the 6 the caller stated", req.MaxSteps)
	}
}
