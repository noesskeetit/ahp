package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_cold_status_test.go — a HITL gate has to keep its name across a restart.
//
// The warm answer is unambiguous: SubmitHITL sets the turn to `awaiting_hitl`
// (internal/space/coordinator.go, TurnAwaiting in internal/space/types.go), and that is the status
// the published lifecycle in docs/space.md promises a reader for a turn parked on an approval.
// The same fact is also durable, in State's own vocabulary: the run row goes to `waiting` with
// wait_for="hitl_response", which is what makes an approval answer in the inbox wake it
// (internal/state/events.go).
//
// The cold projection read only the first half. turnFromRun carried the run status through
// verbatim and justified it by claiming "Space and State share this vocabulary for every status a
// turn can be read in" - which is true for pending/running/done/failed/canceled and FALSE for a
// parked turn, because Space's name for that state is `awaiting_hitl`. So the same turn answered
// two different statuses depending on whether the process serving the read still remembered it:
// `awaiting_hitl` before a runner restart, `waiting` after one. `waiting` is not in Space's turn
// vocabulary at all, so an approval card keyed on the published status simply stops appearing
// after a restart, while GetTurn still looks "healthy" - the same projection-vs-projection split
// this lane has been closing for cancel and for the failure cause, arriving from the other side.
//
// The fix reads the reason instead of renaming the state: a `waiting` row is only called
// `awaiting_hitl` when its own wait_for says the wait IS the HITL answer (the one kind Space
// suspends for). A run parked on anything else keeps `waiting`, because Space did not create that
// wait and would be inventing a gate the platform never asked for.

// coldHitlFixture parks an EXECUTING turn on a HITL gate over the State double and hands back a
// SECOND coordinator over the same durable State: the reader that no longer remembers the turn.
func coldHitlFixture(t *testing.T, waitFor string) (*space.Coordinator, *space.Coordinator, *mockState, contracts.Scope, string) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	live := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil), Core: &stubCore{blockForever: true}, WorkerID: "w1"})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	acc, err := live.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "hitl-cold-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"needs approval"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := live.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, live, acc.TurnID, "w1")
	if waitFor == "" {
		// the real execution path: SubmitHITL parks the run under its own fence.
		if _, err := live.SubmitHITL(context.Background(), "token", space.HITLSubmitRequest{
			TurnID: acc.TurnID, Kind: "approval", Prompt: json.RawMessage(`{"q":"Proceed?"}`),
		}, acc.TurnID, "w1", held.LeaseEpoch); err != nil {
			t.Fatalf("SubmitHITL: %v", err)
		}
	} else {
		// a durable fact Space did not create: park the row on someone else's wait target,
		// which is the case the projection must NOT rename.
		ms.mu.Lock()
		ms.runs[acc.TurnID].Status = "waiting"
		ms.runs[acc.TurnID].WaitFor = waitFor
		ms.mu.Unlock()
	}
	cold := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil), Core: &stubCore{}, WorkerID: "w-cold"})
	return live, cold, ms, scope, acc.TurnID
}

// TestColdReadNamesTheHITLGateTheWayTheLiveRunnerDoes is the defect: the reader that lost the turn
// out of memory must say what the reader that still holds it says.
func TestColdReadNamesTheHITLGateTheWayTheLiveRunnerDoes(t *testing.T) {
	live, cold, ms, scope, turnID := coldHitlFixture(t, "")

	warm, ok := live.TurnSnapshot(turnID)
	if !ok || warm.Status != space.TurnAwaiting {
		t.Fatalf("warm read = %v (ok=%v), want %q - the fixture above stopped describing a HITL gate",
			warm.Status, ok, space.TurnAwaiting)
	}
	// The durable plane must say the same thing the fix is going to read; if this drifts, the
	// mapping below would be renaming something that is no longer a HITL wait.
	ms.mu.Lock()
	durableStatus, durableWaitFor := ms.runs[turnID].Status, ms.runs[turnID].WaitFor
	ms.mu.Unlock()
	if durableStatus != "waiting" || durableWaitFor != "hitl_response" {
		t.Fatalf("durable run row = (%q, wait_for=%q), want (\"waiting\", \"hitl_response\")",
			durableStatus, durableWaitFor)
	}

	coldTurn, err := cold.HydrateTurnFromState(context.Background(), "token", turnID, scope)
	if err != nil {
		t.Fatalf("cold HydrateTurnFromState: %v", err)
	}
	if coldTurn.Status != warm.Status {
		t.Fatalf("one turn, two readers, two statuses: warm %q, cold %q. A reader that only has State "+
			"answers with a status Space never published (%q is State's run vocabulary), so an approval "+
			"UI keyed on the published lifecycle loses the gate exactly when the runner restarts.",
			warm.Status, coldTurn.Status, coldTurn.Status)
	}
	if coldTurn.Status != space.TurnAwaiting {
		t.Fatalf("cold read = %q, want %q", coldTurn.Status, space.TurnAwaiting)
	}
}

// TestColdReadKeepsAWaitSpaceDidNotCreateVerbatim is the control that keeps the mapping honest: a
// parked run whose wait target is not the HITL answer is NOT a gate Space asked for, so the cold
// read must report the durable word rather than a friendlier invented one.
func TestColdReadKeepsAWaitSpaceDidNotCreateVerbatim(t *testing.T) {
	_, cold, _, scope, turnID := coldHitlFixture(t, "external_event")
	coldTurn, err := cold.HydrateTurnFromState(context.Background(), "token", turnID, scope)
	if err != nil {
		t.Fatalf("cold HydrateTurnFromState: %v", err)
	}
	if coldTurn.Status != space.TurnStatus("waiting") {
		t.Fatalf("a run parked on wait_for=external_event was renamed to %q: Space may report the wait "+
			"State recorded, not a HITL gate nobody submitted", coldTurn.Status)
	}
}

// TestColdReadKeepsOtherStatusesVerbatim guards the size of the change: the projection may rename
// exactly one durable state and nothing else, so a rebuilt thread still reads pending/running and
// its terminal turns still read done/failed/canceled.
func TestColdReadKeepsOtherStatusesVerbatim(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	want := map[string]space.TurnStatus{"pending": space.TurnPending, "running": space.TurnRunning,
		"done": space.TurnDone, "failed": space.TurnFailed, "canceled": space.TurnCanceled}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil), Core: &stubCore{}, WorkerID: "w-map"})
	for status, expected := range want {
		turnID := "turn-" + status
		ms.mu.Lock()
		threadID := "thread-" + status
		ms.runs[turnID] = &runRecord{ID: turnID, ThreadID: threadID, Status: status,
			Sequence: 1, Payload: json.RawMessage(`{"type":"turn","input":"hi","max_steps":3}`)}
		ms.mu.Unlock()
		got, err := coord.HydrateTurnFromState(context.Background(), "token", turnID, scope)
		if err != nil {
			t.Fatalf("hydrate run in status %q: %v", status, err)
		}
		if got.Status != expected {
			t.Fatalf("durable run status %q projected as %q, want %q: only the parked state has a Space name",
				status, got.Status, expected)
		}
	}
}
