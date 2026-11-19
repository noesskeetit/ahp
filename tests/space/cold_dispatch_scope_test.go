package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestColdDispatchBindsLeaseScopeNotCaller is the offline (unscoped-double) leg of the
// restart/private-context requirement. It pins the DEFECT FIX: a cold-rehydrated turn is
// bound to the run's durable LEASE scope, NEVER to the scope of whoever triggered the
// dispatch. This is the anti-hijack invariant — subject B triggering DispatchTurn must not
// make B the owner of A's run.
//
// The in-memory State double is deliberately deployment-wide and returns NO lease.scope (a
// real State returns the run's owner scope on ClaimRun). With the fix the cold turn therefore
// binds the empty lease scope rather than the caller's, so NO caller (including the trigger
// itself) is granted ownership from the dispatch call. The positive owner leg — where a real
// lease.scope restores the legitimate owner after a restart — is proven on real PostgreSQL in
// TestColdDispatchOwnerRestoresOnRealState (state_integration), not against this double.
func TestColdDispatchBindsLeaseScopeNotCaller(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	// Process 1: accept a turn (run stays Pending), then "die" (drop coordinator).
	coord1 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w1"})
	acc, err := coord1.AcceptTurn(context.Background(), "token", owner, space.AcceptTurnRequest{
		Key:       "cold-scope-" + contracts.NewID(),
		ContextID: "ctx-cold-scope",
		Input:     json.RawMessage(`{"role":"user","content":"hello"}`),
		MaxSteps:  2,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Process 2 (post-restart): fresh coordinator (empty in-memory turn table) over the SAME
	// durable State, behind per-subject auth. A DIFFERENT subject (eve) triggers the dispatch.
	coord2 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w2"})
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord2, Auth: authPerSubject})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)

	// Eve cold-dispatches Ada's still-pending run (State double claim is deployment-wide).
	if code, body := postSpaceAs(t, hs.URL, "DispatchTurn", nil, "user-eve"); code != http.StatusOK {
		t.Fatalf("cold DispatchTurn(eve) = %d, want 200 (body=%s)", code, body)
	}

	// The rehydrated turn MUST NOT have adopted eve's scope: the fix binds the lease scope,
	// not the dispatch caller. (The double carries no lease.scope, so the bound scope has no
	// Subject — and crucially is not eve.)
	snap, ok := coord2.TurnSnapshot(acc.TurnID)
	if !ok {
		t.Fatal("cold dispatch did not rehydrate the turn")
	}
	if snap.Scope.Subject == "user-eve" {
		t.Fatalf("cold dispatch must NOT bind the turn to the triggering caller (hijack): %+v", snap.Scope)
	}
	// And eve cannot read a turn they do not own (no caller-scope self-grant).
	if code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": acc.TurnID}, "user-eve"); code != http.StatusNotFound {
		t.Fatalf("trigger-caller GetTurn = %d, want 404 (caller scope must not become ownership; body=%s)", code, body)
	}
}

// TestColdDispatchRestoresInputAndMaxSteps covers the sibling field loss on the
// cold-rehydrate path (acceptance #2 bounded steps + #5 restart): a turn created
// with a specific input and step budget, restarted BEFORE any checkpoint exists,
// must hand Core the ORIGINAL input and bounded-step limit. Without restoring
// them from the durable run payload, Core would run from an empty input with the
// default 64-step budget instead of the real turn.
func TestColdDispatchRestoresInputAndMaxSteps(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	origInput := json.RawMessage(`{"role":"user","content":"real-input"}`)

	// Process 1: accept with max_steps=3, no checkpoint saved, then die.
	coord1 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w1"})
	_, err := coord1.AcceptTurn(context.Background(), "token", owner, space.AcceptTurnRequest{
		Key:       "cold-restore-" + contracts.NewID(),
		ContextID: "ctx-cold-restore",
		Input:     origInput,
		MaxSteps:  3,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Process 2 (post-restart): fresh coordinator, capture what Core is handed.
	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{capture: captured}, WorkerID: "w2"})
	if err := coord2.DispatchTurn(context.Background(), "token", owner); err != nil {
		t.Fatalf("cold DispatchTurn: %v", err)
	}

	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("cold dispatch never reached Core")
	}
	if req.MaxSteps != 3 {
		t.Fatalf("cold-resumed MaxSteps = %d, want 3 (bounded-step budget lost on restart)", req.MaxSteps)
	}
	var gotIn, wantIn any
	if err := json.Unmarshal(req.Input, &gotIn); err != nil {
		t.Fatalf("unmarshal captured input %q: %v", req.Input, err)
	}
	if err := json.Unmarshal(origInput, &wantIn); err != nil {
		t.Fatalf("unmarshal orig input: %v", err)
	}
	if !reflect.DeepEqual(gotIn, wantIn) {
		t.Fatalf("cold-resumed input = %s, want %s (turn input lost on restart)", req.Input, origInput)
	}
}
