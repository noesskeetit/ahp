//go:build state_integration

package space_test

import (
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestTurnHistoryRealStateAfterRestart puts the durable turn history in front of the REAL
// State server over real PostgreSQL, because that is where its one non-obvious assumption
// lives: a Run row created the way AcceptTurn creates one must come back as the turn the
// browser had before the restart. A double in this package cannot settle that - the in-package
// mockState also ignores token scope by design - so this test checks three things no mock can:
//
//   - the Run payload really is the accept-time turn payload, and turn_from_run's
//     "payload.type must be turn" gate accepts it;
//   - other rows of the same thread (an inbox run) are NOT invented into turns: a thread's run
//     list is not the same set as its turns;
//   - a foreign-scope token gets nothing, enforced by State itself rather than by filtering
//     here.
func TestTurnHistoryRealStateAfterRestart(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	owner := e.signTask(t, "task-"+contracts.NewID())
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}

	th, err := e.client.CreateThread(ctx, owner, scope, "ctx-th-"+contracts.NewID(), "th-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	turnRun, err := e.client.CreateRun(ctx, owner, th.ID, "th-turn-"+contracts.NewID(),
		json.RawMessage(`{"type":"turn","input":"write a haiku","max_steps":8}`), "")
	if err != nil {
		t.Fatalf("CreateRun turn: %v", err)
	}
	inboxRun, err := e.client.CreateRun(ctx, owner, th.ID, "th-inbox-"+contracts.NewID(),
		json.RawMessage(`{"type":"inbox","kind":"event"}`), "")
	if err != nil {
		t.Fatalf("CreateRun inbox: %v", err)
	}

	// The restart, in the only form a test can produce it: a brand-new Coordinator with an
	// empty in-memory turn table over the same durable State.
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{}, WorkerID: "w-after-restart",
	})

	turns, err := coord.HydrateTurnsFromState(ctx, owner, th.ID, scope)
	if err != nil {
		t.Fatalf("HydrateTurnsFromState: %v", err)
	}
	if len(turns) != 1 {
		t.Fatalf("thread has one turn row and one inbox row; the turn list must be exactly the turn: %+v", turns)
	}
	got := turns[0]
	if got.ID != turnRun.ID || got.RunID != turnRun.ID || got.ThreadID != th.ID {
		t.Fatalf("rebuilt turn lost its durable identity: got %+v want id/run %s thread %s", got, turnRun.ID, th.ID)
	}
	if got.MaxSteps != 8 {
		t.Fatalf("max_steps must come back from the accept-time payload, got %d", got.MaxSteps)
	}
	if string(got.Input) != `"write a haiku"` {
		t.Fatalf("turn input must round-trip from the durable payload, got %s", got.Input)
	}
	if got.Status == "" {
		t.Fatalf("status must be carried from the durable row, got empty")
	}

	one, err := coord.HydrateTurnFromState(ctx, owner, turnRun.ID, scope)
	if err != nil || one == nil {
		t.Fatalf("HydrateTurnFromState(%s) = %v, %v; want the turn", turnRun.ID, one, err)
	}

	// The inbox row is durable but it is not a turn. Answering it as one would show the user
	// a conversation they never started.
	notATurn, err := coord.HydrateTurnFromState(ctx, owner, inboxRun.ID, scope)
	if err != nil {
		t.Fatalf("HydrateTurnFromState on a non-turn row: %v", err)
	}
	if notATurn != nil {
		t.Fatalf("a non-turn row must not be presented as a turn, got %+v", notATurn)
	}

	// Isolation is State's job, and this is the claim the whole read side rests on: the same
	// hydration with a foreign task token must yield nothing at all.
	intruder, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: "intruder-" + contracts.NewID(), Task: "task-f", Root: "run-f",
		Context: "ctx-f", Grant: "grant-f", Depth: 0,
	}, 5*60*1000*1000*1000)
	if err != nil {
		t.Fatalf("sign intruder: %v", err)
	}
	if foreign, ferr := coord.HydrateTurnsFromState(ctx, intruder, th.ID, contracts.Scope{
		Tenant: e.tenant, Deployment: e.deployment, Subject: "someone-else",
	}); ferr == nil {
		t.Fatalf("a foreign token hydrated %d turns from another subject's thread: %+v", len(foreign), foreign)
	}
	if one, oerr := coord.HydrateTurnFromState(ctx, intruder, turnRun.ID, contracts.Scope{}); oerr == nil && one != nil {
		t.Fatalf("a foreign token read another subject's turn: %+v", one)
	}
}
