//go:build state_integration

package space_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// accept_turn_replay_realstate_test.go — UNKNOWN between turns (block 8), on the real server.
//
// AcceptTurn is idempotent by the caller's Key: CreateThread dedups by (scope, key) and CreateRun
// dedups by (thread, key+":run") - both ON CONFLICT ... DO NOTHING paths in internal/state/store.go.
// That is exactly what a caller whose AcceptTurn response was lost (the stand's "AcceptTurn http=000")
// relies on: it retries the SAME Key and must get the SAME turn back, not a second one.
//
// Space honoured that only halfway. On the retry it read the run State handed back (the existing
// run, same id) and then rebuilt a FRESH in-memory Turn with Status=TurnPending and wrote it into
// c.turns UNCONDITIONALLY (internal/space/coordinator.go). So a retry that arrives while the run is
// already running replaced the live turn - wiping status/steps/checkpoint behind a run that is really
// executing, and answering the caller "pending" for a run whose own durable row says "running". The
// durable layer is idempotent; the warm Space state was not.
//
// A replay must be idempotent in Space too: the same turn back, the real status, and the live turn
// left untouched.
func TestAcceptTurnReplayDoesNotResetARunningTurn(t *testing.T) {
	e := cNewEnv(t)

	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{blockForever: true}, WorkerID: "w-accept-replay"})
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srv.Close)

	key := "acceptreplay-" + contracts.NewID()
	body := map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "go"}, "max_steps": 4,
	}
	code, resp := postSpaceAs(t, srv.URL, "AcceptTurn", body, "ada")
	if code != http.StatusOK {
		t.Fatalf("first AcceptTurn = %d, want 200 (body=%s)", code, resp)
	}
	acc1 := decodeMap(t, resp)
	turnID, _ := acc1["turn_id"].(string)
	if turnID == "" {
		t.Fatalf("first AcceptTurn returned no turn_id: %s", resp)
	}
	t.Cleanup(func() { _, _ = postSpaceAs(t, srv.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada") })

	if c, b := postSpaceAs(t, srv.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	waitLeaseHeld(t, coord, turnID, "w-accept-replay")
	if snap, ok := coord.TurnSnapshot(turnID); !ok || snap.Status != space.TurnRunning {
		t.Fatalf("precondition: after dispatch the turn should be running, got %v (ok=%v)", snap.Status, ok)
	}

	// The caller's first response was lost in transport; it retries the identical request.
	code2, resp2 := postSpaceAs(t, srv.URL, "AcceptTurn", body, "ada")
	if code2 != http.StatusOK {
		t.Fatalf("replayed AcceptTurn = %d, want 200 (body=%s)", code2, resp2)
	}
	acc2 := decodeMap(t, resp2)
	if id2, _ := acc2["turn_id"].(string); id2 != turnID {
		t.Fatalf("a replay of the same Key returned a different turn %q, want the same %q", id2, turnID)
	}

	// The live turn must survive the replay: same run, still running, not reset behind the run's back.
	if snap, ok := coord.TurnSnapshot(turnID); !ok || snap.Status != space.TurnRunning {
		t.Fatalf("the replayed AcceptTurn reset a running turn to %q (ok=%v): the durable run is running and "+
			"this worker is executing it, so a lost-ACK retry must not replace the live turn with a fresh pending one",
			snap.Status, ok)
	}
	// And what it answers must match the run, not claim a state the durable row never said.
	if st, _ := acc2["status"].(string); st == string(space.TurnPending) {
		t.Fatalf("replayed AcceptTurn answered %q for a run whose durable status is running - a replay must "+
			"report the turn's real state, not re-manufacture %q", st, space.TurnPending)
	}
}

// Control: a genuinely NEW Key (a different conversation) is still accepted as a fresh pending turn.
// This pins that the fix is not "dedup everything / never create".
func TestAcceptTurnNewKeyStillCreatesAPendingTurn(t *testing.T) {
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{blockForever: true}, WorkerID: "w-accept-new"})
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srv.Close)

	key := "acceptnew-" + contracts.NewID()
	code, resp := postSpaceAs(t, srv.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "go"}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, resp)
	}
	acc := decodeMap(t, resp)
	turnID, _ := acc["turn_id"].(string)
	if turnID == "" {
		t.Fatalf("no turn_id: %s", resp)
	}
	t.Cleanup(func() { _, _ = postSpaceAs(t, srv.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada") })
	if st, _ := acc["status"].(string); st != string(space.TurnPending) {
		t.Fatalf("a brand-new Key must be accepted as pending, got %q", st)
	}
	if snap, ok := coord.TurnSnapshot(turnID); !ok || snap.Status != space.TurnPending {
		t.Fatalf("a brand-new Key must create a tracked pending turn, got %v (ok=%v)", snap.Status, ok)
	}
}
