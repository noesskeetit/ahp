//go:build state_integration

package space_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestColdReadNamesTheHITLGateOnRealState is the real-wire leg of the HITL cold-read fix.
//
// The double in hitl_cold_status_test.go can only prove what the projection DOES with a wait
// target; it cannot prove the REAL State API publishes one. If State's GetRun response dropped
// wait_for, the fix would stay dead code in production while the offline suite stayed green - the
// failure mode this lane keeps meeting whenever a double turns out to be more generous than the
// published contract.
//
// So this drives the real State over PostgreSQL through the real seam: a live runner parks its run
// on a HITL gate, and a coordinator with an empty turn table (the restart) reads that same turn
// back. Three facts are asserted separately because they fail separately:
//
//   - the wire: State's own GetRun response says status=waiting AND wait_for=hitl_response, read
//     off the raw response body rather than off a struct this repo controls;
//   - the parking is done by SubmitHITL's own Suspend under the live lease fence, not by the test
//     poking a row;
//   - the cold projection then says awaiting_hitl - the status the warm reader already says.
func TestColdReadNamesTheHITLGateOnRealState(t *testing.T) {
	e := cNewEnv(t)

	coordExec := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{blockForever: true}, WorkerID: "w-hitlreal-exec"})
	srvExec := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coordExec,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srvExec.Close)

	key := "hitlreal-" + contracts.NewID()
	code, body := postSpaceAs(t, srvExec.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "needs approval"}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(ada) = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	if turnID == "" {
		t.Fatalf("AcceptTurn returned no turn_id: %s", body)
	}
	t.Cleanup(func() {
		// Free the real lease so no other run against this acceptance PG inherits it.
		_, _ = postSpaceAs(t, srvExec.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	})

	if c, b := postSpaceAs(t, srvExec.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn(exec) = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coordExec, turnID, "w-hitlreal-exec")

	ownerTok, _, err := realTaskExchanger{e}.Exchange(e.ctx, space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	if _, err := coordExec.SubmitHITL(e.ctx, ownerTok, space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"q":"Proceed?"}`),
	}, turnID, "w-hitlreal-exec", held.LeaseEpoch); err != nil {
		t.Fatalf("SubmitHITL(exec, lease epoch %d): %v", held.LeaseEpoch, err)
	}

	warm, ok := coordExec.TurnSnapshot(turnID)
	if !ok || warm.Status != space.TurnAwaiting {
		t.Fatalf("warm read = %v (ok=%v), want %q before touching the projection",
			warm.Status, ok, space.TurnAwaiting)
	}

	// The wire, read raw: the fix may only rename a wait that State actually publishes.
	status, rawRun := e.raw("GetRun", ownerTok, []byte(`{"run_id":"`+turnID+`"}`))
	if status != http.StatusOK {
		t.Fatalf("GetRun(raw) = %d, want 200 (body=%v)", status, rawRun)
	}
	if rawRun["status"] != "waiting" {
		t.Fatalf("real State reports the parked run as %v, want \"waiting\"", rawRun["status"])
	}
	if rawRun["wait_for"] != "hitl_response" {
		t.Fatalf("real State GetRun does not publish the HITL wait target (wait_for=%v): no projection may "+
			"name a gate the wire does not carry, and the cold read would stay broken behind a green double",
			rawRun["wait_for"])
	}

	coordCold := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{}, WorkerID: "w-hitlreal-cold"})
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "ada"}
	cold, err := coordCold.HydrateTurnFromState(e.ctx, ownerTok, turnID, scope)
	if err != nil || cold == nil {
		t.Fatalf("cold HydrateTurnFromState(%s) = %v, %v; want the turn", turnID, cold, err)
	}
	if cold.Status != space.TurnAwaiting {
		t.Fatalf("one turn, two readers: warm %q, cold %q. The real State row says waiting+"+
			"wait_for=hitl_response, so after a runner restart an approval UI keyed on Space's published "+
			"status loses the gate.", warm.Status, cold.Status)
	}
}
