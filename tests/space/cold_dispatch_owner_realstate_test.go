//go:build state_integration

package space_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestColdDispatchOwnerRestoresOnRealState proves, against the REAL State server on
// PostgreSQL (authoritative lease.scope + claim reissue), that after a process restart a
// cold-dispatched run rebinds to the LEGITIMATE OWNER — not to whoever triggered the
// dispatch — while a foreign subject still cannot read it. This is the positive owner leg the
// offline unscoped State double cannot model (it returns no lease.scope); it is the
// restart/private-context acceptance leg run on the isolated acceptance-PG.
func TestColdDispatchOwnerRestoresOnRealState(t *testing.T) {
	e := cNewEnv(t)

	// Process 1: Ada accepts a turn (run stays Pending), then the process "dies" (we drop
	// coord1; its in-memory turn table goes with it).
	coord1 := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w-a1"})
	h1 := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord1,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	})
	hs1 := httptest.NewServer(h1)
	t.Cleanup(hs1.Close)

	code, body := postSpaceAs(t, hs1.URL, "AcceptTurn", map[string]any{
		"key": "cdor-ada-" + contracts.NewID(), "context_id": "ctx-cdor-ada",
		"input": map[string]any{"role": "user", "content": "hi"}, "max_steps": 1,
	}, "ada")
	if code != 200 {
		t.Fatalf("AcceptTurn(ada)=%d want 200 body=%s", code, body)
	}
	turnID, _ := decodeMap(t, body)["turn_id"].(string)
	if turnID == "" {
		t.Fatalf("no turn_id in AcceptTurn body: %s", body)
	}

	// Process 2 (restart): a fresh coordinator with an EMPTY turn table over the same durable
	// State. A DIFFERENT subject (bob) triggers the cold dispatch, mimicking a takeover attempt
	// by a foreign worker whose scope must NOT become the run's owner.
	coord2 := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w-b2"})
	h2 := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord2,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	})
	hs2 := httptest.NewServer(h2)
	t.Cleanup(hs2.Close)

	if c, b := postSpaceAs(t, hs2.URL, "DispatchTurn", nil, "bob"); c != http.StatusOK {
		t.Fatalf("cold DispatchTurn(bob)=%d want 200 body=%s", c, b)
	}

	// The rehydrated turn is bound to the durable LEASE scope = Ada's owner scope, NOT bob's.
	snap, ok := coord2.TurnSnapshot(turnID)
	if !ok {
		t.Fatal("cold dispatch did not rehydrate the turn")
	}
	want := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "ada"}
	if snap.Scope != want {
		t.Fatalf("cold turn scope=%+v want owner %+v (owner must be restored from lease.scope)", snap.Scope, want)
	}

	// Positive control: the legitimate owner reads their resumed turn after the restart.
	if c, b := postSpaceAs(t, hs2.URL, "GetTurn", map[string]any{"turn_id": turnID}, "ada"); c != http.StatusOK {
		t.Fatalf("owner GetTurn(ada)=%d want 200 (owner not restored after restart) body=%s", c, b)
	}
	// Negative: the foreign subject who triggered the dispatch cannot read Ada's turn.
	if c, b := postSpaceAs(t, hs2.URL, "GetTurn", map[string]any{"turn_id": turnID}, "bob"); c != http.StatusNotFound {
		t.Fatalf("foreign GetTurn(bob)=%d want 404 (private context must hold) body=%s", c, b)
	}
}
