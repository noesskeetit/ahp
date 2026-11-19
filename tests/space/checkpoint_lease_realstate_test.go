//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestSaveCheckpointLeaseFenceOnRealState is the regression for finding 20261031T014517Z
// against the REAL State server on PostgreSQL (authoritative lease fence, not a double):
//
//  1. the process that holds the run's lease writes a snapshot and gets a 200, and the
//     snapshot is durable - ListCheckpoints reads it back in Space's own namespace;
//  2. the ingress process (same subject, same durable State, NO lease because a different
//     process executes the run) is refused with a 409 STALE_EPOCH. Before the fix this
//     leg answered HTTP 500 {"code":"INTERNAL"}: State had refused correctly, and Space
//     threw the reason away.
//
// It also proves the fence Space sends is the GRANTED lease and not the local
// zero-profile: the owner's write only succeeds because its worker/epoch are the ones
// ClaimRun issued to it.
func TestSaveCheckpointLeaseFenceOnRealState(t *testing.T) {
	e := cNewEnv(t)

	// The ingress face: it accepts turns and never executes them.
	coordIngress := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{}, WorkerID: "w-realsnap-ingress",
	})
	srvIngress := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coordIngress,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srvIngress.Close)

	// The execution face: it claims the run and is still inside its Core loop, so State
	// holds ITS lease for the whole test.
	coordExec := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{blockForever: true}, WorkerID: "w-realsnap-exec",
	})
	srvExec := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator:       coordExec,
		Auth:              space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
		RequireScopedTask: true,
	}))
	t.Cleanup(srvExec.Close)

	key := "realsnap-" + contracts.NewID()
	code, body := postSpaceAs(t, srvIngress.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "snapshot me"}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(ada) = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn missing ids: %s", body)
	}
	t.Cleanup(func() {
		// Free the real lease so no other run of this acceptance PG inherits it.
		_, _ = postSpaceAs(t, srvExec.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	})

	// The executor takes the run over: State issues it a worker+epoch.
	if c, b := postSpaceAs(t, srvExec.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn(exec) = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coordExec, turnID, "w-realsnap-exec")
	if held.LeaseEpoch <= 0 {
		t.Fatalf("executor recorded a zero epoch: %+v", held)
	}

	snapshot := map[string]any{
		"turn_id":  turnID,
		"sequence": 1,
		"state":    map[string]any{"step": 1},
		"messages": []any{map[string]any{"role": "user", "content": "snapshot me"}},
	}

	// (1) The lease owner writes: 200 and a durable checkpoint id.
	code, body = postSpaceAs(t, srvExec.URL, "SaveCheckpoint", snapshot, "ada")
	if code != http.StatusOK {
		t.Fatalf("SaveCheckpoint(owner, lease epoch %d) = %d, want 200 (body=%s)", held.LeaseEpoch, code, body)
	}
	cpObj, _ := decodeMap(t, body)["checkpoint"].(map[string]any)
	cpID, _ := cpObj["id"].(string)
	if cpID == "" {
		t.Fatalf("owner snapshot response has no checkpoint id: %s", body)
	}

	// The snapshot is durable and readable by the owner's own scope.
	ownerTok, _, err := realTaskExchanger{e}.Exchange(context.Background(), space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	listed, err := e.client.ListCheckpoints(e.ctx, ownerTok, threadID, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != cpID || listed[0].Namespace != space.CheckpointNamespace {
		t.Fatalf("ListCheckpoints after the owner write = %+v, want exactly the saved %s in %s",
			listed, cpID, space.CheckpointNamespace)
	}

	// (2) The ingress face - same subject, same State, no lease - is refused honestly.
	code, body = postSpaceAs(t, srvIngress.URL, "SaveCheckpoint", snapshot, "ada")
	if code != http.StatusConflict {
		t.Fatalf("SaveCheckpoint(ingress, no lease) = %d, want 409 (body=%s) - the finding's 500 is a regression", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrStaleEpoch {
		t.Fatalf("ingress refusal code = %q, want %q (body=%s)", got, contracts.ErrStaleEpoch, body)
	}

	// And it really wrote nothing: the durable list is unchanged.
	after, err := e.client.ListCheckpoints(e.ctx, ownerTok, threadID, space.CheckpointNamespace, 10)
	if err != nil {
		t.Fatalf("ListCheckpoints after the refused write: %v", err)
	}
	if len(after) != len(listed) {
		t.Fatalf("a refused snapshot changed durable state: before=%d after=%d", len(listed), len(after))
	}

	// The state must not be a JSON error the client cannot branch on either way.
	if !json.Valid(body) {
		t.Fatalf("refusal body is not valid JSON: %s", body)
	}
}
