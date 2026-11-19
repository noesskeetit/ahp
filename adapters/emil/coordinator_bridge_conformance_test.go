package emil

// coordinator_bridge_conformance_test.go — CLIENT-CONFORMANCE only (director-endorsed):
// a fake State HTTP SERVER drives OUR StateClient + Coordinator through the
// CoordinatorBridge to assert the turn-lifecycle contract (HITL/outbox/steering land
// in the State inbox with the right key/kind; NO scope/tenant/subject/deployment in
// the wire body; Recover classifies pending turns; unknown-turn steering is rejected).
// This is NOT a live State proof — it tests the bridge/coordinator we ship. Live proof
// lives in the real-PG suites.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type recorded struct {
	op   string
	body map[string]any
}

func newRecordingState(t *testing.T) (*space.StateClient, func() []recorded) {
	t.Helper()
	var mu sync.Mutex
	var rec []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := r.URL.Path[len("/v1/state/"):]
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		rec = append(rec, recorded{op: op, body: b})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		switch op {
		case "CreateThread":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "th1", "epoch": 1})
		case "CreateRun":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "run1", "thread_id": "th1", "status": "queued"})
		case "Enqueue":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "ib1", "thread_id": b["thread_id"], "key": b["key"], "kind": b["kind"], "sequence": 1})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	t.Cleanup(srv.Close)
	return space.NewStateClient(srv.URL, nil), func() []recorded {
		mu.Lock()
		defer mu.Unlock()
		return append([]recorded(nil), rec...)
	}
}

func assertNoScopeLeak(t *testing.T, b map[string]any, op string) {
	t.Helper()
	for _, f := range []string{"scope", "tenant", "subject", "deployment"} {
		if _, ok := b[f]; ok {
			t.Fatalf("%s body must NOT contain scope field %q (scope comes from the task-token principal): %v", op, f, b)
		}
	}
}

func TestCoordinatorBridgeTurnLifecycle(t *testing.T) {
	ctx := context.Background()
	sc, all := newRecordingState(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w1"})
	br := NewCoordinatorBridge(coord)

	// Register a turn via the real Coordinator path (AcceptTurn -> CreateThread+CreateRun).
	accept, err := coord.AcceptTurn(ctx, "tok", contracts.Scope{}, space.AcceptTurnRequest{Key: "k1", ContextID: "c1", Input: json.RawMessage(`{"text":"hi"}`)})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if accept.TurnID != "run1" || accept.ThreadID != "th1" {
		t.Fatalf("AcceptTurn wired wrong ids: %+v", accept)
	}

	// Snapshot must reflect the registered turn.
	if snap, ok := br.TurnSnapshot(accept.TurnID); !ok || snap.Status != space.TurnPending {
		t.Fatalf("TurnSnapshot must see the pending turn, got ok=%v status=%v", ok, snap.Status)
	}

	// Recover BEFORE any HITL mutation: the pending turn is recoverable (would-be-replayed),
	// never unknown. (SubmitHITL below flips status to awaiting, which is out of Recover's
	// pending/running classification.)
	rec, err := br.Recover(ctx, "tok")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(rec.Recovered) != 1 || rec.Recovered[0].ID != accept.TurnID {
		t.Fatalf("Recover must list the pending turn as recovered, got %+v", rec)
	}

	// SubmitHITL -> Enqueue hitl_request in the inbox, no scope leak, thread-scoped.
	hitl, err := br.SubmitHITL(ctx, "tok", accept.TurnID, "approval", json.RawMessage(`{"question":"go?"}`), nil, accept.TurnID, "w1", 1)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	if hitl.Status != space.HITLPending || hitl.TurnID != accept.TurnID {
		t.Fatalf("SubmitHITL returned wrong gate: %+v", hitl)
	}
	var enq *recorded
	for i := range all() {
		if all()[i].op == "Enqueue" {
			enq = &all()[i]
		}
	}
	if enq == nil {
		t.Fatalf("SubmitHITL must Enqueue to State inbox")
	}
	assertNoScopeLeak(t, enq.body, "SubmitHITL/Enqueue")
	if enq.body["thread_id"] != "th1" || enq.body["kind"] != "hitl_request" {
		t.Fatalf("SubmitHITL Enqueue must be thread-scoped kind=hitl_request, got %v", enq.body)
	}
	if k, _ := enq.body["key"].(string); len(k) < len("hitl:") || k[:5] != "hitl:" {
		t.Fatalf("SubmitHITL Enqueue key must be prefixed hitl:, got %q", k)
	}

	// RespondHITL -> Enqueue hitl_response.
	if err := br.RespondHITL(ctx, "tok", hitl.ID, true, json.RawMessage(`{"ok":true}`), "th1"); err != nil {
		t.Fatalf("RespondHITL: %v", err)
	}
	var resp *recorded
	for i := range all() {
		if all()[i].op == "Enqueue" && all()[i].body["kind"] == "hitl_response" {
			resp = &all()[i]
		}
	}
	if resp == nil {
		t.Fatalf("RespondHITL must Enqueue a hitl_response to the inbox")
	}
	assertNoScopeLeak(t, resp.body, "RespondHITL/Enqueue")

	// EmitOutbox -> Enqueue outbox item.
	if _, err := br.EmitOutbox(ctx, "tok", accept.TurnID, "notify-1", "notification", json.RawMessage(`{"to":"user"}`), "th1", "w1", 1); err != nil {
		t.Fatalf("EmitOutbox: %v", err)
	}
	var obx *recorded
	for i := range all() {
		if all()[i].op == "Enqueue" && all()[i].body["kind"] == "outbox" {
			obx = &all()[i]
		}
	}
	if obx == nil {
		t.Fatalf("EmitOutbox must Enqueue an outbox item to the inbox")
	}
	assertNoScopeLeak(t, obx.body, "EmitOutbox/Enqueue")
	if k, _ := obx.body["key"].(string); k != "outbox:notify-1" {
		t.Fatalf("EmitOutbox key must be outbox:<key>, got %q", k)
	}

	// Steering a turn that is not running (pending here) must be rejected, not silently accepted.
	if err := br.Steer(ctx, "tok", accept.TurnID, "stop"); err == nil {
		t.Fatalf("Steer of a non-running turn must be rejected (conflict), got nil")
	}

}
