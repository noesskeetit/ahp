//go:build state_integration

package space_test

// coordinator_bridge_realstate_test.go — drives the CoordinatorBridge (the Emil
// workflow.py turn-lifecycle seam -> the authoritative space.Coordinator, gap-doc T1b)
// against the REAL State API over PostgreSQL and asserts its operations write through
// to the real thread inbox: HITL submit (approvals), HITL respond, outbox emit, and
// steering. This is the real-API upgrade of the bridge's offline client-conformance
// (fake State server); it proves acceptance criteria #1 (HITL/outbox/steering through
// the seam) and #3 (approvals=HITL) against the authoritative State service. Nothing
// fakes State — it runs against the real stateapi + PG from cNewEnv.
//
// Author-side candidate evidence; independent live verification is iv's. Emil name/port
// are NOT a gate.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCoordinatorBridge_RealState_TurnLifecycle(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "bridge-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "bridge-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "w1", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease: %v", err)
	}

	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w1"})
	bridge := emil.NewCoordinatorBridge(coord)

	// Register a running turn in the coordinator WITHOUT running the model loop, so the
	// turn-lifecycle seam can operate on it and forward to the real State inbox.
	turnID := l.RunID
	bridge.RehydrateTurn(space.Turn{ID: turnID, RunID: l.RunID, ThreadID: l.ThreadID, Status: space.TurnRunning, MaxSteps: 8})

	// Steering (Running -> Steering), then outbox, then HITL submit (-> Awaiting) + respond.
	if err := bridge.Steer(ctx, l.TaskToken, turnID, "focus on revenue"); err != nil {
		t.Fatalf("bridge.Steer: %v", err)
	}
	if _, err := bridge.EmitOutbox(ctx, l.TaskToken, turnID, "note-1", "notification", json.RawMessage(`{"msg":"did work"}`), l.ThreadID, l.Worker, l.Epoch); err != nil {
		t.Fatalf("bridge.EmitOutbox: %v", err)
	}
	hitl, err := bridge.SubmitHITL(ctx, l.TaskToken, turnID, "approval", json.RawMessage(`{"action":"send_email"}`), nil, l.RunID, l.Worker, l.Epoch)
	if err != nil {
		t.Fatalf("bridge.SubmitHITL: %v", err)
	}
	if err := bridge.RespondHITL(ctx, l.TaskToken, hitl.ID, true, json.RawMessage(`{"ok":true}`), l.ThreadID); err != nil {
		t.Fatalf("bridge.RespondHITL: %v", err)
	}

	// TurnSnapshot surfaces the in-memory status advanced by the seam (approvals).
	if snap, ok := bridge.TurnSnapshot(turnID); !ok || snap.Status != space.TurnAwaiting {
		t.Fatalf("TurnSnapshot after SubmitHITL: ok=%v status=%v want awaiting_hitl", ok, snap.Status)
	}

	// The authoritative evidence: every operation landed in the REAL State inbox.
	inbox, err := rs.ReadInbox(ctx, l.TaskToken, l.ThreadID, 0, 50)
	if err != nil {
		t.Fatalf("ReadInbox over real State: %v", err)
	}
	haveKinds := map[string]int{}
	for _, ev := range inbox {
		haveKinds[ev.Kind]++
	}
	for _, want := range []string{"steering", "outbox", "hitl_request", "hitl_response"} {
		if haveKinds[want] == 0 {
			t.Fatalf("real State inbox missing kind %q; have=%v (total=%d)", want, haveKinds, len(inbox))
		}
	}
	// Outbox is keyed by the caller key (at-least-once); verify the key round-tripped.
	var sawOutboxKey bool
	for _, ev := range inbox {
		if ev.Kind == "outbox" && ev.Key == "outbox:note-1" {
			sawOutboxKey = true
		}
	}
	if !sawOutboxKey {
		t.Fatalf("outbox key not persisted as expected: have=%v", haveKinds)
	}

	if err := rs.Release(ctx, *l, "done"); err != nil {
		t.Logf("Release(non-fatal): %v", err)
	}
	t.Logf("CoordinatorBridge real State: steering=%d outbox=%d hitl_request=%d hitl_response=%d (turn advanced to awaiting_hitl)", haveKinds["steering"], haveKinds["outbox"], haveKinds["hitl_request"], haveKinds["hitl_response"])
}
