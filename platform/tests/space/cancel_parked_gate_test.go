package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// cancel_parked_gate_test.go — cancelling a turn that is waiting for a human answers 200 and stops nothing.
//
// SubmitHITL parks the run with Suspend, and Suspend hands the thread lease back (internal/state/store.go
// Suspend -> clearLease). Every durable close of a run is fenced by that lease (Store.currentLease), and
// State publishes no operation that closes a waiting run without one - the operation table has no
// unsuspend and no run-status write (internal/stateapi/stateapi.go). So the cancel that follows reaches a
// run it cannot close: Release answers STALE_EPOCH, which CancelTurn tolerates because the very same answer
// also means "somebody already closed this", and the read behind it adopts only TERMINAL statuses
// (adoptTurnStatus). A row still saying `waiting` is not adopted, so the turn is stamped `canceled` in
// memory and the caller is told the cancel worked.
//
// What that leaves behind is worse than the split the 9892e2a guard was written for. The run stays the
// thread's first unfinished run with no deadline, and ClaimRun hands out no later run of a thread while a
// waiting run blocks it (claimCandidatesSQL), so EVERY future turn of that conversation queues behind a run
// everybody believes was canceled. The cancel's own terminal frame is fenced off by the same missing lease,
// so the durable transcript of a thread the user just canceled holds no terminal event - the D4 promise,
// broken by this path. And the gate stays pending, so a human is still being asked to approve a turn the
// interface says was canceled.
//
// The fix may not make cancelling impossible either: the third test below is the control that keeps the
// refusal a statement about a lease Space no longer holds rather than a wall.

// warmTurnStatus reads what the process that still holds the turn says it is.
func warmTurnStatus(t *testing.T, coord *space.Coordinator, turnID string) space.TurnStatus {
	t.Helper()
	snap, ok := coord.TurnSnapshot(turnID)
	if !ok {
		t.Fatalf("the coordinator no longer knows turn %s at all", turnID)
	}
	return snap.Status
}

// leaseHeldOnDouble asks the double whether the run's thread lease is still the one this worker was given.
func leaseHeldOnDouble(ms *mockState, runID string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	_, ok := ms.leases[runID]
	return ok
}

// An older State server without CancelWaitingRun must fail closed. The actual
// supported transition is verified against PG in cancel_waiting_realstate_test.go.
func TestCancelParkedTurnFailsClosedOnOlderState(t *testing.T) {
	g := parkedGate(t)
	err := g.coord.CancelTurn(context.Background(), g.turnID, "tok-s1")
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("older State route refusal = %v", err)
	}
	if got := warmTurnStatus(t, g.coord, g.turnID); got != space.TurnAwaiting {
		t.Fatalf("failed cancel changed turn to %s", got)
	}
	if status, _ := durableRun(g.ms, g.turnID); status != "waiting" {
		t.Fatalf("failed cancel changed durable run to %s", status)
	}
	if err := g.coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{HITLID: g.hitlID, Approved: true, Response: json.RawMessage(`{}`)}, g.threadID); err != nil {
		t.Fatal(err)
	}
	if status, _ := durableRun(g.ms, g.turnID); status != "pending" {
		t.Fatalf("normal gate answer failed after refused cancellation: %s", status)
	}
}

// TestCancelOfAParkSpaceDidNotCreateStillWorks is the control that keeps the refusal from becoming a wall:
// a run parked on someone else's wait target, whose lease this worker still holds, is closable - and a
// cancel that can close the run must close it and say so.
func TestCancelOfAParkSpaceDidNotCreateStillWorks(t *testing.T) {
	live, _, ms, _, turnID := coldHitlFixture(t, "external_event")
	if status, waitFor := durableRun(ms, turnID); status != "waiting" || waitFor != "external_event" {
		t.Fatalf("fixture: the run is (%q, wait_for=%q), want parked on a wait Space did not create", status, waitFor)
	}
	if !leaseHeldOnDouble(ms, turnID) {
		t.Fatalf("fixture: this worker must still hold the lease, or the control proves nothing")
	}
	if err := live.CancelTurn(context.Background(), turnID, "token"); err != nil {
		t.Fatalf("cancel of a parked run whose lease is still held = %v, want it to work: the defect is the lease "+
			"Suspend gave back, not the wait itself", err)
	}
	if status, _ := durableRun(ms, turnID); status != "canceled" {
		t.Fatalf("a cancel that could close the run left it %q, want %q", status, "canceled")
	}
}
