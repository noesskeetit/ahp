package statejournal

import (
	"crypto/rand"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func newVMAdmission(t *testing.T) (contracts.Scope, EnqueueRequest) {
	t.Helper()
	return contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "agent", Subject: "user"}, EnqueueRequest{
		SessionID: "session", Harness: "langgraph", FormatVersion: 1, Key: "start",
		Input: Payload{Inline: []byte(`{"message":"hello"}`)}, ExecutionMode: journal.ExecutionVMManaged,
	}
}

func TestPullClaimAndSweeperIgnoreVMManaged(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	scope, req := newVMAdmission(t)
	receipt, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil || receipt.Status != SessionQueued {
		t.Fatalf("enqueue: %+v %v", receipt, err)
	}
	if _, err := store.ClaimTurn(ctx, scope, req.SessionID, "pull", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("pull claimed queued VM: %v", err)
	}
	worker := journal.DeterministicExecutionID(scope, req.SessionID, receipt.HeadSeq, 1)
	claim := journal.VMTurnClaimRequest{TurnSeq: receipt.HeadSeq, Generation: 1, ExecutionID: worker}
	first, err := store.ClaimVMTurn(ctx, scope, req.SessionID, claim, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE ahp_journal.sessions SET lease_expires_at=clock_timestamp() WHERE tenant_id=$1", scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if count, err := store.RequeueExpiredTurns(ctx, scope.Tenant, 64); count != 0 || err != nil {
		t.Fatalf("pull sweeper touched VM: %d %v", count, err)
	}
	if _, err := store.ClaimTurn(ctx, scope, req.SessionID, "pull", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("pull claimed expired VM: %v", err)
	}
	claim.Generation++
	claim.ExecutionID = journal.DeterministicExecutionID(scope, req.SessionID, receipt.HeadSeq, claim.Generation)
	if _, err := store.ClaimVMTurn(ctx, scope, req.SessionID, claim, time.Minute); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("VM claim bypassed fencing: %v", err)
	}
	fenced := journal.FenceRequeueRequest{TurnSeq: first.InputSeq, Epoch: first.Epoch, ExecutionID: first.Worker}
	if err := store.RequeueFencedVMTurn(ctx, scope, req.SessionID, fenced); err != nil {
		t.Fatal(err)
	}
	stale := journal.VMTurnClaimRequest{TurnSeq: receipt.HeadSeq, Generation: 1, ExecutionID: worker}
	if _, err := store.ClaimVMTurn(ctx, scope, req.SessionID, stale, time.Minute); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("stale execution got the replacement claim: %v", err)
	}
	second, err := store.ClaimVMTurn(ctx, scope, req.SessionID, claim, time.Minute)
	if err != nil || second.Epoch != 2 || second.InputSeq != first.InputSeq {
		t.Fatalf("replacement: %+v %v", second, err)
	}
}

func TestSteerPeekSurvivesLostResponseAndTargetTurnIsolation(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope, req := newVMAdmission(t)
	admitted, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	claim := journal.VMTurnClaimRequest{TurnSeq: admitted.HeadSeq, Generation: 1,
		ExecutionID: journal.DeterministicExecutionID(scope, req.SessionID, admitted.HeadSeq, 1)}
	turn, err := store.ClaimVMTurn(ctx, scope, req.SessionID, claim, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req.Key, req.DeliveryMode = "followup", journal.DeliveryFollowup
	followup, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	req.Key, req.DeliveryMode, req.TargetTurnSeq = "steer", journal.DeliverySteer, turn.InputSeq
	steer, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, control, err := store.HeartbeatLeaseWithControl(ctx, scope, req.SessionID, turn.Lease, time.Minute)
		if err != nil || control == nil || len(control.PendingSteer) != 1 || control.PendingSteer[0].Seq != steer.HeadSeq {
			t.Fatalf("read without ACK: %+v %v", control, err)
		}
	}
	finish := AppendRequest{Key: "finish", ExpectedSeq: steer.HeadSeq, TurnSeq: turn.InputSeq, FinishTurn: SessionIdle}
	if _, err := store.AppendEvents(ctx, scope, req.SessionID, turn.Lease, false, finish); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("normal completion bypassed pending steer: %v", err)
	}
	finish.SteerAck = &journal.SteerAck{TurnSeq: turn.InputSeq, UpToSeq: steer.HeadSeq, Count: 1}
	closed, err := store.AppendEvents(ctx, scope, req.SessionID, turn.Lease, false, finish)
	if err != nil || closed.Control == nil || closed.Control.NextTurn == nil || closed.Control.NextTurn.InputSeq != followup.HeadSeq {
		t.Fatalf("finish with ACK: %+v %v", closed, err)
	}
	replay, err := store.AppendEvents(ctx, scope, req.SessionID, turn.Lease, false, finish)
	if err != nil || replay.Control == nil || replay.Control.NextTurn == nil || replay.Control.NextTurn.InputSeq != followup.HeadSeq {
		t.Fatalf("lost finish response: %+v %v", replay, err)
	}
	_, control, err := store.HeartbeatLeaseWithControl(ctx, scope, req.SessionID, turn.Lease, time.Minute)
	if err != nil || control != nil {
		t.Fatalf("steer spilled into next turn: %+v %v", control, err)
	}

	t.Run("delayed write at same epoch", func(t *testing.T) {
		delayed := AppendRequest{Key: "late", ExpectedSeq: closed.HeadSeq, TurnSeq: turn.InputSeq, FinishTurn: SessionIdle}
		if _, err := store.AppendEvents(ctx, scope, req.SessionID, turn.Lease, false, delayed); contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("previous turn wrote into next: %v", err)
		}
	})
}

func TestVMCancellationClosesUnappliedControl(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope, req := newVMAdmission(t)
	initial, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	turn, err := store.ClaimVMTurn(ctx, scope, req.SessionID, journal.VMTurnClaimRequest{
		TurnSeq: initial.HeadSeq, Generation: 1, ExecutionID: journal.DeterministicExecutionID(scope, req.SessionID, initial.HeadSeq, 1),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	req.DeliveryMode, req.Key = journal.DeliverySteer, "steer"
	steer, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	req.DeliveryMode, req.Key = journal.DeliveryFollowup, "next"
	followup, err := store.EnqueueTurn(ctx, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CancelTurn(ctx, scope, req.SessionID, turn.InputSeq); err != nil {
		t.Fatal(err)
	}
	_, delta, err := store.HeartbeatLeaseWithControl(ctx, scope, req.SessionID, turn.Lease, time.Minute)
	if err != nil || delta == nil || !delta.CancelRequested || delta.TurnSeq != turn.InputSeq {
		t.Fatalf("cancellation delivered to wrong turn: %+v %v", delta, err)
	}
	receipt, err := store.AppendEvents(ctx, scope, req.SessionID, turn.Lease, false, AppendRequest{
		Key: "cancelled", ExpectedSeq: followup.HeadSeq, TurnSeq: turn.InputSeq, FinishTurn: SessionIdle,
	})
	if err != nil || receipt.Control != nil {
		t.Fatalf("cancellation activated a followup: %+v %v", receipt, err)
	}
	events, err := store.GetEvents(ctx, scope, req.SessionID, receipt.HeadSeq-1, 1)
	if err != nil || len(events.Events) != 1 {
		t.Fatalf("terminal event missing: %+v %v", events, err)
	}
	var ended journal.TurnCompleted
	err = json.Unmarshal(events.Events[0].Payload.Inline, &ended)
	if err != nil || ended.Outcome != "INTERRUPTED" || !slices.Equal(ended.CanceledControlSeqs, []int64{steer.HeadSeq, followup.HeadSeq}) {
		t.Fatalf("unapplied input has no terminal disposition: %+v %v", ended, err)
	}
}
