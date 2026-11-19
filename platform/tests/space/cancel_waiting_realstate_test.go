//go:build state_integration

package space_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func waitingCancelFixture(t *testing.T) (*cEnv, *space.Coordinator, string, string, string, string) {
	t.Helper()
	e := cNewEnv(t)
	tok := e.signTask(t, "ingress") // claims.Context=ctx-1
	th, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-1", "cancel-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.client.CreateRun(e.ctx, tok, th.ID, "cancel-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "worker", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "worker"})
	coord.RehydrateTurn(space.Turn{ID: run.ID, RunID: run.ID, ThreadID: th.ID, Status: space.TurnRunning, LeaseWorker: claim.Lease.Worker, LeaseEpoch: claim.Lease.Epoch})
	gate, err := coord.SubmitHITL(e.ctx, tok, space.HITLSubmitRequest{TurnID: run.ID, Kind: "approval", Prompt: json.RawMessage(`{"question":"continue?"}`)}, run.ID, claim.Lease.Worker, claim.Lease.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	next, err := e.client.CreateRun(e.ctx, tok, th.ID, "next-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	return e, coord, tok, run.ID, next.ID, gate.ID
}

func TestCancelWaitingRunRealStateUnblocksFIFO(t *testing.T) {
	e, coord, tok, runID, nextID, gateID := waitingCancelFixture(t)
	if err := coord.CancelTurn(e.ctx, runID, tok); err != nil {
		t.Fatalf("parked cancellation: %v", err)
	}
	run, err := e.client.GetRun(e.ctx, tok, runID)
	if err != nil || run.Status != "canceled" {
		t.Fatalf("durable run=%+v err=%v", run, err)
	}
	if err := coord.CancelTurn(e.ctx, runID, tok); err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	gate, err := coord.GetHITL(e.ctx, tok, run.ThreadID, gateID)
	if err != nil || string(gate.Status) != "canceled" {
		t.Fatalf("gate=%+v err=%v", gate, err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "next-worker", 30*time.Second)
	if err != nil || claim == nil || claim.Lease.Run.ID != nextID {
		t.Fatalf("next FIFO run not claimable: %+v %v", claim, err)
	}
	code, stream := e.raw("ReadStream", tok, []byte(`{"thread_id":"`+run.ThreadID+`","after":0,"limit":100}`))
	if code != http.StatusOK {
		t.Fatal(stream)
	}
	count := 0
	for _, ev := range stream["events"].([]any) {
		v := ev.(map[string]any)
		if v["key"] == space.CancelKey(runID) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("cancel terminal events=%d", count)
	}
}

func TestCancelWaitingRunRejectsForeignConversationRealState(t *testing.T) {
	e, _, _, runID, _, _ := waitingCancelFixture(t)
	foreign, err := e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject, Task: "foreign", Context: "other-context", Root: "root", Grant: "grant", Depth: 0}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := e.raw("CancelWaitingRun", foreign, []byte(`{"run_id":"`+runID+`"}`))
	if status != http.StatusForbidden {
		t.Fatalf("foreign conversation cancellation status=%d body=%v", status, body)
	}
}

func TestCanceledGateResponseCannotWakeNextTurnRealState(t *testing.T) {
	e, coord, tok, runID, nextID, gateID := waitingCancelFixture(t)
	if err := coord.CancelTurn(e.ctx, runID, tok); err != nil {
		t.Fatal(err)
	}
	next, err := e.client.ClaimRun(e.ctx, "", "next-worker", 30*time.Second)
	if err != nil || next == nil {
		t.Fatalf("next claim: %v", err)
	}
	if err := e.client.Suspend(e.ctx, next.TaskToken, nextID, next.Lease.Worker, next.Lease.Epoch, "hitl_response", nil); err != nil {
		t.Fatal(err)
	}
	err = coord.RespondHITL(e.ctx, tok, space.HITLRespondRequest{HITLID: gateID, Approved: true}, next.Lease.Thread.ID)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("answer to canceled gate accepted: %v", err)
	}
	run, err := e.client.GetRun(e.ctx, tok, nextID)
	if err != nil || run.Status != "waiting" {
		t.Fatalf("old gate woke next turn: %+v %v", run, err)
	}
}

func TestCancelAcceptedPendingRunIsDurableRealState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "parent")
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}})
	acc, err := coord.AcceptTurn(e.ctx, tok, contracts.Scope{}, space.AcceptTurnRequest{Key: "pending", ContextID: "ctx-1", Input: json.RawMessage(`{"text":"never execute"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.CancelTurn(e.ctx, acc.TurnID, tok); err != nil {
		t.Fatal(err)
	}
	run, err := e.client.GetRun(e.ctx, tok, acc.TurnID)
	if err != nil || run.Status != "canceled" {
		t.Fatalf("pending cancel left durable run %+v %v", run, err)
	}
	if claim, err := e.client.ClaimRun(e.ctx, "", "other-worker", time.Minute); err != nil || claim != nil && claim.Lease != nil {
		t.Fatalf("canceled pending run was claimed: %+v %v", claim, err)
	}
}
