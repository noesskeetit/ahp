//go:build state_integration

package space_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A released lease cannot close a parked run. The separate CancelWaitingRun
// operation requires a credential bound to that exact conversation; an unrelated
// session task cannot use it even when the human subject matches.

// releaseRaw asks the real server to close a run with the given fence and reports what it answered.
func releaseRaw(t *testing.T, e *cEnv, tok, runID, worker string, epoch int64) (int, string) {
	t.Helper()
	// The close this helper attempts is a cancel, and since DEF-RELEASE-REASON-1 a
	// canceled run owes its reason through the release door too. Stating it keeps this
	// gate measuring what it was written to measure - the lease fence and the park -
	// instead of tripping over a missing reason before the fence is ever consulted.
	body, _ := json.Marshal(map[string]any{
		"run_id": runID, "worker": worker, "epoch": epoch, "status": "canceled",
		"close_reason": space.CloseReasonCanceled,
	})
	status, out := e.raw("Release", tok, body)
	return status, fmt.Sprint(out)
}

// claimRaw asks the deployment for the next runnable lease; an empty run id means State handed out nothing.
func claimRaw(t *testing.T, e *cEnv, worker string) (runID string, epoch int64) {
	t.Helper()
	claim, err := e.client.ClaimRun(e.ctx, "", worker, 4*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim == nil || claim.Lease == nil {
		return "", 0
	}
	return claim.Lease.Run.ID, claim.Lease.Epoch
}

// TestParkedRunRejectsStaleReleaseOnRealState preserves the lease fence: only
// a newly claimed lease can use Release, and a parked head blocks later work.
func TestParkedRunRejectsStaleReleaseOnRealState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "run-cancelpark-real")

	thread, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-"+contracts.NewID(), "cp-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run1, err := e.client.CreateRun(e.ctx, tok, thread.ID, "cp-run1-"+contracts.NewID(), json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatalf("CreateRun(run1): %v", err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "w-cancelpark", 4*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim == nil || claim.Lease == nil || claim.Lease.Run.ID != run1.ID {
		t.Fatalf("ClaimRun gave no lease for the run under test: %+v", claim)
	}
	fenceTok, epoch := claim.TaskToken, claim.Lease.Epoch

	// Park it exactly the way SubmitHITL does, and queue a second turn of the same conversation behind it.
	suspendBody, _ := json.Marshal(map[string]any{
		"run_id": run1.ID, "worker": "w-cancelpark", "epoch": epoch, "wait_for": "hitl_response",
	})
	if status, out := e.raw("Suspend", fenceTok, suspendBody); status != http.StatusOK {
		t.Fatalf("Suspend = %d (body=%v)", status, out)
	}
	run2, err := e.client.CreateRun(e.ctx, tok, thread.ID, "cp-run2-"+contracts.NewID(), json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatalf("CreateRun(run2): %v", err)
	}

	// FACT 1: the close of a parked run is refused with the lease the park itself returned.
	if status, body := releaseRaw(t, e, fenceTok, run1.ID, "w-cancelpark", epoch); status == http.StatusOK ||
		!strings.Contains(body, "STALE_EPOCH") {
		t.Fatalf("closing a parked run = %d (body=%v), want a refusal naming STALE_EPOCH: if the close goes "+
			"through, Space's cancel is honest after all and the offline defect is the double's invention", status, body)
	}
	// FACT 2: the row is still parked, so the run is neither closed nor cancellable by that door.
	if run, gerr := e.client.GetRun(e.ctx, fenceTok, run1.ID); gerr != nil ||
		run.Status != "waiting" || run.WaitFor != "hitl_response" {
		t.Fatalf("the refused close left the run %+v (read err %v), want it untouched at (waiting, hitl_response)", run, gerr)
	}
	// FACT 3: there is no way back to a fence, and the whole conversation queues behind the park.
	if got, _ := claimRaw(t, e, "w-cancelpark-later"); got != "" {
		t.Fatalf("ClaimRun handed out %s while the thread's first unfinished run is parked with no deadline; "+
			"that is the run nobody can close, so nothing behind it should move either", got)
	}
	// FACT 4: answering and re-claiming restores a valid Release fence. The
	// separate CancelWaitingRun path is exercised by cancel_waiting_realstate_test.
	wakeBody, _ := json.Marshal(map[string]any{
		"thread_id": thread.ID, "key": "hitl_response:gate-close:release", "kind": "hitl_response",
		"payload": map[string]any{"hitl_id": "gate-close", "approved": false},
	})
	if status, out := e.raw("Enqueue", fenceTok, wakeBody); status != http.StatusOK {
		t.Fatalf("Enqueue(answer) = %d (body=%v)", status, out)
	}
	got, newEpoch := claimRaw(t, e, "w-cancelpark-again")
	if got != run1.ID {
		t.Fatalf("after the park was released ClaimRun produced %q, want the parked run %s back for closing", got, run1.ID)
	}
	if newEpoch == epoch {
		t.Fatalf("the re-claimed lease kept the epoch the park returned (%d); the fence is supposed to move", newEpoch)
	}
	if status, body := releaseRaw(t, e, fenceTok, got, "w-cancelpark-again", newEpoch); status != http.StatusOK {
		t.Fatalf("closing the run after its park was released = %d (body=%v), want 200 - the close needs a live "+
			"lease, not a different operation", status, body)
	}
	// And the queue moves the moment the parked run is closed: the turn behind it is now claimable.
	if next, _ := claimRaw(t, e, "w-cancelpark-next"); next != run2.ID {
		t.Fatalf("after the parked run was closed ClaimRun produced %q, want the blocked run %s: that pair is the "+
			"blast radius of a park nobody can close", next, run2.ID)
	}
}

// TestParkedTurnCancelRejectsUnboundSessionOnRealState verifies that a session
// whose task context differs from the conversation cannot cancel waiting or
// pending work. The authorized conversation-bound positive is covered separately.
func TestParkedTurnCancelRejectsUnboundSessionOnRealState(t *testing.T) {
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(e.srv.URL, e.srv.Client()),
		Core:        &stubCore{blockForever: true},
		WorkerID:    "w-cancelpark",
	})
	hs := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator: coord,
		Auth:        space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
	}))
	t.Cleanup(hs.Close)

	key := "cancelpark-" + contracts.NewID()
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "needs approval"}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(ada) = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn returned no ids: %s", body)
	}
	if c, b := postSpaceAs(t, hs.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coord, turnID, "w-cancelpark")
	ownerTok, _, err := realTaskExchanger{e}.Exchange(e.ctx, space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	gate, err := coord.SubmitHITL(e.ctx, ownerTok, space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, turnID, "w-cancelpark", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}

	// This session task belongs to ctx-task-ada, while the thread has a
	// distinct context. The same human subject does not bypass that binding.
	code, body = postSpaceAs(t, hs.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	if code != http.StatusForbidden {
		t.Fatalf("CancelTurn of a turn parked on its gate = %d (body=%s), want 403: the session task is not bound to this conversation", code, body)
	}
	if !strings.Contains(string(body), "context") {
		t.Fatalf("the refusal says %s: it has to name the credential context mismatch", body)
	}
	run, err := e.client.GetRun(e.ctx, ownerTok, turnID)
	if err != nil {
		t.Fatalf("GetRun after the refused cancel: %v", err)
	}
	if run.Status != "waiting" || run.WaitFor != "hitl_response" {
		t.Fatalf("the refused cancel left the durable run (%q, wait_for=%q), want the park untouched", run.Status, run.WaitFor)
	}
	if c, b := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "ada"); c != http.StatusOK ||
		!strings.Contains(string(b), string(space.TurnAwaiting)) {
		t.Fatalf("GetTurn after the refused cancel = %d (body=%s), want the turn still %q", c, b, space.TurnAwaiting)
	}

	// Answering the gate still releases the run. The session retains no authority
	// to cancel it; a second refusal must refresh the visible durable status.
	if err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: gate.ID, Approved: false, Response: json.RawMessage(`{"note":"rejected after the refused cancel"}`),
	}, threadID); err != nil {
		t.Fatalf("RespondHITL after the refused cancel: %v", err)
	}
	if run, err = e.client.GetRun(e.ctx, ownerTok, turnID); err != nil || run.Status != "pending" {
		t.Fatalf("answering the gate no longer releases the park: run %+v (err %v), want pending", run, err)
	}
	code, body = postSpaceAs(t, hs.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	if code != http.StatusForbidden {
		t.Fatalf("CancelTurn of a released-but-lease-less run = %d (body=%s), want 403: the row says %q but the session still lacks conversation authority", code, body, run.Status)
	}
	if c, b := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "ada"); c != http.StatusOK ||
		!strings.Contains(string(b), "pending") {
		t.Fatalf("GetTurn after the second refusal = %d (body=%s), want the status the row holds (pending): warm and "+
			"cold readers must not disagree about a run this cancel did not close", c, b)
	}
	if run, err = e.client.GetRun(e.ctx, ownerTok, turnID); err != nil || run.Status != "pending" {
		t.Fatalf("a refused cancel rewrote the durable run to %+v (err %v), want it left pending", run, err)
	}
}
