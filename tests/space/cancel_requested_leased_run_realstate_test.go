//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A cancel that arrives while a runner HOLDS the lease is not the asker's to write:
// the fence belongs to the holder, and stamping a status through it from outside is
// the write this project refuses everywhere (see the fence tests next to this file).
// The director's contract for this case (2026-11-09) splits the work in two:
//
//   - the ASKER journals an intent - one append-only event keyed
//     space.CancelRequestedKey(runID), readable by the holder and by any operator;
//   - the HOLDER (this lane) watches its own run's journal, stops on the intent, and
//     closes the run itself with close_reason=cancel_requested, keeping every effect
//     the run already got accepted.
//
// The split matters for what is claimed here: the State-side operation that turns a
// user's Stop into that event is another lane's deliverable, so this fixture writes
// the event with the run's own granted fence - which is exactly the event the real
// operation will append. Everything asserted below is about the holder's behaviour
// once that row exists, and everything is read out of band from ahp_state.runs /
// ahp_state.stream_events, never from the plane that wrote it.
//
// Drain bound: the holder looks once a second (cancelObserveInterval) and the lease it
// renews lives 30s (claimLeaseTTL, PDF rev3.2 s.36 c.36), so a run cannot stay
// leased-and-cancelling without an outcome. The test measures that bound instead of
// trusting it, and it is written so that removing the watcher turns it RED: nothing
// else in the fixture can end the run - the model never returns and MaxSteps is far away.
type cancelIntentCore struct {
	state *space.StateClient
	// intentFor overrides which run id the intent is journaled against. Empty means
	// "this run" (the real case); an explicit other id is the negative control.
	intentFor string
	// journaled receives the durable key as soon as the intent row is written.
	journaled chan string

	mu      sync.Mutex
	failure error
}

func (k *cancelIntentCore) setFailure(err error) {
	k.mu.Lock()
	k.failure = err
	k.mu.Unlock()
}

func (k *cancelIntentCore) Failure() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.failure
}

func (k *cancelIntentCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	// (1) one accepted effect, emitted through the coordinator's own durable path, so
	// the frame whose survival is asserted below was written by THIS run, not by a
	// fixture reaching around it.
	select {
	case results <- space.StepResult{StepType: space.StepModel,
		Content: json.RawMessage(`{"text":"h22 effect accepted before the cancel"}`)}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Wait until that frame is durable before journaling the intent, so the comparison
	// below is "the accepted row across the cancel" and not "a row still in flight".
	effect := space.StepKey(req.RunID, 0)
	if err := k.awaitFrame(ctx, req, effect); err != nil {
		k.setFailure(err)
		return err
	}
	// (2) the cancel intent, in the shared spelling this lane and the State-side
	// operation agreed on (space.CancelRequestedKey).
	target := req.RunID
	if k.intentFor != "" {
		target = k.intentFor
	}
	key := space.CancelRequestedKey(target)
	payload, _ := json.Marshal(map[string]string{
		"requested_by": "s-h22-cancel-asker",
		"note":         "the user pressed Stop while this run held its lease",
	})
	if _, err := k.state.AppendStream(ctx, req.TaskToken, req.ThreadID, req.RunID, req.Worker, req.Epoch,
		key, "cancel_requested", payload); err != nil {
		k.setFailure(err)
		return err
	}
	select {
	case k.journaled <- key:
	case <-ctx.Done():
		return ctx.Err()
	}
	// (3) Nothing else ends this run: no further step is ever produced.
	<-ctx.Done()
	results <- space.StepResult{StepType: space.StepError, Done: true}
	return ctx.Err()
}

func (k *cancelIntentCore) awaitFrame(ctx context.Context, req *space.CoreRequest, key string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		events, err := k.state.ReadStream(ctx, req.TaskToken, req.ThreadID, 0, 200)
		if err != nil {
			return err
		}
		for _, ev := range events {
			if ev.Key == key {
				return nil
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return context.DeadlineExceeded
}

func intentRowOutBand(t *testing.T, threadID, runID, key string) (durableRow, bool) {
	t.Helper()
	for _, row := range streamRowsForRun(t, threadID, runID) {
		if row.Key == key {
			return row, true
		}
	}
	return durableRow{}, false
}

func TestCancelOfALeasedRunIsObservedByTheHolderAndEndsTheRunRealState(t *testing.T) {
	e := cNewEnv(t)
	token := e.signTask(t, "h22-cancel-held")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	journaled := make(chan string, 1)
	core := &cancelIntentCore{state: e.client, journaled: journaled}
	worker := "h22-held-cancel-worker"
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core, WorkerID: worker})

	accepted, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{
		Key: "h22-held-cancel", ContextID: "ctx-h22-held-cancel",
		Input: json.RawMessage(`"work until the user stops you"`), MaxSteps: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatal(err)
	}

	var intentKey string
	select {
	case intentKey = <-journaled:
	case <-time.After(30 * time.Second):
		if f := core.Failure(); f != nil {
			t.Fatalf("fixture could not journal the intent: %v", f)
		}
		t.Fatal("the fixture never journaled a cancel intent")
	}
	runID := accepted.TurnID

	// (1) OBSERVABLE, IMMEDIATELY, AND EARLY. The intent row is in the durable journal
	// the moment the asker wrote it, while the run is still leased and running - that
	// is the difference between "queued" and "lost", which the old silent 409 could
	// not express. Read out of band, not through the plane that served the cancel.
	row, ok := intentRowOutBand(t, accepted.ThreadID, runID, intentKey)
	if !ok {
		t.Fatalf("the cancel intent is not in the durable journal: key=%s", intentKey)
	}
	if row.Kind != "cancel_requested" {
		t.Fatalf("the intent row is kind=%q want cancel_requested", row.Kind)
	}
	if status, closeReason, _ := runRowStatus(t, runID); status == "done" || status == "failed" || status == "canceled" {
		t.Fatalf("the run was already terminal when the intent arrived, so nothing was measured: status=%q close_reason=%q", status, closeReason)
	}

	// (2) TERMINAL WITHIN THE LEASE TTL. The holder itself closes the run, with the
	// reason that names the mechanism, well inside the 30s lease the heartbeat renews.
	deadline := time.Now().Add(25 * time.Second)
	var status, closeReason string
	for time.Now().Before(deadline) {
		status, closeReason, _ = runRowStatus(t, runID)
		if status == "canceled" || status == "done" || status == "failed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	elapsed := 25*time.Second - time.Until(deadline)
	if status != "canceled" {
		t.Fatalf("the holder did not end the cancelled run: status=%q close_reason=%q after %s", status, closeReason, elapsed.Round(time.Millisecond))
	}
	if closeReason != string(space.CloseReasonCancelRequested) {
		t.Fatalf("close_reason=%q want %q: a cancel that had to wait for the holder must be distinguishable from a direct cancel",
			closeReason, space.CloseReasonCancelRequested)
	}
	if elapsed > claimLeaseTTLBound() {
		t.Fatalf("the drain took %s, past the %s lease bound the contract promises", elapsed, claimLeaseTTLBound())
	}

	// (3) THE ACCEPTED EFFECT SURVIVES, BYTE FOR BYTE. One model frame was durable
	// before the intent; the cancel stops the run, it does not undo the run.
	rows := streamRowsForRun(t, accepted.ThreadID, runID)
	effect := space.StepKey(runID, 0)
	frames, terminalFrames := 0, 0
	for _, r := range rows {
		if r.Kind == "status" {
			terminalFrames++
		}
		if r.Key == effect {
			frames++
			if r.Digest == "" {
				t.Fatalf("the accepted effect row has no digest: %+v", r)
			}
		}
	}
	if frames != 1 {
		t.Fatalf("accepted effect %s rows=%d want exactly 1: %+v", effect, frames, rows)
	}
	if terminalFrames != 1 {
		t.Fatalf("terminal status frames=%d want exactly 1: %+v", terminalFrames, rows)
	}
	if _, stillLeased := coord.TurnSnapshot(runID); stillLeased {
		if snap, _ := coord.TurnSnapshot(runID); snap.Status != space.TurnCanceled {
			t.Fatalf("the in-memory turn disagrees with the durable row: %q", snap.Status)
		}
	}

	// (4) NOBODY RESTAMPS A CLOSED RUN. After the holder's own terminal Release the
	// lease is gone, so a stranger's fence - and the holder's own former fence - must
	// both be refused, and the durable row must not move to a verdict nobody earned.
	if code, body := e.raw("Release", token, []byte(`{"run_id":"`+runID+`","worker":"intruder-worker","epoch":99,`+
		`"status":"done","close_reason":"model_finished"}`)); code == 200 {
		t.Fatalf("a stranger's fence completed a canceled run: %v", body)
	}
	if code, body := e.raw("Release", token, []byte(`{"run_id":"`+runID+`","worker":"`+worker+`","epoch":1,`+
		`"status":"done","close_reason":"model_finished"}`)); code == 200 {
		t.Fatalf("the former lease holder completed a canceled run: %v", body)
	}
	if afterStatus, afterReason, _ := runRowStatus(t, runID); afterStatus != "canceled" || afterReason != string(space.CloseReasonCancelRequested) {
		t.Fatalf("a late Release moved the canceled run: %q/%q", afterStatus, afterReason)
	}

	// (5) A REPEAT CANCEL ADDS NOTHING. The same user decision arriving twice must not
	// grow the journal or reopen the row.
	before := len(streamRowsForRun(t, accepted.ThreadID, runID))
	if err := coord.CancelTurn(e.ctx, runID, token); err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	if after := len(streamRowsForRun(t, accepted.ThreadID, runID)); after != before {
		t.Fatalf("a repeat cancel grew the journal from %d to %d rows", before, after)
	}
	if afterStatus, afterReason, _ := runRowStatus(t, runID); afterStatus != "canceled" || afterReason != string(space.CloseReasonCancelRequested) {
		t.Fatalf("a repeat cancel rewrote the verdict: %q/%q", afterStatus, afterReason)
	}
}

// TestCancelRequestedForAnotherRunDoesNotStopThisRunRealState is the control that makes
// the assertion above mean something: an intent journaled for a DIFFERENT run must not
// end this one. Without it the terminal in the test above could be the shape of the
// fixture (a core that returns) rather than a reaction to the right key.
func TestCancelRequestedForAnotherRunDoesNotStopThisRunRealState(t *testing.T) {
	e := cNewEnv(t)
	token := e.signTask(t, "h22-cancel-decoy")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	journaled := make(chan string, 1)
	core := &cancelIntentCore{state: e.client, journaled: journaled, intentFor: "run-that-does-not-exist"}
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core, WorkerID: "h22-decoy-worker"})

	accepted, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{
		Key: "h22-decoy", ContextID: "ctx-h22-decoy",
		Input: json.RawMessage(`"ignore somebody else's cancel"`), MaxSteps: 64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-journaled:
	case <-time.After(30 * time.Second):
		if f := core.Failure(); f != nil {
			t.Fatalf("fixture could not journal the decoy intent: %v", f)
		}
		t.Fatal("the fixture never journaled the decoy intent")
	}
	// Given a run that is running and a journal carrying an intent for ANOTHER run, the
	// only honest outcome after a few watcher ticks is: still running.
	time.Sleep(4 * time.Second)
	status, closeReason, waitFor := runRowStatus(t, accepted.TurnID)
	if status == "canceled" {
		t.Fatalf("a cancel for another run stopped this one: status=%q close_reason=%q", status, closeReason)
	}
	if status != "running" {
		t.Fatalf("expected the run to still be running (it is the only thing that can end it), got %q wait_for=%q", status, waitFor)
	}
	if err := coord.CancelTurn(e.ctx, accepted.TurnID, token); err != nil {
		t.Fatalf("cleanup cancel: %v", err)
	}
}

// claimLeaseTTLBound restates the lease bound the drain must fit inside. It is a literal
// on purpose: a bound read from the same constant the code uses would pass even if that
// constant were raised to a value no user would accept.
func claimLeaseTTLBound() time.Duration { return 30 * time.Second }
