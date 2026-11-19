package space_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// cancel_terminal_turn_test.go — a cancel must not rewrite an outcome that already happened.
//
// The defect is in Coordinator.CancelTurn (internal/space/coordinator.go): it checks that the
// turn EXISTS and says nothing about its status. For a turn that already finished it still runs
// the whole cancel mechanism to the end — revoke the run's grant, append a `canceled` terminal
// frame, Release the run, then stamp the in-memory turn `canceled` UNCONDITIONALLY. The release
// of a run whose lease was already closed by its own completion answers STALE_EPOCH, and that
// answer is precisely the one error the durable-close step tolerates, so the stamp lands anyway.
//
// Two lies come out of one call, both durable-facing:
//
//  (GetTurn)     it answers `canceled` while the run row the SDK reads says `done` — the same
//                 "two places that can disagree about a run" the failure_reason field comment
//                 refuses to create;
//  (transcript)   it appends a `canceled` status frame to the transcript of a run that completed
//                 on its own, so a browser replaying the stream sees the user kill a
//                 conversation that answered.
//
// What is deliberately NOT refused: cancelling an already CANCELED turn stays 200 and idempotent
// (cancel_terminal_frame_reported_test.go:181 pins exactly that — one keyed frame, no second
// write), and a turn this process knows nothing about stays 404. Cancelling is a security action;
// a caller who cannot name the outcome must still be able to stop the work.
//
// The third case below is the race that guard alone does not close: memory says `running`, the
// durable row was closed by someone else. The cancel has already revoked the grant by then, the
// frame append is fenced off, and the release answers STALE_EPOCH — the honest answer is the
// DURABLE status, adopted instead of stamped over.

// cancelFrameKeys lists the durable stream keys of a thread, so a test can prove the cancel
// wrote NOTHING rather than prove it wrote the same thing twice.
func cancelFrameKeys(t *testing.T, ms *mockState, threadID string) []string {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var out []string
	for _, ev := range ms.streams {
		if ev.ThreadID == threadID {
			out = append(out, ev.Key)
		}
	}
	return out
}

// closeRunBehindTheCoordinator states a durable fact directly: the run row is terminal and its
// lease is gone, while the coordinator still believes the turn is running. That is the shape a
// lapsed lease + another writer's close leaves behind, and it cannot be produced through the
// published edge without a second worker and a clock.
func closeRunBehindTheCoordinator(t *testing.T, ms *mockState, runID, status string) {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	r, ok := ms.runs[runID]
	if !ok {
		t.Fatalf("no durable run %s to close", runID)
	}
	r.Status = status
	delete(ms.leases, runID)
}

// TestCancelACompletedTurnIsRefusedAndChangesNothing is the defect: a turn that is `done` has no
// work left to stop, so the cancel must be refused and must leave the turn, the run row and the
// transcript exactly as they were.
func TestCancelACompletedTurnIsRefusedAndChangesNothing(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, artifactCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, hs.URL, "cancel-done", "alice")
	if st, _ := turn["status"].(string); st != string(space.TurnDone) {
		t.Fatalf("status = %v, want done - the fixture above never produced a completed turn", turn["status"])
	}
	turnID, _ := turn["id"].(string)
	threadID, _ := turn["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("a completed turn has to name itself and its thread: %v", turn)
	}
	framesBefore := durableTerminalFrames(t, ms, threadID)
	keysBefore := len(cancelFrameKeys(t, ms, threadID))

	code, body, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusConflict {
		t.Fatalf("cancel of a completed turn = %d (body=%s), want 409: the run already finished, so "+
			"there is nothing to stop, and a 200 reports an outcome the run never had (answer=%v)", code, raw, body)
	}
	if !strings.Contains(strings.ToLower(string(raw)), string(space.TurnDone)) {
		t.Fatalf("the refusal does not name the status it refuses (%s): a caller racing a completion has "+
			"to learn what actually happened to its turn", raw)
	}

	readCode, readBody := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if readCode != http.StatusOK {
		t.Fatalf("GetTurn after the refused cancel = %d (body=%s)", readCode, readBody)
	}
	read := decodeMap(t, readBody)
	if st, _ := read["status"].(string); st != string(space.TurnDone) {
		t.Fatalf("GetTurn after a REFUSED cancel = %v, want %q: the refusal mutated the turn, so the "+
			"caller is told its turn was canceled by a call that reported it failed", read["status"], space.TurnDone)
	}
	if reason, ok := read["failure_reason"].(string); ok && reason != "" {
		t.Fatalf("a refused cancel also invented a failure cause (%q) for a turn that completed", reason)
	}

	if st := durableRunStatus(t, hs.URL, threadID); st != string(space.TurnDone) {
		t.Fatalf("durable run status after the refused cancel = %q, want %q: Space's answer and the run row "+
			"the SDK reads must not be split apart by a call that was refused", st, space.TurnDone)
	}
	if frames := durableTerminalFrames(t, ms, threadID); strings.Contains(strings.Join(frames, ","), string(space.TurnCanceled)) {
		t.Fatalf("a refused cancel wrote a terminal frame into the transcript of a completed run: %v", frames)
	}
	if keys := len(cancelFrameKeys(t, ms, threadID)); keys != keysBefore {
		t.Fatalf("a refused cancel added stream frames: %d keys now, %d before", keys, keysBefore)
	}
	if got, want := strings.Join(durableTerminalFrames(t, ms, threadID), ","), strings.Join(framesBefore, ","); got != want {
		t.Fatalf("the transcript of a completed run changed: %q, was %q", got, want)
	}
}

// TestCancelAFailedTurnIsRefusedAndKeepsItsCause is the same refusal on the other terminal
// status, and it protects the answer this lane just shipped (e894131): a turn that failed with a
// named cause must not have that cause replaced by a cancel that never happened.
func TestCancelAFailedTurnIsRefusedAndKeepsItsCause(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, emptyTurnCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, hs.URL, "cancel-failed", "alice")
	if st, _ := turn["status"].(string); st != string(space.TurnFailed) {
		t.Fatalf("status = %v, want failed - the fixture above never produced a failed turn", turn["status"])
	}
	turnID, _ := turn["id"].(string)
	threadID, _ := turn["thread_id"].(string)

	code, _, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusConflict {
		t.Fatalf("cancel of a failed turn = %d (body=%s), want 409", code, raw)
	}
	if !strings.Contains(strings.ToLower(string(raw)), string(space.TurnFailed)) {
		t.Fatalf("the refusal does not name the status it refuses (%s)", raw)
	}

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if code != http.StatusOK {
		t.Fatalf("GetTurn after the refused cancel = %d (body=%s)", code, body)
	}
	read := decodeMap(t, body)
	if st, _ := read["status"].(string); st != string(space.TurnFailed) {
		t.Fatalf("GetTurn after a refused cancel = %v, want %q", read["status"], space.TurnFailed)
	}
	if reason, _ := read["failure_reason"].(string); reason != d8Reason {
		t.Fatalf("failure_reason after a refused cancel = %q, want the cause the run emitted (%q): a "+
			"refused cancel must not overwrite what killed the turn", reason, d8Reason)
	}
	if st := durableRunStatus(t, hs.URL, threadID); st != string(space.TurnFailed) {
		t.Fatalf("durable run status after the refused cancel = %q, want %q", st, space.TurnFailed)
	}
}

// TestCancelALiveTurnStillWorks is the control that keeps the guard from becoming a wall: a turn
// that never started executing has work that can still be stopped, and cancelling it is a
// security action that must not be held hostage by a status check.
func TestCancelALiveTurnStillWorks(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, &stubCore{blockForever: true}, authPerSubject)
	turnID, threadID := runLiveTurn(t, hs.URL)

	code, body, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusOK {
		t.Fatalf("cancel of a running turn = %d (body=%s), want 200 (answer=%v)", code, raw, body)
	}
	if st := durableRunStatus(t, hs.URL, threadID); st != string(space.TurnCanceled) {
		t.Fatalf("durable run status after a legitimate cancel = %q, want %q", st, space.TurnCanceled)
	}
	if frames := durableTerminalFrames(t, ms, threadID); !strings.Contains(strings.Join(frames, ","), string(space.TurnCanceled)) {
		t.Fatalf("a legitimate cancel left the transcript without its terminal frame: %v", frames)
	}
}

// TestCancelTurnAdoptsTheDurableStatusWhenTheMemoryLags covers the residual
// race: the guard cannot help when memory still says `running` and the durable row was closed by
// somebody else. The grant revoke and the release have already happened by then, and the release
// answer (STALE_EPOCH) is the difference between "I canceled it" and "it was already finished" —
// so that answer has to be READ, not tolerated.
func TestCancelTurnAdoptsTheDurableStatusWhenTheMemoryLags(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, &stubCore{blockForever: true}, authPerSubject)
	turnID, threadID := runLiveTurn(t, hs.URL)

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if code != http.StatusOK {
		t.Fatalf("GetTurn = %d (body=%s)", code, body)
	}
	runID, _ := decodeMap(t, body)["run_id"].(string)
	if runID == "" {
		t.Fatalf("a running turn has to name the run it holds: %s", body)
	}
	closeRunBehindTheCoordinator(t, ms, runID, string(space.TurnDone))

	code, _, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusConflict {
		t.Fatalf("cancel of a turn whose run is durably done = %d (body=%s), want 409: the durable close "+
			"refused this cancel's release, which is the proof that the run was already finished", code, raw)
	}

	code, body = postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if code != http.StatusOK {
		t.Fatalf("GetTurn after the refused cancel = %d (body=%s)", code, body)
	}
	read := decodeMap(t, body)
	if st, _ := read["status"].(string); st == string(space.TurnCanceled) {
		t.Fatalf("the cancel stamped `canceled` over a run that is durably done: the caller is told it "+
			"stopped work it did not stop (%v)", read)
	} else if st != string(space.TurnDone) {
		t.Fatalf("GetTurn after the refused cancel = %v, want the durable status %q adopted, not left "+
			"stale: the run row says the work finished (%s)", read["status"], space.TurnDone, body)
	}
	// Let the run this cancel just stopped actually reach its own terminal decision, then read the
	// turn again. Without the settle, a status written a moment later by the dying execution is
	// invisible to this test, and the assertion would pass on timing instead of on the rule: the
	// first version of this test did exactly that, failing under `-run` and passing unfiltered.
	time.Sleep(250 * time.Millisecond)
	code, body = postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if code != http.StatusOK {
		t.Fatalf("GetTurn after the worker settled = %d (body=%s)", code, body)
	}
	settled := decodeMap(t, body)
	if st, _ := settled["status"].(string); st != string(space.TurnDone) {
		t.Fatalf("GetTurn after the run settled = %v, want the adopted durable status %q to survive: an "+
			"execution that was stopped for an already-closed run must not restamp the turn (%v)",
			settled["status"], space.TurnDone, settled)
	}
	if reason, ok := settled["failure_reason"].(string); ok && reason != "" {
		t.Fatalf("a run stopped for an already-finished turn invented a cause (%q) for a turn State closed as done", reason)
	}
	if frames := durableTerminalFrames(t, ms, threadID); strings.Contains(strings.Join(frames, ","), string(space.TurnCanceled)) {
		t.Fatalf("a refused cancel wrote a `canceled` frame into the transcript of a completed run: %v", frames)
	}
	if !strings.Contains(strings.ToLower(string(raw)), string(space.TurnDone)) {
		t.Fatalf("the refusal does not name the durable status it is adopting (%s): the caller has to see "+
			"which status it lost the race against", raw)
	}
}
