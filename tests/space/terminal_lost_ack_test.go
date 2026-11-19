package space_test

// Block 8 (PDF rev 3.2): потерянный ACK / транзиентный сбой на терминальном кадре.
//
// The terminal status frame is the ONLY durable frame nobody can ever write later: the
// run's lease closes with the run, and after a cancel the run's credential is revoked.
// So one transient failure of that single append permanently leaves the thread's
// durable stream without its terminal event - the durable twin of D4, where a reader
// replaying the stream sees a run that stopped emitting with no closing event.
//
// From the caller's side two very different failures look identical:
//
//	(a) the write was never applied (retry is required, or the frame is lost forever);
//	(b) the write WAS applied and the ACK died (retry is required to learn it, and is
//	    safe because State makes an append idempotent by (key, kind, payload) digest).
//
// So a correct writer retries the SAME append a bounded number of times: (a) converges
// to a durable frame, (b) converges to exactly ONE frame. One attempt and a WARN is
// neither of those. The pair of tests below fails apart if either half is missing.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func lostackScopeOf() contracts.Scope {
	return contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
}

// durableFramesOf reads the durable thread stream straight off the State plane.
func durableFramesOf(t *testing.T, ms *mockState, threadID string) []streamItem {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var out []streamItem
	for _, it := range ms.streams {
		if it.ThreadID == threadID {
			out = append(out, it)
		}
	}
	return out
}

// statusFramesOf lists the durable frames that carry a run status, oldest first.
func statusFramesOf(t *testing.T, ms *mockState, threadID string) []string {
	t.Helper()
	var out []string
	for _, it := range durableFramesOf(t, ms, threadID) {
		if it.Kind != "status" {
			continue
		}
		var v struct {
			Status string `json:"status"`
		}
		if err := json.Unmarshal(it.Payload, &v); err != nil {
			t.Fatalf("durable status frame %s carries an unreadable payload %s: %v", it.Key, it.Payload, err)
		}
		out = append(out, v.Status)
	}
	return out
}

// waitForStatusFrames waits out the writer's retry budget: the assertion is about what
// is DURABLE once the writer has finished trying, not about a lucky interleaving.
func waitForStatusFrames(t *testing.T, ms *mockState, threadID string, want int, timeout time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got []string
	for {
		got = statusFramesOf(t, ms, threadID)
		if len(got) == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func lostackSpace(t *testing.T, core space.CoreRunner) (*space.Coordinator, *mockState) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	t.Cleanup(ms.disarmDroppedACK)
	return space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        core,
		WorkerID:    "w-lostack",
	}), ms
}

// TestTerminalFrameSurvivesTransientFailure: the terminal append fails and NOTHING was
// written. The thread must still end with a durable terminal frame, which means the
// writer has to retry the idempotent append instead of reporting and moving on.
func TestTerminalFrameSurvivesTransientFailure(t *testing.T) {
	coord, ms := lostackSpace(t, &claimableCore{})
	turnID, threadID := acceptTurn(t, coord, "ctx-lostack-transient", "lostack-t-"+contracts.NewID())

	ms.armFailedAppend(1, func(_, kind string) bool { return kind == "status" })
	if err := coord.DispatchTurn(context.Background(), "token", lostackScopeOf()); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if got := waitForStatus(t, ms, turnID, "done", 5*time.Second); got != "done" {
		t.Fatalf("run status = %q, want done", got)
	}

	if got := waitForStatusFrames(t, ms, threadID, 1, 3*time.Second); len(got) != 1 || got[0] != "done" {
		t.Fatalf("one transient failure of the terminal append left %d durable status frames %v, want exactly 1 \"done\": "+
			"no one else can ever write this frame, so the writer must retry it", len(got), got)
	}
}

// TestTerminalFrameSurvivesLostACK: the same transport error, but the write IS durable.
// Retrying must not stack a second terminal frame - State's digest idempotency is what
// makes the retry safe, and the double now models it for exactly this reason.
func TestTerminalFrameSurvivesLostACK(t *testing.T) {
	coord, ms := lostackSpace(t, &claimableCore{})
	turnID, threadID := acceptTurn(t, coord, "ctx-lostack-acked", "lostack-a-"+contracts.NewID())

	ms.armDroppedACK(1, func(_, kind string) bool { return kind == "status" })
	if err := coord.DispatchTurn(context.Background(), "token", lostackScopeOf()); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if got := waitForStatus(t, ms, turnID, "done", 5*time.Second); got != "done" {
		t.Fatalf("run status = %q, want done", got)
	}

	time.Sleep(500 * time.Millisecond) // the whole retry budget, so a duplicate would show
	if got := statusFramesOf(t, ms, threadID); len(got) != 1 || got[0] != "done" {
		t.Fatalf("a lost ACK produced %d durable status frames %v, want exactly 1 \"done\" (retry must be idempotent)", len(got), got)
	}
}

// TestCanceledThreadSurvivesTransientFailure: the cancel half of the same property -
// the canceled state must reach the durable stream even when the first attempt at the
// terminal frame is lost, because after the revoke no one else can write it.
func TestCanceledThreadSurvivesTransientFailure(t *testing.T) {
	core := newSilentCore()
	coord, ms := lostackSpace(t, core)
	turnID, threadID := acceptTurn(t, coord, "ctx-cancelack-transient", "cancelack-t-"+contracts.NewID())
	if err := coord.DispatchTurn(context.Background(), "token", lostackScopeOf()); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-core.started:
	case <-time.After(5 * time.Second):
		t.Fatal("core never started - nothing to cancel")
	}

	ms.armFailedAppend(1, func(_, kind string) bool { return kind == "status" })
	if err := coord.CancelTurn(context.Background(), turnID, "token"); err != nil {
		t.Fatalf("CancelTurn must complete even when the terminal write fails: %v", err)
	}
	ms.disarmDroppedACK()

	if got := waitForStatusFrames(t, ms, threadID, 1, 3*time.Second); len(got) != 1 || got[0] != "canceled" {
		t.Fatalf("a transient failure of the cancel's terminal frame left %d durable status frames %v, want exactly 1 \"canceled\"", len(got), got)
	}
	if st, _ := ms.durableStatusOfThread(threadID); st != "canceled" {
		t.Fatalf("durable run status = %q, want canceled", st)
	}
}

// TestCanceledThreadSurvivesLostACK: cancel's terminal write committed but the ACK died.
// The cancel is a security action and must succeed, and the stream must carry exactly
// one terminal frame.
func TestCanceledThreadSurvivesLostACK(t *testing.T) {
	core := newSilentCore()
	coord, ms := lostackSpace(t, core)
	turnID, threadID := acceptTurn(t, coord, "ctx-cancelack-lost", "cancelack-l-"+contracts.NewID())
	if err := coord.DispatchTurn(context.Background(), "token", lostackScopeOf()); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-core.started:
	case <-time.After(5 * time.Second):
		t.Fatal("core never started - nothing to cancel")
	}

	ms.armDroppedACK(1, func(_, kind string) bool { return kind == "status" })
	if err := coord.CancelTurn(context.Background(), turnID, "token"); err != nil {
		t.Fatalf("CancelTurn must complete even when the ACK is lost: %v", err)
	}
	ms.disarmDroppedACK()

	time.Sleep(500 * time.Millisecond)
	if got := statusFramesOf(t, ms, threadID); len(got) != 1 || got[0] != "canceled" {
		t.Fatalf("a lost ACK on the cancel's terminal frame produced %d durable status frames %v, want exactly 1 \"canceled\"", len(got), got)
	}
	if st, _ := ms.durableStatusOfThread(threadID); st != "canceled" {
		t.Fatalf("durable run status = %q, want canceled", st)
	}
}

// TestTerminalFrameRetriesAreBounded: retries must converge AND terminate. A State that
// keeps refusing the terminal append may not turn the end of a run into an unbounded
// loop - the run still has to be closed durably.
func TestTerminalFrameRetriesAreBounded(t *testing.T) {
	coord, ms := lostackSpace(t, &claimableCore{})
	turnID, threadID := acceptTurn(t, coord, "ctx-lostack-storm", "lostack-s-"+contracts.NewID())

	// Every status append fails, forever (well, for a thousand attempts).
	ms.armFailedAppend(1000, func(_, kind string) bool { return kind == "status" })
	started := time.Now()
	if err := coord.DispatchTurn(context.Background(), "token", lostackScopeOf()); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if got := waitForStatus(t, ms, turnID, "done", 10*time.Second); got != "done" {
		t.Fatalf("run status = %q, want done: the run must still close durably", got)
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("terminal retries took %v - the retry budget is not bounded", elapsed)
	}
	if got := statusFramesOf(t, ms, threadID); len(got) != 0 {
		t.Fatalf("every append failed yet %d status frames are durable %v", len(got), got)
	}
}
