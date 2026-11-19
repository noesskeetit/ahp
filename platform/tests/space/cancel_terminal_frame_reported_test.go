package space_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// cancel_terminal_frame_reported_test.go — the SECOND cancel mechanism, measured and
// made visible (vm2-a2a-multinode, forum 20261101T104800Z §1 item 4; hive-deputy
// 20261101T110811Z §4: "закрытым НЕ считай: проверь его своим тестом на байтах B и
// зафиксируй результат").
//
// What production measured: CancelTurn revokes the run's own grant, and the terminal
// frame of the thread's durable stream was then written with the credential Identity
// had just killed -- `WARN space: terminal status event not durable err=state:
// FORBIDDEN: identity: grant is revoked`. 4c7ea24 moved that write into CancelTurn
// itself, presented with the CALLER's still-live token, and cancel_durable_close_test.go
// pins the revoke/close ordering with a State double that refuses writes the way
// Identity does (erunState.forbidAppends).
//
// What stayed silent, and is the actual hole this file closes: when THAT write is
// refused too -- any refusal, not a transport blip -- the cancel answered a bare
// {"ok":true} while the thread's durable transcript had no terminal frame at all. The
// browser is not hung, because sse.go closes a thread whose durable runs are all
// terminal; but a client reading State directly (the SDK, the second deployment, a
// transcript-vs-checkpoint diff) gets a stream that simply stops, and nobody was told.
// The cancel still has to complete -- revoking a credential is a security action that a
// journal failure must not hold hostage -- so the fix is not to fail the cancel, it is to
// stop lying by omission: the answer says whether the durable terminal frame exists.

// runLiveTurn parks a turn mid-execution (the Core never returns) and waits until the
// DURABLE run row says running, so the cancel below races a live run rather than a
// turn that was never claimed.
func runLiveTurn(t *testing.T, base string) (turnID, threadID string) {
	t.Helper()
	code, body := postSpaceAs(t, base, "AcceptTurn", map[string]any{
		"key":        "ctf-" + contracts.NewID(),
		"context_id": "ctx-" + contracts.NewID(),
		"input":      map[string]any{"role": "user", "content": "hold forever"},
		"max_steps":  3,
	}, "alice")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ = acc["turn_id"].(string)
	threadID, _ = acc["thread_id"].(string)
	if c, b := postSpaceAs(t, base, "DispatchTurn", nil, "alice"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, b := postSpaceAs(t, base, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
		if c != http.StatusOK {
			t.Fatalf("ListRunHistory = %d (body=%s)", c, b)
		}
		runs, _ := decodeMap(t, b)["runs"].([]any)
		if len(runs) == 1 {
			if st, _ := runs[0].(map[string]any)["status"].(string); st == "running" {
				return turnID, threadID
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the run never reached durable status running - nothing to cancel")
		}
		time.Sleep(15 * time.Millisecond)
	}
}

// durableTerminalFrames counts the thread's DURABLE status frames. It reads the stored
// rows, not the SSE projection: the projection synthesizes `event: end` out of the run
// store, and a synthesized frame is exactly what must not be counted here.
func durableTerminalFrames(t *testing.T, ms *mockState, threadID string) []string {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var out []string
	for _, ev := range ms.streams {
		if ev.ThreadID != threadID || ev.Kind != "status" {
			continue
		}
		var p map[string]any
		if json.Unmarshal(ev.Payload, &p) == nil {
			if st, _ := p["status"].(string); st != "" {
				out = append(out, st)
			}
		}
	}
	return out
}

func cancelTurnAs(t *testing.T, base, turnID string) (int, map[string]any, []byte) {
	t.Helper()
	code, body := postSpaceAs(t, base, "CancelTurn", map[string]any{"turn_id": turnID}, "alice")
	return code, decodeMap(t, body), body
}

// TestCancelTurnReportsWhenTheTerminalFrameIsNotDurable is the measured hole: every
// terminal-frame write is refused (the arm is permanent, so the retry of an idempotent
// write cannot rescue it either), the cancel must still complete, and the answer must
// say out loud that the durable transcript did not get its terminal frame.
func TestCancelTurnReportsWhenTheTerminalFrameIsNotDurable(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, &stubCore{blockForever: true}, authPerSubject)
	turnID, threadID := runLiveTurn(t, hs.URL)

	// 1000, not -1: the double counts down and stops failing at zero, so a negative
	// arm would fail NOTHING and the test would pass on a lie. This is the same
	// "permanent for the life of the test" idiom terminal_lost_ack_test.go:207 uses.
	ms.armFailedAppend(1000, func(_, kind string) bool { return kind == "status" })
	defer ms.disarmDroppedACK()

	code, body, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusOK {
		t.Fatalf("CancelTurn = %d, want 200 - revoking a credential must not be held hostage by a journal failure (body=%s)", code, raw)
	}
	if ok, _ := body["ok"].(bool); !ok {
		t.Fatalf("CancelTurn body must report the cancel itself as done, got %s", raw)
	}
	durable, present := body["terminal_frame_durable"]
	if !present {
		t.Fatalf("CancelTurn said %q with no word about the terminal frame that could not be written", raw)
	}
	if durable == true {
		t.Fatalf("the answer claims a durable terminal frame while every status append was refused: %s", raw)
	}

	// The cancel itself must have happened, and happened durably: security first.
	if st := durableRunStatus(t, hs.URL, threadID); st != "canceled" {
		t.Fatalf("durable run status after the cancel = %q, want canceled", st)
	}
	if frames := durableTerminalFrames(t, ms, threadID); len(frames) != 0 {
		t.Fatalf("the refusal was injected but the durable stream still has terminal frames %v", frames)
	}
}

// TestCancelTurnReportsTheDurableTerminalFrame is the positive control, and the reason
// the negative case above means anything: when the write is accepted, the durable stream
// really holds the canceled frame and the answer says so. On debf8b1 (no cancel-side
// journal at all) this is RED for the right reason.
func TestCancelTurnReportsTheDurableTerminalFrame(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, &stubCore{blockForever: true}, authPerSubject)
	turnID, threadID := runLiveTurn(t, hs.URL)

	code, body, raw := cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusOK {
		t.Fatalf("CancelTurn = %d, want 200 (body=%s)", code, raw)
	}
	if durable, _ := body["terminal_frame_durable"].(bool); !durable {
		t.Fatalf("the terminal frame was accepted by State, yet the answer says it is not durable: %s", raw)
	}
	frames := durableTerminalFrames(t, ms, threadID)
	if len(frames) != 1 || frames[0] != "canceled" {
		t.Fatalf("durable status frames of a canceled thread = %v, want exactly [canceled]", frames)
	}

	// First let the parked worker actually reach its own terminal decision, so what is being
	// measured is the claim and not a race. The worker's terminal path reads
	// turn.cancelTerminalDurable under the coordinator lock (coordinator.go:830) to decide
	// whether to write its own terminal frame, and a REPEAT cancel rewrites that flag with
	// the outcome of its own append. Without settling, "one frame" could pass simply because
	// the worker had not been scheduled yet, and the same run could later fail for a reason
	// that has nothing to do with the seam. The wait is for a local goroutine, not a
	// timeout standing in for an assertion: the assertion is the frame count, before and
	// after the repeat.
	time.Sleep(250 * time.Millisecond)
	if frames := durableTerminalFrames(t, ms, threadID); len(frames) != 1 || frames[0] != "canceled" {
		t.Fatalf("the parked worker's own terminal path changed the durable frames to %v; a canceled "+
			"turn must have exactly one terminal frame, written by whoever got there first", frames)
	}

	// And exactly one frame stays after a repeat cancel: the cancel is idempotent, and its
	// journal write is keyed, so the second call must not append a second terminal frame.
	code, _, _ = cancelTurnAs(t, hs.URL, turnID)
	if code != http.StatusOK {
		t.Fatalf("a second cancel of the same turn = %d, want 200 (idempotent)", code)
	}
	if frames := durableTerminalFrames(t, ms, threadID); len(frames) != 1 || frames[0] != "canceled" {
		t.Fatalf("repeat cancel changed the durable terminal frames to %v", frames)
	}
}

// durableRunStatus reads the durable status of the thread's single run through the
// published edge. It is not called runStatus because that name already belongs to a
// helper reading the double's run table directly (orphan_claim_test.go:76).
func durableRunStatus(t *testing.T, base, threadID string) string {
	t.Helper()
	c, b := postSpaceAs(t, base, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
	if c != http.StatusOK {
		t.Fatalf("ListRunHistory = %d (body=%s)", c, b)
	}
	runs, _ := decodeMap(t, b)["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("want exactly one durable run, got %d", len(runs))
	}
	st, _ := runs[0].(map[string]any)["status"].(string)
	return st
}
