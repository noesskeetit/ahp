package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// turn_failure_reason_test.go — D8: a failed turn has to say WHY in GetTurn.
//
// hive-director 20261101T123847Z §4, assigned to this lane: on the live pin a failed turn
// carried its cause in the thread stream (`event: error` with
// content.error='model returned an empty turn twice', terminal=true) and NOTHING in the JSON
// of GetTurn, which answered `status=failed, steps=1`. Evidence
// reports/director/evidence/d8-getturn-missing-terminal-reason-20261101T1225Z.json.
//
// The cause is not lost inside the runner: internal/core/runtime.go puts it into a StepError
// frame, and the coordinator already has it twice over - in that frame's content and, when Core
// returns an error, in `stepErr` (coordinator.go logs it in a line that says out loud "Core's
// error used to vanish here"). What does not exist is a place to carry it: `space.Turn` has no
// cause field at all, so the projection that answers GetTurn has nothing to say. That is why
// this is a projection defect rather than a logging one, and why fixing it by logging harder
// would leave the user-visible JSON exactly as silent as it is now.
//
// What is being refused: a caller polling a failed turn must not have to re-read and re-derive
// the whole durable stream to learn why its turn died, and the answer must be the SAME cause the
// stream carried - not a paraphrase, and not a status echoed back as its own reason.

const d8Reason = "model returned an empty turn twice"

// emptyTurnCore is the shape the gateway produces when the model answers empty twice: one
// terminal StepError frame carrying the reason, then Run returns (internal/core/runtime.go:304).
type emptyTurnCore struct{}

func (emptyTurnCore) Run(ctx context.Context, _ *space.CoreRequest, results chan<- space.StepResult) error {
	content, err := json.Marshal(map[string]string{"error": d8Reason})
	if err != nil {
		return err
	}
	select {
	case results <- space.StepResult{StepType: space.StepError, Content: content, Done: true}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acceptDispatchPoll reads the turn back over the production HTTP edge until it is terminal.
func acceptDispatchPoll(t *testing.T, base, key, subject string) map[string]any {
	t.Helper()
	code, body := postSpaceAs(t, base, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key, "input": json.RawMessage(`"say something"`),
	}, subject)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d (%s)", code, body)
	}
	turnID, _ := decodeMap(t, body)["turn_id"].(string)
	if turnID == "" {
		t.Fatalf("no turn_id in %s", body)
	}
	if code, body = postSpaceAs(t, base, "DispatchTurn", nil, subject); code != http.StatusOK {
		t.Fatalf("DispatchTurn = %d (%s)", code, body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		code, body = postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": turnID}, subject)
		if code != http.StatusOK {
			t.Fatalf("GetTurn = %d (%s)", code, body)
		}
		turn := decodeMap(t, body)
		if status, _ := turn["status"].(string); status == string(space.TurnFailed) || status == string(space.TurnDone) || status == string(space.TurnCanceled) {
			return turn
		}
		if time.Now().After(deadline) {
			t.Fatalf("turn %s never reached a terminal status: %s", turnID, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestGetTurnFailedNamesTheCauseIsTheDefect: status=failed alone is not an answer.
func TestGetTurnFailedNamesTheCauseIsTheDefect(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, emptyTurnCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, hs.URL, "d8-failed", "alice")
	if status, _ := turn["status"].(string); status != string(space.TurnFailed) {
		t.Fatalf("status = %v, want failed - the double above did not reproduce the shape", turn["status"])
	}
	reason, _ := turn["failure_reason"].(string)
	if reason == "" {
		t.Fatalf("GetTurn answered a failed turn with no cause at all (%v) - the reader is told the "+
			"turn died and never told what killed it, which is D8", turn)
	}
	if reason != d8Reason {
		t.Fatalf("failure_reason = %q, want the cause the stream carried (%q): GetTurn must report the "+
			"reason the run emitted, not a paraphrase of it", reason, d8Reason)
	}
}

// TestGetTurnSucceededCarriesNoCause keeps the field honest in the other direction: a cause
// belongs to a failure. A reason stamped on a healthy turn would be a second status, and the
// two would eventually disagree.
func TestGetTurnSucceededCarriesNoCause(t *testing.T) {
	ms := newMockState()
	hs := sharedSpace(t, ms, artifactCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, hs.URL, "d8-done", "bob")
	if status, _ := turn["status"].(string); status != string(space.TurnDone) {
		t.Fatalf("status = %v, want done (%v)", turn["status"], turn)
	}
	if reason, ok := turn["failure_reason"].(string); ok && reason != "" {
		t.Fatalf("a completed turn carries failure_reason %q - a cause is a fact about a failure only", reason)
	}
}
