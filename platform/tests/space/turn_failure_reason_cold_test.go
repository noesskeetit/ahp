package space_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// turn_failure_reason_cold_test.go — the boundary D8 named instead of hiding.
//
// D8 (hive-director 20261101T123847Z §4) put the cause into the turn projection, and its own
// doc said the limit out loud: the cause was the LIVE projection, so a turn this process no
// longer remembers reported `status=failed` with no cause at all. That is the same defect the
// director assigned, arriving again one restart later: the durable transcript still carries the
// terminal `error` frame - the reader of GetTurn just refuses to read it.
//
// So this is not a new rule. It is D8's rule applied where D8 left off: for a failed turn
// GetTurn must report the cause the run itself emitted, and the transcript is the single source
// of that cause. Nothing here copies a cause into the run row - two places that say why a run
// died are two places that can disagree, which is the argument the D8 doc already made about
// status. The cold read reads the transcript the SSE reader reads, with the caller's OWN task
// token, and attributes a frame to the turn by the writer's own key form (StepKey: turnID + ":"
// + step), because the event row carries no turn id.
//
// What is refused here, in both directions:
//   - a cold failed turn must not come back cause-less while the transcript names the cause;
//   - a cold read must not INVENT one: no `error` frame for that turn means the answer says the
//     transcript carries no cause, and a transcript that cannot be read says nothing at all
//     rather than guessing. A status is a durable fact; a cause assembled from nothing is not.

// TestColdGetTurnFailedRestoresTheCauseFromTheTranscript is the defect: the same turn, read
// before and after the runner forgets it.
func TestColdGetTurnFailedRestoresTheCauseFromTheTranscript(t *testing.T) {
	ms := newMockState()
	before := sharedSpace(t, ms, emptyTurnCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, before.URL, "d8-cold-failed", "alice")
	turnID, _ := turn["id"].(string)
	if turnID == "" {
		t.Fatalf("no turn id in %v", turn)
	}
	if status, _ := turn["status"].(string); status != string(space.TurnFailed) {
		t.Fatalf("status = %v, want failed - the double above did not reproduce the shape", turn["status"])
	}
	if reason, _ := turn["failure_reason"].(string); reason != d8Reason {
		t.Fatalf("the LIVE projection already lost the cause (%q) - that is D8 itself, not this test's subject", reason)
	}

	// The restart: a coordinator with no memory of this turn, over the SAME durable State.
	after := sharedSpace(t, ms, &stubCore{}, authPerSubject)
	code, body := postSpaceAs(t, after.URL, "GetTurn", map[string]any{"turn_id": turnID}, "alice")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn = %d (%s) - the owner must still read their own turn after a restart", code, body)
	}
	got := decodeMap(t, body)
	if status, _ := got["status"].(string); status != string(space.TurnFailed) {
		t.Fatalf("cold status = %v, want failed (%v)", got["status"], got)
	}
	reason, _ := got["failure_reason"].(string)
	if reason == "" {
		t.Fatalf("a cold GetTurn answers %v with no cause while the durable transcript still carries the "+
			"error frame: the reader is back to re-deriving the whole stream, which is D8 returning on every "+
			"restart", got)
	}
	if reason != d8Reason {
		t.Fatalf("cold failure_reason = %q, want the cause the transcript carries (%q) - the cold read must "+
			"report the run's own reason, never a paraphrase of it", reason, d8Reason)
	}
}

// TestColdGetTurnDoneCarriesNoCause keeps D8's other direction true across a restart: the cold
// path must not go looking for causes on a healthy turn, and must not stamp one there.
func TestColdGetTurnDoneCarriesNoCause(t *testing.T) {
	ms := newMockState()
	before := sharedSpace(t, ms, artifactCore{}, authPerSubject)

	turn := acceptDispatchPoll(t, before.URL, "d8-cold-done", "bob")
	turnID, _ := turn["id"].(string)
	if turnID == "" {
		t.Fatalf("no turn id in %v", turn)
	}
	if status, _ := turn["status"].(string); status != string(space.TurnDone) {
		t.Fatalf("status = %v, want done (%v)", turn["status"], turn)
	}

	after := sharedSpace(t, ms, &stubCore{}, authPerSubject)
	code, body := postSpaceAs(t, after.URL, "GetTurn", map[string]any{"turn_id": turnID}, "bob")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn = %d (%s)", code, body)
	}
	if reason, ok := decodeMap(t, body)["failure_reason"].(string); ok && reason != "" {
		t.Fatalf("a cold completed turn carries failure_reason %q - a cause is a fact about a failure only", reason)
	}
}

// TestColdGetTurnFailedSaysSoWhenTheTranscriptHasNoCauseFrame is the half a permissive fix
// would get wrong. A failed turn whose transcript carries no `error` frame is a real shape: a
// preflight refusal performs NO stream append on purpose, so nothing durable explains it. The
// honest answer is that the durable record carries no cause - stated in words, because an empty
// field puts the reader back at D8 - and it must never be dressed up as the cause itself.
func TestColdGetTurnFailedSaysSoWhenTheTranscriptHasNoCauseFrame(t *testing.T) {
	const want = "space: durable transcript carries no cause frame for this turn"

	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-FAILED-NO-CAUSE", "failed", turnPayload(t, "write a haiku", 8))
	// The terminal status frame is there; the `error` frame is not. Exactly the preflight shape.
	st.seedStreamFrame("TH-1", "RUN-FAILED-NO-CAUSE:0", "status", `{"status":"failed"}`)
	hs := restartEdge(t, st)

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-FAILED-NO-CAUSE"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn = %d (%s)", code, body)
	}
	got := decodeMap(t, body)
	reason, _ := got["failure_reason"].(string)
	if reason == "" {
		t.Fatalf("a cold failed turn answered with no cause at all (%v): the invariant the director asked for "+
			"is that a failed turn is never silent - say that the record carries no cause instead", got)
	}
	if reason != want {
		t.Fatalf("cold failure_reason = %q, want exactly %q: with no error frame there is nothing to quote, so "+
			"the answer must name that limit rather than invent a cause", reason, want)
	}
	if strings.Contains(reason, "failed,steps") {
		t.Fatalf("failure_reason %q restates the status instead of explaining it", reason)
	}
}

// TestColdCauseReadUsesTheCallersOwnToken pins the security shape of the new read. The cold path
// now performs one MORE durable read than it used to, and that read must be authorized by the
// caller's own scope-bound task token - never by a token this process minted for itself, and
// never on behalf of the turn's owner when the caller is somebody else.
func TestColdCauseReadUsesTheCallersOwnToken(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-1", "failed", turnPayload(t, "write a haiku", 8))
	st.seedStreamFrame("TH-1", "RUN-1:0", "error", `{"step_type":"error","content":{"error":"`+d8Reason+`"},"done":true}`)
	hs := restartEdge(t, st)

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("owner cold GetTurn = %d (%s)", code, body)
	}
	if reason, _ := decodeMap(t, body)["failure_reason"].(string); reason != d8Reason {
		t.Fatalf("cold failure_reason = %q, want %q", reason, d8Reason)
	}
	st.mu.Lock()
	var streamCalls []string
	for _, c := range st.calls {
		if strings.HasPrefix(c, "ReadStream as ") {
			streamCalls = append(streamCalls, c)
		}
	}
	st.mu.Unlock()
	if len(streamCalls) == 0 {
		t.Fatalf("the cold cause read never reached State's ReadStream (calls recorded: %v)", streamCalls)
	}
	for _, c := range streamCalls {
		if !strings.HasSuffix(c, " as "+bearerTaskPrefix+"s1") {
			t.Fatalf("the cause was read from State under a credential that is not the caller's own: %q", c)
		}
	}

	// A different subject must still get 404 for the same turn, with the new read in place.
	if code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-1"}, "s2"); code != http.StatusNotFound {
		t.Fatalf("another subject's failed turn = %d (%s), want 404 - the extra read must not widen access", code, body)
	}
}

// TestColdCauseReadStaysSilentWhenTheTranscriptCannotBeRead keeps the new read from becoming a
// liar under a State fault. If the transcript cannot be read, this process does not know whether
// a cause was recorded - so it says nothing. Claiming "the record carries no cause" on a failed
// read would be an invented fact about durable data, and dropping the turn's status with it would
// turn a read outage into a user-visible lie about their own chat.
func TestColdCauseReadStaysSilentWhenTheTranscriptCannotBeRead(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-1", "failed", turnPayload(t, "write a haiku", 8))
	st.streamFault = true
	hs := restartEdge(t, st)

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("GetTurn = %d (%s) - a transcript outage must not take the turn read down with it", code, body)
	}
	got := decodeMap(t, body)
	if status, _ := got["status"].(string); status != "failed" {
		t.Fatalf("status = %v, want failed: the status comes from the Run row and is unaffected by the "+
			"stream outage (%v)", got["status"], got)
	}
	if reason, ok := got["failure_reason"].(string); ok && reason != "" {
		t.Fatalf("failure_reason = %q while the transcript could not be read at all - an unreadable record "+
			"proves nothing, in either direction", reason)
	}
}
