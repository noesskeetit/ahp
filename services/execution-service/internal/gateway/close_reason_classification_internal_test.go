package gateway

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// TestTerminalReasonClassification pins the decision that closes DEF-J4-CHILD-TERMINAL-REASON
// for the transitions a service run cannot be driven into -- above all the effect-ledger replay
// in settleFromLedger, whose door is sealed by operations.Accept today (see the note in
// def_j4_child_terminal_reason_test.go). Testing the classifier instead of faking a path is the
// honest shape: the claim being made is "this is what the reason WOULD be, from these inputs".
//
// Every case below states what an operator reading metadata->>'close_reason' is entitled to
// learn, and the precedence cases are the ones that used to be guesses.
func TestTerminalReasonClassification(t *testing.T) {
	revoked := `{"error":"execbridge: task budget: FORBIDDEN: identity: grant is revoked"}`
	var ledgerPayload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(revoked), &ledgerPayload); err != nil {
		t.Fatalf("fixture is not the payload failAttempt writes: %v", err)
	}

	cases := []struct {
		what     string
		res      ExecResult
		evidence string
		want     string
	}{
		{
			what:     "a replayed failure whose only evidence is the ledger cause",
			res:      ExecResult{State: a2a.TaskStateFailed},
			evidence: ledgerFailureCause(json.RawMessage(revoked)),
			want:     CloseReasonGrantRevoked,
		},
		{
			what: "a failure whose sentence sits in the agent message, not in a report",
			res: ExecResult{State: a2a.TaskStateFailed, Message: a2a.NewMessage(a2a.MessageRoleAgent,
				a2a.NewTextPart("task failed: Post \"https://model.internal/v1/chat\": dial tcp: connect: connection refused"))},
			want: CloseReasonProviderTransport,
		},
		{
			what:     "a cancel",
			res:      ExecResult{State: a2a.TaskStateCanceled},
			evidence: "task cancelled",
			want:     CloseReasonCanceled,
		},
		{
			what: "a completion nobody reported",
			res:  ExecResult{State: a2a.TaskStateCompleted},
			want: CloseReasonUnreported,
		},
		{
			what:     "a budget refusal that also mentions the network stays a budget fact",
			res:      ExecResult{State: a2a.TaskStateFailed},
			evidence: "step limit reached after a connection reset",
			want:     CloseReasonStepLimit,
		},
		{
			what:     "a revoked grant stays revocation even though it is also a FORBIDDEN",
			res:      ExecResult{State: a2a.TaskStateFailed},
			evidence: "FORBIDDEN: identity: an ancestor grant is revoked",
			want:     CloseReasonGrantRevoked,
		},
		{
			what: "an executor report outranks any inference",
			res: ExecResult{State: a2a.TaskStateCompleted,
				Stats:   ExecStats{CloseReason: CloseReasonModelFinished},
				Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("FORBIDDEN: identity: grant is revoked"))},
			evidence: "FORBIDDEN: identity: grant is revoked",
			want:     CloseReasonModelFinished,
		},
		{
			what: "a task that is still running owes no reason yet",
			res:  ExecResult{State: a2a.TaskStateWorking},
			want: "",
		},
	}
	for _, c := range cases {
		if got := terminalCloseReason(c.res, c.evidence); got != c.want {
			t.Errorf("%s: terminalCloseReason = %q, want %q", c.what, got, c.want)
		}
	}

	// The classifier's own precedence, independent of any state: an unmeasured failure with no
	// sentence at all must say so rather than guess.
	if got := ClassifyFailureCause(nil); got != CloseReasonUnreported {
		t.Errorf("ClassifyFailureCause with nothing to read = %q, want %q: guessing is what made the field untrustworthy", got, CloseReasonUnreported)
	}
	if got := ClassifyFailureCause(errors.New("FORBIDDEN: identity: grant does not hold capability model")); got != CloseReasonAuthorityRefused {
		t.Errorf("a capability refusal = %q, want %q: it is neither a revocation nor a model failure", got, CloseReasonAuthorityRefused)
	}
	if got := ClassifyFailureCause(errors.New("prompt rejected by policy")); got != CloseReasonError {
		t.Errorf("a plain model/policy refusal = %q, want %q", got, CloseReasonError)
	}
	// And the ledger payload reader must not invent a cause out of an ExecResult-shaped row or
	// out of garbage: no `error` field means no evidence, not an empty-word classification.
	if got := ledgerFailureCause(json.RawMessage(`{"state":"TASK_STATE_FAILED"}`)); got != "" {
		t.Errorf("ledgerFailureCause on an ExecResult payload = %q, want empty", got)
	}
	if got := ledgerFailureCause(json.RawMessage(`not json`)); got != "" {
		t.Errorf("ledgerFailureCause on an undecodable payload = %q, want empty (and must not panic)", got)
	}
}
