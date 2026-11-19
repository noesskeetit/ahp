package harness

import (
	"strings"
	"testing"
)

// DEF-PROF-2. The RED (dot-e2e-runtime/20261109/cause_attribution_red_test.go.txt,
// sha256 2ccdf48863d007d698c51aeb0d3121cc44cdecef251c7c4a0a203a463c710820) showed
// the phase accounting could not name the cause the platform had persisted: in
// run 20261109T214813Z-3rf2sj 200+ durable generations carried
// "provider_empty_completion" in ahp_model.generations.error while the artifact
// could only say "generation_failed", so every diagnosis needed a manual
// database autopsy.
//
// The RED as literally written asks a status-only call to produce a cause that
// was never handed to it; going green that way would mean inventing a cause.
// The accounting now accepts outcomes {status, durable error} and keys the
// breakdown by a stable token -- and refuses to name a cause it did not see.

const emptyCompletionError = "provider_empty_completion: the provider ended the turn with no text and no tool calls (finish=length)"

func TestAccountPhaseOutcomesNamesDurableCause(t *testing.T) {
	a := AccountPhaseOutcomes(2, 0, []Outcome{
		{Status: "FAILED", Error: emptyCompletionError},
		{Status: "FAILED", Error: emptyCompletionError, OutputLen: ptr(0)},
	})
	if a.FailureCauses["provider_empty_completion"] != 2 {
		t.Fatalf("durable cause lost: failure_causes=%v", a.FailureCauses)
	}
	if a.LogicalBreakdown["generation_failed/provider_empty_completion"] != 2 {
		t.Fatalf("logical_breakdown is not keyed by the durable cause: %v", a.LogicalBreakdown)
	}
	if a.Verdict != "FAIL" {
		t.Fatalf("failures must never read as PASS: %s", a.Verdict)
	}
	// The transport plane stays a separate fact (DEF-PROF-1).
	if a.TransportErrors != 0 || a.TerminalFailed != 2 || a.LogicalErrors != 2 {
		t.Fatalf("planes collapsed: %+v", a)
	}
	sum := 0
	for _, v := range a.LogicalBreakdown {
		sum += v
	}
	if sum != a.LogicalErrors {
		t.Fatalf("breakdown %d must sum to logical_errors %d", sum, a.LogicalErrors)
	}
}

// An unknown cause is reported as unknown. Naming a cause that was not observed
// is the fabrication this accounting exists to prevent.
func TestStatusOnlyAccountingNeverInventsACause(t *testing.T) {
	a := AccountPhase(2, 0, []string{"FAILED", "FAILED"})
	if _, ok := a.LogicalBreakdown["generation_failed/provider_empty_completion"]; ok {
		t.Fatal("a cause was attributed although no durable error was read")
	}
	if a.FailureCauses["cause_unavailable"] != 2 {
		t.Fatalf("cause-less failures must be named as such: %v", a.FailureCauses)
	}
	if a.LogicalBreakdown["generation_failed"] != 2 || a.Verdict != "FAIL" {
		t.Fatalf("status-only accounting regressed: %+v", a)
	}
}

// "done" with no text is not a working answer.
func TestCompletedWithEmptyOutputIsNotSuccess(t *testing.T) {
	a := AccountPhaseOutcomes(1, 0, []Outcome{{Status: "COMPLETED", OutputLen: ptr(0)}})
	if a.CompletedEmptyOutput != 1 {
		t.Fatalf("empty completion not counted: %+v", a)
	}
	if a.Verdict != "FAIL" || !strings.Contains(strings.Join(a.VerdictReasons, ","), "completed_empty_output") {
		t.Fatalf("an empty answer must not PASS: %+v", a)
	}
	answered := AccountPhaseOutcomes(1, 0, []Outcome{{Status: "COMPLETED", OutputLen: ptr(64)}})
	if answered.Verdict != "PASS" || answered.CompletedEmptyOutput != 0 {
		t.Fatalf("a real answer must PASS: %+v", answered)
	}
	// Output never read is not "output of length zero".
	unread := AccountPhaseOutcomes(1, 0, []Outcome{{Status: "COMPLETED"}})
	if unread.CompletedEmptyOutput != 0 || unread.Verdict != "PASS" {
		t.Fatalf("unread output read as empty: %+v", unread)
	}
}

func TestCauseTokenIsStable(t *testing.T) {
	for _, in := range []string{
		emptyCompletionError,
		"  provider_empty_completion",
		"provider_empty_completion.",
		"PROVIDER_EMPTY_COMPLETION: anything at all",
	} {
		if got := CauseToken(in); got != "provider_empty_completion" {
			t.Errorf("CauseToken(%q)=%q, want provider_empty_completion", in, got)
		}
	}
	if CauseToken("") != "" {
		t.Error("no error text must produce no cause, not a placeholder cause")
	}
}

// The stream terminal frame is where a stream request's cause lives.
func TestStreamOutcomeCarriesDurableCause(t *testing.T) {
	oc := StreamOutcome([]byte(`{"type":"terminal","generation":{"status":"FAILED","error":"` + emptyCompletionError + `","output":""}}`))
	if oc.Status != "FAILED" || CauseToken(oc.Error) != "provider_empty_completion" {
		t.Fatalf("stream cause lost: %+v", oc)
	}
	if oc.OutputLen == nil || *oc.OutputLen != 0 {
		t.Fatalf("stream output length must be read from the row: %+v", oc)
	}
	errFrame := StreamOutcome([]byte(`{"type":"error","message":"budget admission: insufficient budget capacity"}`))
	// The platform writes "<cause>: <detail>", so the token is the text before
	// the first colon -- nothing more may be invented from the detail.
	if errFrame.Status != "FAILED" || CauseToken(errFrame.Error) != "budget_admission" {
		t.Fatalf("error frame cause lost: %+v", errFrame)
	}
}

func ptr(v int) *int { return &v }
