package main

import (
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/profiling/harness"
)

// п.3 / DEF-PROF-2. Empty completion must arrive in the accounting as a NAMED
// terminal failure, not as a blind "generation_failed". Durable fixture taken
// from the profiling database (261 FAILED rows on qwen-38-27b-fp8):
// error="provider_empty_completion: the provider ended the turn with no text
// and no tool calls (finish=length)", output empty, finish_reason=length.
const durableEmptyCompletion = "provider_empty_completion: the provider ended the turn with no text and no tool calls (finish=length)"

func TestDurableEmptyCompletionIsNamedNotBlind(t *testing.T) {
	oc := outcomeFromDurable("FAILED", durableEmptyCompletion, "")
	if oc.Status != "FAILED" || oc.Error != durableEmptyCompletion {
		t.Fatalf("durable row fields lost at the parser boundary: %+v", oc)
	}
	if got := harness.CauseToken(oc.Error); got != "provider_empty_completion" {
		t.Fatalf("cause token = %q, want provider_empty_completion", got)
	}
	a := harness.AccountPhaseOutcomes(1, 0, []harness.Outcome{oc})
	if a.FailureCauses["provider_empty_completion"] != 1 {
		t.Fatalf("artifact cannot name the durable cause: %v", a.FailureCauses)
	}
	if a.FailureCauses["generation_failed"] != 0 {
		t.Fatalf("a named cause must not also be booked as a blind generation_failed: %v", a.FailureCauses)
	}
	if a.LogicalBreakdown["generation_failed/provider_empty_completion"] != 1 {
		t.Fatalf("logical_breakdown not keyed by cause: %v", a.LogicalBreakdown)
	}
	if a.TransportErrors != 0 || a.TerminalFailed != 1 || a.Verdict != "FAIL" {
		t.Fatalf("planes collapsed or failure read as success: %+v", a)
	}
	if oc.OutputLen != nil {
		t.Fatalf("a FAILED row says nothing about output length; the accounting must not infer it: %v", *oc.OutputLen)
	}
}

// A COMPLETED row with no text is the other half of the same trap: it is not a
// working answer, and the verdict must refuse PASS.
func TestDurableCompletedEmptyOutputRefusesPass(t *testing.T) {
	oc := outcomeFromDurable("COMPLETED", "", "")
	if oc.OutputLen == nil || *oc.OutputLen != 0 {
		t.Fatalf("COMPLETED with empty output must be observed as length 0: %+v", oc)
	}
	a := harness.AccountPhaseOutcomes(1, 0, []harness.Outcome{oc})
	if a.CompletedEmptyOutput != 1 || a.Verdict != "FAIL" {
		t.Fatalf("empty answer read as success: %+v", a)
	}
	answered := outcomeFromDurable("COMPLETED", "", "This is a concise ten word sentence for your test.")
	want := len("This is a concise ten word sentence for your test.")
	if answered.OutputLen == nil || *answered.OutputLen != want {
		t.Fatalf("real answer output length lost: %+v, want %d", answered, want)
	}
	if b := harness.AccountPhaseOutcomes(1, 0, []harness.Outcome{answered}); b.Verdict != "PASS" {
		t.Fatalf("a real answer must PASS: %+v", b)
	}
}

// The failure mode in Erdos's RED: reading status only, then guessing.
func TestDurableFailureWithoutErrorStaysUnknown(t *testing.T) {
	a := harness.AccountPhaseOutcomes(1, 0, []harness.Outcome{outcomeFromDurable("FAILED", "", "")})
	if a.FailureCauses["cause_unavailable"] != 1 {
		t.Fatalf("an unread cause must be declared unavailable: %v", a.FailureCauses)
	}
	for cause := range a.FailureCauses {
		if strings.Contains(cause, "provider_empty_completion") {
			t.Fatalf("cause invented from nothing: %v", a.FailureCauses)
		}
	}
	if a.Verdict != "FAIL" {
		t.Fatalf("unknown cause must still fail the phase: %s", a.Verdict)
	}
}
