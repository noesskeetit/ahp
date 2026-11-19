package dote2e_test

// Cause attribution for a failed model leg (director directive 2026-11-10).
//
// Both standalone legs used to print a READY-MADE cause for any failure: "answers
// 500 INTERNAL ... ahp_model.budgets stays empty ... failure is inside task-budget
// establishment (internal/execbridge/httpapi.go)". Since then the profile proved a
// different, observed failure mode - provider_empty_completion at
// finish_reason=length under a 64-token ceiling (216 occurrences). A leg that
// fails for the second reason while printing the first is a false attribution, the
// exact class root has already rejected.
//
// modelFailureCause therefore derives the cause from the facts the leg read back
// (transport result, generation status, finish_reason, output bytes). The old
// hypothesis survives only as a labelled historical note, and it claims to match
// only when this run's observed facts actually match it.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// modelObserved is the fact set a model leg can read back. Nothing else may
// influence the cause.
type modelObserved struct {
	err          error
	httpStatus   int
	generationID string
	status       string
	finishReason string
	output       string
	model        string
	maxTokens    int
}

const historicalBudgetHypothesis = "historical note (2026-11-0x, NOT asserted by this run unless it matches): " +
	"SubmitModel answering 500 INTERNAL while ahp_model.budgets stays empty and no generation row is created " +
	"pointed at task-budget establishment (internal/execbridge/httpapi.go TaskBudget -> taskBudgetChain/EnsureTaskTree)"

func modelFailureCause(o modelObserved) map[string]any {
	finish := strings.TrimSpace(o.finishReason)
	emptyOutput := strings.TrimSpace(o.output) == ""
	completed := strings.EqualFold(strings.TrimSpace(o.status), "COMPLETED")

	cause := ""
	diagnosis := ""
	switch {
	case strings.TrimSpace(o.generationID) == "" && o.err != nil:
		switch {
		case o.httpStatus == 500:
			cause = "submit_http_500_no_generation_row"
			diagnosis = fmt.Sprintf("the submit was refused with HTTP %d and no generation row exists, so nothing reached the provider", o.httpStatus)
		case o.httpStatus >= 400 && o.httpStatus < 500:
			cause = fmt.Sprintf("submit_rejected_http_%d", o.httpStatus)
			diagnosis = fmt.Sprintf("the gateway refused the submit with HTTP %d before any generation row was created", o.httpStatus)
		default:
			cause = "submit_failed_before_generation_row"
			diagnosis = "the submit never produced a generation row; the transport/HTTP detail is in observed.error"
		}
	case strings.TrimSpace(o.status) == "":
		cause = "no_generation_status_returned"
		diagnosis = "the platform returned no generation status at all; the leg cannot name a product cause beyond that"
	case !completed:
		cause = "generation_" + strings.ToLower(strings.TrimSpace(o.status))
		diagnosis = fmt.Sprintf("the generation row reached a non-completed terminal status %q", o.status)
	case emptyOutput:
		cause = "provider_empty_completion"
		diagnosis = fmt.Sprintf("the generation completed with NO text (finish_reason=%q at max_tokens=%d): the provider answered empty, or the ceiling consumed the completion", finish, o.maxTokens)
	default:
		if finish == "length" {
			cause = "answer_truncated_at_max_tokens"
			diagnosis = fmt.Sprintf("the answer ran into the token ceiling (finish_reason=length, max_tokens=%d) before carrying the expected marker", o.maxTokens)
		} else {
			cause = "model_answer_missing_marker"
			diagnosis = fmt.Sprintf("a COMPLETED generation with %d output bytes did not carry the expected marker (finish_reason=%q)", len(o.output), finish)
		}
	}

	matchesHistorical := cause == "submit_http_500_no_generation_row"
	return map[string]any{
		"cause":        cause,
		"cause_source": "derived from the transport result and the generation row this leg read back",
		"diagnosis":    diagnosis,
		"observed": map[string]any{"generation": o.generationID, "status_value": o.status,
			"finish_reason": finish, "output_bytes": len(o.output), "max_tokens": o.maxTokens,
			"http_status": o.httpStatus, "model": o.model, "error": errText(o.err)},
		// A ceiling the TEST asked for must be visible as a candidate cause, so a
		// test-configuration failure is never presented as a product defect.
		"test_config_ceiling_suspected":    finish == "length",
		"historical_note":                  historicalBudgetHypothesis,
		"historical_note_matches_this_run": matchesHistorical,
	}
}

// --- RED tests (pure: no Docker, no PG, no model) ---

func TestEmptyCompletionIsAttributedToObservedFacts(t *testing.T) {
	cause := modelFailureCause(modelObserved{generationID: "gen-1", status: "COMPLETED",
		// The model label is opaque input here: the leg attributes a cause from the generation
		// row it read back, and this fixture travels into observed.model unchanged. Naming the
		// routed identity would add a second carrier of it without testing anything.
		finishReason: "length", output: "", model: "generation-row model (fixture)", maxTokens: 1024})
	if cause["cause"] != "provider_empty_completion" {
		t.Fatalf("an empty completion must be attributed to what was observed, got %v", cause["cause"])
	}
	// The narrative may exist in this map in exactly one place: the note that is
	// labelled as a note. Scanning the whole map would make this assertion and the
	// "the historical note must be labelled as a note" assertion below mutually
	// unsatisfiable - which is how this test went red on its own after the note was
	// added. So the scan is narrowed to the fields that ASSERT something (cause,
	// cause_source, diagnosis, observed) and the note is checked separately, for
	// being quarantined rather than for being absent.
	claimFields := fmt.Sprint(cause["cause"], cause["cause_source"], cause["diagnosis"], cause["observed"])
	for _, forbidden := range []string{"execbridge", "budgets", "500 INTERNAL", "EnsureTaskTree"} {
		if strings.Contains(claimFields, forbidden) {
			t.Fatalf("the derived cause must not carry the historical narrative (%q) when the facts do not: %s", forbidden, claimFields)
		}
	}
	if strings.Contains(fmt.Sprint(cause["cause"]), fmt.Sprint(cause["historical_note"])) {
		t.Fatal("the historical note must not be merged into the cause")
	}
	if cause["historical_note_matches_this_run"] != false {
		t.Fatal("the historical hypothesis must declare that it does NOT match this run")
	}
	if !strings.Contains(fmt.Sprint(cause["historical_note"]), "NOT asserted by this run") {
		t.Fatal("the historical note must be labelled as a note")
	}
	if cause["test_config_ceiling_suspected"] != true {
		t.Fatal("finish_reason=length must flag the token ceiling as a candidate cause")
	}
	observed, _ := cause["observed"].(map[string]any)
	if observed["finish_reason"] != "length" || observed["max_tokens"] != 1024 {
		t.Fatalf("the observed facts must travel with the cause, got %v", observed)
	}
}

func TestHttp500WithoutGenerationRowIsTheOnlyShapeNamingTheHistoricalDefect(t *testing.T) {
	cause := modelFailureCause(modelObserved{err: errors.New("boom"), httpStatus: 500,
		generationID: "", maxTokens: 1024})
	if cause["cause"] != "submit_http_500_no_generation_row" {
		t.Fatalf("HTTP 500 with no generation row must name that shape, got %v", cause["cause"])
	}
	if cause["historical_note_matches_this_run"] != true {
		t.Fatal("the historical note may claim a match only for this observed shape")
	}
	rejection := modelFailureCause(modelObserved{generationID: "gen-2", status: "COMPLETED",
		finishReason: "stop", output: "hello", maxTokens: 1024})
	if rejection["cause"] == "submit_http_500_no_generation_row" ||
		rejection["historical_note_matches_this_run"] == true {
		t.Fatal("a completed generation must never borrow the 500/budget attribution")
	}
}

func TestTruncationAtTheCeilingIsFlaggedAsPossibleTestConfig(t *testing.T) {
	cause := modelFailureCause(modelObserved{generationID: "gen-3", status: "COMPLETED",
		finishReason: "length", output: "partial answer", maxTokens: 64})
	if cause["cause"] != "answer_truncated_at_max_tokens" {
		t.Fatalf("a length-truncated answer must name the ceiling, got %v", cause["cause"])
	}
	if cause["test_config_ceiling_suspected"] != true {
		t.Fatal("the leg's own ceiling must be visible as a candidate cause, not hidden behind a product diagnosis")
	}
}

func TestUnknownShapeNamesItselfAndBorrowsNoNarrative(t *testing.T) {
	cause := modelFailureCause(modelObserved{generationID: "gen-4", status: "FAILED",
		finishReason: "", output: "", maxTokens: 1024})
	if cause["cause"] != "generation_failed" {
		t.Fatalf("a non-completed terminal must be reported as itself, got %v", cause["cause"])
	}
	if strings.Contains(fmt.Sprint(cause["diagnosis"]), "execbridge") {
		t.Fatal("the diagnosis must stay inside the observed facts")
	}
}

// TestStandaloneLegsDoNotRunAtThe64TokenCeiling is a static anti-regression: the
// 64-token ceiling that produced the empty completions must not come back into
// either standalone leg.
func TestStandaloneLegsDoNotRunAtThe64TokenCeiling(t *testing.T) {
	if customAgentLegMaxTokens < 1024 {
		t.Fatalf("standalone legs must ask for at least the accepted benchmark ceiling, got %d", customAgentLegMaxTokens)
	}
	goSource, err := os.ReadFile(filepath.Join("customagent_sdk_test.go"))
	if err != nil {
		t.Fatalf("read the Go leg: %v", err)
	}
	if strings.Contains(string(goSource), "MaxTokens: 64") {
		t.Fatal("the Go standalone leg is back at the 64-token ceiling")
	}
	if !strings.Contains(string(goSource), "MaxTokens: customAgentLegMaxTokens") {
		t.Fatal("the Go standalone leg must use the named constant")
	}
	pySource, err := os.ReadFile(filepath.Join("customagent", "python", "custom_agent.py"))
	if err != nil {
		t.Fatalf("read the Python leg: %v", err)
	}
	if strings.Contains(string(pySource), "max_tokens=64") {
		t.Fatal("the Python standalone leg is back at the 64-token ceiling")
	}
	if !strings.Contains(string(pySource), "LEG_MAX_TOKENS = 1024") {
		t.Fatal("the Python standalone leg must use the named constant")
	}
	if !strings.Contains(string(pySource), "model_failure_cause(") {
		t.Fatal("the Python leg must derive its cause from observed facts, not print a remembered one")
	}
}
