package gateway

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestResolveStepBudget(t *testing.T) {
	t.Parallel()
	ok := []struct {
		name      string
		req, msg  map[string]any
		ceiling   int
		wantSteps int
		wantSrc   string
	}{
		{"nothing stated falls to the deployment bound", nil, nil, 4, 0, BudgetSourceDeployment},
		{"caller budget at the request level is honored", map[string]any{"max_steps": float64(8)}, nil, 16, 8, BudgetSourceCaller},
		{"caller budget on the message is honored", nil, map[string]any{"max_steps": float64(6)}, 16, 6, BudgetSourceCaller},
		{"agreeing request and message is not a conflict", map[string]any{"max_steps": float64(8)}, map[string]any{"max_steps": float64(8)}, 16, 8, BudgetSourceCaller},
		{"below the ceiling is the caller's number, not the ceiling", map[string]any{"max_steps": float64(2)}, nil, 4, 2, BudgetSourceCaller},
		{"no ceiling configured leaves the stated budget as given", map[string]any{"max_steps": float64(64)}, nil, 0, 64, BudgetSourceCaller},
		{"json.Number decodes as an integer budget", map[string]any{"max_steps": json.Number("9")}, nil, 16, 9, BudgetSourceCaller},
		{"int survives a programmatic caller", map[string]any{"max_steps": 5}, nil, 16, 5, BudgetSourceCaller},
	}
	for _, tc := range ok {
		got, err := resolveStepBudget(tc.req, tc.msg, tc.ceiling)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", tc.name, err)
		}
		if got.Steps != tc.wantSteps || got.Source != tc.wantSrc {
			t.Fatalf("%s: got %+v, want steps=%d source=%s", tc.name, got, tc.wantSteps, tc.wantSrc)
		}
	}

	bad := []struct {
		name string
		req  map[string]any
		msg  map[string]any
	}{
		{"zero steps is not a bound", map[string]any{"max_steps": float64(0)}, nil},
		{"negative steps is not a bound", map[string]any{"max_steps": float64(-3)}, nil},
		{"a fractional budget is not a step count", map[string]any{"max_steps": float64(2.5)}, nil},
		{"a string budget is not a number", map[string]any{"max_steps": "8"}, nil},
		{"a boolean budget is not a number", map[string]any{"max_steps": true}, nil},
		{"a disagreeing request and message has no honest answer", map[string]any{"max_steps": float64(4)}, map[string]any{"max_steps": float64(9)}},
	}
	for _, tc := range bad {
		if _, err := resolveStepBudget(tc.req, tc.msg, 0); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("%s: must fail as ErrInvalid (JSON-RPC -32602), got %v", tc.name, err)
		}
	}
}

// A budget above the operator's ceiling must be refused, never clamped: the
// alternative is that a caller believes it asked for 12 and the run gets 4.
func TestStepBudgetAboveCeilingIsRefused(t *testing.T) {
	t.Parallel()
	_, err := resolveStepBudget(map[string]any{"max_steps": float64(12)}, nil, 4)
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("over-ceiling budget must be ErrInvalid (-32602), got %v", err)
	}
	for _, want := range []string{"12", "4"} {
		if got := err.Error(); !strings.Contains(got, want) {
			t.Fatalf("refusal must name both the request and the ceiling (%q missing in %q)", want, got)
		}
	}
}

func TestMergeAttemptStats(t *testing.T) {
	t.Parallel()
	if got := mergeAttemptStats(ExecResult{}, BudgetSourceDeployment, 1); len(got) != 0 {
		t.Fatalf("an executor that reported nothing must not fabricate stats, got %v", got)
	}

	got := mergeAttemptStats(ExecResult{Stats: ExecStats{StepsConsumed: 3, StepBudget: 3, CloseReason: CloseReasonStepLimit}},
		BudgetSourceCaller, 2)
	if got[MetadataStepsConsumed] != 3 || got[MetadataCloseReason] != CloseReasonStepLimit || got[MetadataAttempt] != 2 {
		t.Fatalf("stats must be journalled under their documented keys, got %v", got)
	}

	// The requested budget and the effective bound are different keys. Overwriting
	// the caller's `max_steps` with the number that actually ran is exactly the
	// substitution this whole path exists to make visible.
	if _, polluted := got[MetadataStepBudget]; polluted {
		t.Fatalf("attempt stats must not overwrite the caller's max_steps: %v", got)
	}
	if got[MetadataStepBudgetEffective] != 3 {
		t.Fatalf("the effective bound must be journalled under %s: %v", MetadataStepBudgetEffective, got)
	}

	// Without a report the source is derived from what the task already says.
	if src := budgetSourceOf(map[string]any{MetadataStepBudgetSource: BudgetSourceCaller}); src != BudgetSourceCaller {
		t.Fatalf("a caller-recorded task must keep its source, got %q", src)
	}
	for _, md := range []map[string]any{nil, {}, {MetadataStepBudget: 6}} {
		if src := budgetSourceOf(md); src != BudgetSourceDeployment {
			t.Fatalf("a task with no recorded caller must read as deployment-bound, got %q", src)
		}
	}
}
