package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// alwaysToolModel always asks for a tool call, so the loop can never terminate on
// its own (no StepDone) and must instead stop at the step ceiling.
type alwaysToolModel struct{ calls int }

func (m *alwaysToolModel) Generate(_ context.Context, _ *ModelRequest) (*ModelResponse, error) {
	m.calls++
	return &ModelResponse{
		Content:   json.RawMessage(`""`),
		ToolCalls: []ToolCall{{ID: "t", Name: "noop"}},
	}, nil
}

// okToolExecutor satisfies any tool call so the loop keeps going until the ceiling.
type okToolExecutor struct{}

func (okToolExecutor) Execute(_ context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{"ok":true}`), nil
}

// TestRuntimeEmitsTerminalAtDefaultStepLimit locks the fix that the terminal
// "step limit reached" step fires even when MaxSteps is unset, i.e. when the loop
// falls back to the default ceiling of 64. Before the fix the terminal was gated on
// req.MaxSteps > 0, so a run with the default limit ran the full 64 model steps and
// then fell out of the loop with NO terminal step and a nil error — a silent
// truncation the caller could not distinguish from a clean stop.
func TestRuntimeEmitsTerminalAtDefaultStepLimit(t *testing.T) {
	const defaultCeiling = 64
	m := &alwaysToolModel{}
	tr := NewToolRegistry(okToolExecutor{})
	tr.Register(ToolDef{Name: "noop"})
	rt := NewRuntime(RuntimeConfig{Model: m, Tools: tr})

	// Generous buffer: emit() is non-blocking (select/default), so an undersized
	// channel would drop the very terminal step this test must observe.
	results := make(chan space.StepResult, 4*defaultCeiling)
	err := rt.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"go"`)}, results)

	if err == nil || !strings.Contains(err.Error(), "step limit") {
		t.Fatalf("Run err = %v, want a step-limit error at the default ceiling", err)
	}
	if m.calls != defaultCeiling {
		t.Fatalf("model called %d times, want %d (default ceiling)", m.calls, defaultCeiling)
	}
	var sawTerminal bool
	for _, r := range drain(results) {
		if r.StepType == space.StepError && r.Done && strings.Contains(string(r.Content), "step limit") {
			sawTerminal = true
		}
	}
	if !sawTerminal {
		t.Fatal("expected a terminal StepError/Done 'step limit reached' step at the default ceiling")
	}
}
