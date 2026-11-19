package core

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// run_binding_budget_test.go — the step budget has to reach the tool that delegates.
//
// hive-director 20261101T113922Z row 4: "бюджет не пропускается вниз", owned here jointly
// with vm2-a2a-multinode (ЗАМ 20261101T115132Z). vm2-a2a-multinode 20261101T115659Z §2
// asked for one exact shape, and this file pins that shape: MaxSteps plus MaxStepsStated,
// measured (a) stated, (b) unstated, (c) unchanged after the run started.
//
// Before this file the number lived only on space.CoreRequest: internal/core/runtime.go
// stamps the tool context with the run's scope and its RunBinding, and a tool that calls a
// SECOND DEPLOYMENT had nothing to forward. The lane producing the wire metadata today
// (adapters/emil/step_budget.go, owned by vm2-a2a-multinode) carries a context stamp as a
// stopgap and says out loud that it exists only until RunBinding carries the budget
// natively. The number belongs to the binding the runtime already injects, not to a
// per-adapter side channel.
//
// The wire contract already exists on the execution plane: internal/gateway/budget.go
// publishes MetadataStepBudget ("max_steps"), refuses a value below 1 ("must be a positive
// whole number of steps"), and journalled step_budget_source precisely because "8 asked, 4
// effective" is otherwise invisible after the fact.
//
// What is being refused here, in both directions:
//   - silence. A delegation that says nothing about the budget cannot be audited against
//     the caller's request;
//   - laundering. The loop's own fallback ceiling (runtime.go, 64 steps when nothing was
//     asked for) is a fact about THIS life of THIS loop, not a number the caller stated,
//     and it must not be forwarded as if it were: the child's deployment default is the
//     honest answer to an unstated budget. That is also why an int alone is not enough and
//     MaxStepsStated exists - a child must be able to tell "the caller said 64" from
//     "nobody said anything" without the platform having to forbid an explicit 64.

// budgetCatcher is the delegating tool: it records what the runtime put on the context of
// every call, which is the only thing a tool that forwards a budget can read.
type budgetCatcher struct {
	mu       sync.Mutex
	bindings []RunBinding
}

func (c *budgetCatcher) Execute(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	binding, ok := RunBindingFromContext(ctx)
	if !ok {
		binding = RunBinding{}
	}
	c.bindings = append(c.bindings, binding)
	return json.RawMessage(`{"ok":true}`), nil
}

func (c *budgetCatcher) seen() []RunBinding {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RunBinding(nil), c.bindings...)
}

// toolThenDone emits the configured number of tool calls and then finishes, so every
// captured context belongs to a real tool call inside a real run.
type toolThenDone struct {
	toolCalls int
	calls     int
}

func (m *toolThenDone) Generate(_ context.Context, _ *ModelRequest) (*ModelResponse, error) {
	m.calls++
	if m.calls <= m.toolCalls {
		return &ModelResponse{Content: json.RawMessage(`""`),
			ToolCalls: []ToolCall{{ID: "c1", Name: "delegate"}}}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"finished"`)}, nil
}

// runToolCalls runs the shape every delegating turn has: one run, N tool calls.
func runToolCalls(t *testing.T, req *space.CoreRequest, toolCalls int) []RunBinding {
	t.Helper()
	catcher := &budgetCatcher{}
	reg := NewToolRegistry(catcher)
	reg.Register(ToolDef{Name: "delegate"})
	rt := NewRuntime(RuntimeConfig{Model: &toolThenDone{toolCalls: toolCalls}, Tools: reg})

	results := make(chan space.StepResult, 16)
	done := make(chan error, 1)
	go func() {
		done <- rt.Run(context.Background(), req, results)
		close(results)
	}()
	for range results { //nolint:revive // the steps are not what is under test here
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	bindings := catcher.seen()
	if len(bindings) != toolCalls {
		t.Fatalf("the tool ran %d times, want exactly %d - nothing was captured to read", len(bindings), toolCalls)
	}
	for i, b := range bindings {
		if b.ContextID == "" && b.TaskID == "" {
			t.Fatalf("call %d had no RunBinding on its context at all - a delegating tool cannot name its run", i)
		}
	}
	return bindings
}

// TestRunBindingCarriesTheStatedStepBudget is the missing forwarding step: the caller
// stated a bound for this run, so a tool that delegates inside the run can repeat it
// instead of staying silent, and can say that the caller stated it.
func TestRunBindingCarriesTheStatedStepBudget(t *testing.T) {
	bindings := runToolCalls(t, &space.CoreRequest{
		Input:    json.RawMessage(`"go"`),
		RunID:    "run-budget-1",
		MaxSteps: 7,
	}, 1)
	binding := bindings[0]
	if binding.TaskID != "run-budget-1" {
		t.Fatalf("binding.TaskID = %q, want the run the tool call belongs to", binding.TaskID)
	}
	if binding.MaxSteps != 7 {
		t.Fatalf("binding.MaxSteps = %d, want the 7 the caller asked for - a delegation that "+
			"forwards anything else changes the budget the user stated", binding.MaxSteps)
	}
	if !binding.MaxStepsStated {
		t.Fatal("binding.MaxStepsStated = false for a run whose caller stated 7; the child would " +
			"be told step_budget_source=deployment for a number its caller named")
	}
}

// TestRunBindingKeepsAnUnstatedBudgetUnstated is the other half, and the reason the
// assertion above is not just "copy the number the loop happens to use": with nothing
// asked for, the loop still bounds itself with its own default, while the binding must
// keep saying "nobody stated a budget", because a child must fall back to its OWN
// deployment default and label it as such.
func TestRunBindingKeepsAnUnstatedBudgetUnstated(t *testing.T) {
	bindings := runToolCalls(t, &space.CoreRequest{
		Input: json.RawMessage(`"go"`),
		RunID: "run-budget-2",
	}, 1)
	binding := bindings[0]
	if binding.MaxSteps != 0 {
		t.Fatalf("binding.MaxSteps = %d for a run whose caller stated nothing; the loop's own "+
			"fallback ceiling is not the caller's budget, and forwarding it would tell the second "+
			"deployment a number nobody asked for", binding.MaxSteps)
	}
	if binding.MaxStepsStated {
		t.Fatal("binding.MaxStepsStated = true for a run whose caller stated nothing - this is the "+
			"laundering: the process default reported as the caller's will", binding.MaxSteps)
	}
}

// TestRunBindingBudgetDoesNotChangeWhileTheRunRuns is leg (c) of the requested contract:
// the budget is a property of the run, not of the step. A tool that reads it on call 1 and
// again on call 2 must see one number, or a delegation could ask for whichever bound was
// in favour mid-flight.
func TestRunBindingBudgetDoesNotChangeWhileTheRunRuns(t *testing.T) {
	bindings := runToolCalls(t, &space.CoreRequest{
		Input:    json.RawMessage(`"go"`),
		RunID:    "run-budget-3",
		MaxSteps: 5,
	}, 2)
	first, last := bindings[0], bindings[len(bindings)-1]
	if first != last {
		t.Fatalf("the binding changed mid-run: first %+v, last %+v", first, last)
	}
	if last.MaxSteps != 5 || !last.MaxStepsStated {
		t.Fatalf("binding = %+v, want MaxSteps 5 stated on every call", last)
	}
}
