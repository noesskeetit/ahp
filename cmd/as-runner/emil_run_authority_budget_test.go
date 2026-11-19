package main

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// toolContext rebuilds the context internal/core hands to a tool in the middle of
// a run: the run's scope plus task token (internal/core/artifact_tool.go
// WithRunScope) and the run's binding (internal/core/run_binding.go
// WithRunBinding). Whatever emilRunAuthority reads from this is what a delegating
// tool would carry to a SECOND DEPLOYMENT on a live run.
func toolContext(b core.RunBinding) context.Context {
	ctx := core.WithRunScope(context.Background(),
		contracts.Scope{Tenant: "a2a", Deployment: "a2a-parent", Subject: "alice"},
		"run-own-task-token")
	return core.WithRunBinding(ctx, b)
}

// TestEmilRunAuthorityForwardsTheStatedBudget is the host half of the budget
// contract (vm2-state-runner a44dd6d): a caller that named a bound must have that
// exact number forwarded to the child, so the child can journal
// step_budget_source=caller for a bound it actually received.
func TestEmilRunAuthorityForwardsTheStatedBudget(t *testing.T) {
	ra := emilRunAuthority(toolContext(core.RunBinding{
		ContextID: "thread-1", TaskID: "run-task-1", MaxSteps: 4, MaxStepsStated: true,
	}))
	if ra == nil {
		t.Fatal("a run with a task token must get an authority, got nil")
	}
	if ra.MaxSteps != 4 {
		t.Fatalf("the stated bound of 4 must be forwarded, got %d", ra.MaxSteps)
	}
	if ra.ContextID != "thread-1" || ra.TaskID != "run-task-1" {
		t.Fatalf("context and task must cross the hop, got %+v", ra)
	}
}

// TestEmilRunAuthorityKeepsAnUnstatedBudgetUnstated keeps the child's
// step_budget_source=deployment reachable: forwarding an invented number would
// attribute the platform's own default to the caller.
func TestEmilRunAuthorityKeepsAnUnstatedBudgetUnstated(t *testing.T) {
	ra := emilRunAuthority(toolContext(core.RunBinding{
		ContextID: "thread-1", TaskID: "run-task-1",
	}))
	if ra == nil {
		t.Fatal("expected an authority for a run with a task token")
	}
	if ra.MaxSteps != 0 {
		t.Fatalf("an unstated budget must stay 0, got %d", ra.MaxSteps)
	}
}

// TestEmilRunAuthorityDoesNotLaunderTheLoopFallback is the specific lie this
// field exists to prevent: the loop bounds itself with 64 when nobody asked, and
// that is a fact about the loop, not a caller's will. Statedness, not the number,
// decides what crosses the hop, so a future runtime that stamps a default into
// MaxSteps still cannot make a child report 64 as the user's request.
func TestEmilRunAuthorityDoesNotLaunderTheLoopFallback(t *testing.T) {
	ra := emilRunAuthority(toolContext(core.RunBinding{
		ContextID: "thread-1", TaskID: "run-task-1", MaxSteps: 64, MaxStepsStated: false,
	}))
	if ra == nil {
		t.Fatal("expected an authority for a run with a task token")
	}
	if ra.MaxSteps != 0 {
		t.Fatalf("an unstated 64 must not be forwarded as a caller bound, got %d", ra.MaxSteps)
	}
}

// TestEmilRunAuthorityDeclinesWithoutARun keeps the no-run case honest: with no
// run credential the adapter keeps its configured bearer instead of inventing a
// subject or a budget.
func TestEmilRunAuthorityDeclinesWithoutARun(t *testing.T) {
	if ra := emilRunAuthority(context.Background()); ra != nil {
		t.Fatalf("no run in context must yield no authority, got %+v", ra)
	}
}
