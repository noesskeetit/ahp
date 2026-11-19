package model

// Whole-tree cost legs on real PostgreSQL. The read model is the thing the
// acceptance compares at both ends of a delegation and the thing the UI wants to
// display, so every leg asserts numbers, not the presence of a struct.
//
// Isolation is by tenant: each leg uses its own tenant name, and every query in
// the read model is tenant-scoped, so legs neither interfere with each other nor
// disturb other suites sharing the test database.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func costPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	requireModelDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	budgets := NewBudgetStore(pool)
	if err := budgets.Migrate(ctx); err != nil {
		t.Fatalf("budgets migrate: %v", err)
	}
	gens := NewGenerationStore(pool)
	if err := gens.Migrate(ctx); err != nil {
		t.Fatalf("generations migrate: %v", err)
	}
	return pool
}

// costTenant gives each leg a private accounting namespace.
func costTenant(name string) string {
	return fmt.Sprintf("h21cost%s%06d", name, time.Now().UnixNano()%1000000)
}

func costScope(tenant string) contracts.Scope {
	return contracts.Scope{Tenant: tenant, Subject: "s-cost", Deployment: "d-cost"}
}

// chargeGenerationRow writes one generation charged to a budget, with the usage a
// provider reported for it. Written directly: this is the observation side of the
// ledger, and the read model must join it however it got there.
func chargeGenerationRow(t *testing.T, pool *pgxpool.Pool, scope contracts.Scope, task, key, budgetID string, in, out int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := json.Marshal(ChatRequest{Model: "model-under-test", Messages: []Message{{Role: "user", Content: "x"}}})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO ahp_model.generations
  (id, tenant, subject, deployment, task, key, model, status, request, effect_key, budget_id,
   usage_prompt, usage_completion, usage_total, digest)
VALUES ($1,$2,$3,$4,$5,$6,'model-under-test','COMPLETED',$7,$8,$9,$10,$11,$12,'digest')
ON CONFLICT (tenant, subject, deployment, key) DO NOTHING`,
		"h21g-"+key, scope.Tenant, scope.Subject, scope.Deployment, task, key, req,
		"effect-"+key, budgetID, in, out, in+out); err != nil {
		t.Fatalf("insert generation: %v", err)
	}
}

func reserveAndSettle(t *testing.T, store *BudgetStore, scope contracts.Scope, budgetID, key string, amount, actual int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := store.Reserve(ctx, scope, budgetID, key, amount); err != nil {
		t.Fatalf("reserve %s: %v", key, err)
	}
	if _, err := store.Settle(ctx, scope, budgetID, key, actual); err != nil {
		t.Fatalf("settle %s: %v", key, err)
	}
}

// TestTreeCostSumsParentAndDelegatedChild is the core number: a task root, a child
// carved out of it, charges on both, and one answer that adds up -- in one unit.
func TestTreeCostSumsParentAndDelegatedChild(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("par"))
	root, err := budgets.Root(ctx, scope, "a2a-task:task-parent", budgetKeyTaskTokenRoot, 20000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, "gen:1", 700, 661)
	reserveAndSettle(t, budgets, scope, root.ID, "gen:2", 800, 766)
	chargeGenerationRow(t, pool, scope, "a2a-task:task-parent", "h21k-1-"+scope.Tenant, root.ID, 400, 261)
	chargeGenerationRow(t, pool, scope, "a2a-task:task-parent", "h21k-2-"+scope.Tenant, root.ID, 500, 266)

	childScope := scope
	childScope.Deployment = "d-child"
	child, err := budgets.Allocate(ctx, scope, root.ID, childScope, "a2a-task:task-child", "delegated:run", 5000)
	if err != nil {
		t.Fatalf("allocate child: %v", err)
	}
	reserveAndSettle(t, budgets, childScope, child.ID, "gen:3", 100, 100)
	chargeGenerationRow(t, pool, childScope, "a2a-task:task-child", "h21k-3-"+scope.Tenant, child.ID, 60, 40)

	// Asked with the PARENT's task id.
	trees, err := budgets.TreeCostByTask(ctx, scope, "task-parent")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one token tree", len(trees))
	}
	tr := trees[0]
	if tr.Unit != CostUnitTokens {
		t.Fatalf("unit = %q, want %q", tr.Unit, CostUnitTokens)
	}
	if len(tr.Nodes) != 2 {
		t.Fatalf("nodes = %d, want parent + child: %+v", len(tr.Nodes), tr.Nodes)
	}
	if tr.Spent != 1527 {
		t.Fatalf("tree spent = %d, want 1527 (661+766 on the parent, 100 on the child)", tr.Spent)
	}
	if tr.Generations != 3 || tr.TokensIn != 960 || tr.TokensOut != 567 || tr.TokensTotal != 1527 {
		t.Fatalf("tree observations = gens %d in %d out %d total %d, want 3/960/567/1527",
			tr.Generations, tr.TokensIn, tr.TokensOut, tr.TokensTotal)
	}
	if !tr.Reconciles {
		t.Fatalf("a consistent tree reported a mismatch: %v", tr.Mismatches)
	}
	// Parent and child numbers stay separate -- "what did this cost" is answered
	// per node as well as in total.
	var parentNode, childNode CostNode
	for _, n := range tr.Nodes {
		switch n.Depth {
		case 0:
			parentNode = n
		case 1:
			childNode = n
		}
	}
	if parentNode.Task != "a2a-task:task-parent" || parentNode.Spent != 1427 || parentNode.Generations != 2 {
		t.Fatalf("parent node = %+v, want a2a-task:task-parent spent 1427 over 2 generations", parentNode)
	}
	if childNode.Task != "a2a-task:task-child" || childNode.Spent != 100 || childNode.Generations != 1 || childNode.Limit != 5000 {
		t.Fatalf("child node = %+v, want a2a-task:task-child spent 100 over 1 generation with limit 5000", childNode)
	}
	// While the child is open, the root holds the WHOLE carve-out, not the child's
	// remaining part of it: an open child keeps its whole carve held on the parent
	// (that is what makes an over-delegation impossible), so the honest number is
	// 5000 and the child's own 100 is charged on the child.
	if tr.HeldForChildren != 5000 {
		t.Fatalf("held for children = %d, want the whole open carve of 5000", tr.HeldForChildren)
	}
	if tr.AvailableToRoot != 20000-1427-5000 || tr.Exhausted {
		t.Fatalf("available = %d exhausted=%v, want %d and not exhausted", tr.AvailableToRoot, tr.Exhausted, 20000-1427-5000)
	}

	// Closing the child returns its unused remainder (PDF :1789: "остаток после
	// завершения возвращается"), and the tree total must NOT change: the child's
	// 100 was already counted once, and propagating it into the parent's node must
	// not make the tree look like it spent 1627.
	if err := budgets.Close(ctx, childScope, child.ID); err != nil {
		t.Fatalf("close child: %v", err)
	}
	after, err := budgets.TreeCostByTask(ctx, scope, "task-parent")
	if err != nil {
		t.Fatalf("tree cost after close: %v", err)
	}
	if after[0].Spent != 1527 {
		t.Fatalf("tree spent after the child closed = %d, want 1527 unchanged (a propagated child charge must not be counted twice)", after[0].Spent)
	}
	if after[0].HeldForChildren != 0 {
		t.Fatalf("held for children after close = %d, want 0 (the remainder came back)", after[0].HeldForChildren)
	}
	if after[0].AvailableToRoot != 20000-1527 {
		t.Fatalf("available after close = %d, want %d", after[0].AvailableToRoot, 20000-1527)
	}
	if !after[0].Reconciles {
		t.Fatalf("a closed, consistent tree reported mismatches: %v", after[0].Mismatches)
	}

	// Asking with the CHILD's task id answers with the same tree, not a fragment.
	kids, err := budgets.TreeCostByTask(ctx, scope, "task-child")
	if err != nil {
		t.Fatalf("tree cost by child: %v", err)
	}
	if len(kids) != 1 || kids[0].Spent != 1527 || kids[0].RootID != tr.RootID {
		t.Fatalf("asking by child gave %+v, want the same whole tree", kids)
	}
}

// TestTreeCostReportsAnExhaustedStepTree covers the delegated-run denomination:
// the measured live shape (bound 3, three steps consumed, a child carve that was
// closed and returned its unused remainder).
func TestTreeCostReportsAnExhaustedStepTree(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("step"))
	root, err := budgets.Root(ctx, scope, "treedelegate-run:run-1", budgetKeyRunStepRoot, 3)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, StepKeyForTest(0), 1, 1)
	// A delegation carves the remainder, then comes back unused (the child never
	// reached it) -- the live measurement's shape.
	child, err := budgets.Allocate(ctx, scope, root.ID, scope, "treedelegate-child:run-1:child-1", "delegated:steps", 2)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if err := budgets.Close(ctx, scope, child.ID); err != nil {
		t.Fatalf("close child: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, StepKeyForTest(1), 1, 1)
	reserveAndSettle(t, budgets, scope, root.ID, StepKeyForTest(2), 1, 1)

	trees, err := budgets.TreeCostByTask(ctx, scope, "run-1")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if len(trees) != 1 {
		t.Fatalf("trees = %d, want one step tree", len(trees))
	}
	tr := trees[0]
	if tr.Unit != CostUnitSteps {
		t.Fatalf("unit = %q, want %q", tr.Unit, CostUnitSteps)
	}
	if tr.Spent != 3 {
		t.Fatalf("spent = %d, want 3 steps", tr.Spent)
	}
	if !tr.Exhausted || tr.AvailableToRoot != 0 {
		t.Fatalf("a run that consumed its whole bound reported available %d exhausted=%v", tr.AvailableToRoot, tr.Exhausted)
	}
	var closed bool
	for _, n := range tr.Nodes {
		if n.Depth == 1 {
			closed = n.Closed
			if n.Spent != 0 {
				t.Fatalf("delegated child spent %d, want 0 (it never ran)", n.Spent)
			}
		}
	}
	if !closed {
		t.Fatal("the returned child grant must read as closed")
	}
}

// TestTreeCostDoesNotAnswerForAnotherTenant: the read is scoped, and the answer
// for a foreign task is the same as for a missing one.
func TestTreeCostDoesNotAnswerForAnotherTenant(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	owner := costScope(costTenant("own"))
	if _, err := budgets.Root(ctx, owner, "a2a-task:secret-task", budgetKeyTaskTokenRoot, 1000); err != nil {
		t.Fatalf("root: %v", err)
	}
	other := costScope(costTenant("oth"))
	_, err := budgets.TreeCostByTask(ctx, other, "secret-task")
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant read = %v, want not-found (no existence signal)", err)
	}
}

// TestTreeCostReportsAMismatchInsteadOfPassing gives the reconciliation teeth:
// charge and reported usage that disagree must be named, not averaged away.
func TestTreeCostReportsAMismatchInsteadOfPassing(t *testing.T) {
	pool := costPool(t)
	budgets := NewBudgetStore(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	scope := costScope(costTenant("mism"))
	root, err := budgets.Root(ctx, scope, "a2a-task:task-mismatch", budgetKeyTaskTokenRoot, 20000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, "gen:1", 500, 500)
	chargeGenerationRow(t, pool, scope, "a2a-task:task-mismatch", "h21k-9-"+scope.Tenant, root.ID, 30, 20)

	trees, err := budgets.TreeCostByTask(ctx, scope, "task-mismatch")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	tr := trees[0]
	if tr.Reconciles {
		t.Fatal("charged 500 against a generation that reported 50 and the read model called that consistent")
	}
	if len(tr.Mismatches) == 0 {
		t.Fatal("a mismatch was reported without saying what mismatched")
	}
}

// StepKeyForTest mirrors the run step reservation key naming.
func StepKeyForTest(step int) string { return fmt.Sprintf("core:step:%d", step) }
