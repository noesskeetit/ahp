package model

// Legs for the server-side label source that feeds routing. Real PostgreSQL, no
// mocks: the thing under test is which record the gateway consults, and a fake
// would answer that question for the wrong reason.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/labels"
)

func labelStore(t *testing.T) *DeploymentLabelStore {
	t.Helper()
	pool := costPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := NewDeploymentLabelStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return store
}

func TestDeploymentLabelsRoundTripAndAbsentMeansEmpty(t *testing.T) {
	store := labelStore(t)
	ctx := context.Background()
	tenant := costTenant("lbl")
	deployment := "d-classified"

	// No record yet: empty, and no error. A gateway must keep serving (with the
	// empty set still gated) when the operator has classified nobody.
	got, err := store.LabelsFor(ctx, tenant, deployment)
	if err != nil {
		t.Fatalf("absent record must not fail: %v", err)
	}
	if len(got.Taints) != 0 || len(got.Subjects) != 0 || len(got.Workspaces) != 0 {
		t.Fatalf("absent record = %+v, want the empty set", got)
	}

	want := labels.Set{Subjects: []string{"s-cost"}, Taints: []string{labels.TaintSecret}}
	if err := store.Set(ctx, tenant, deployment, want, "test"); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err = store.LabelsFor(ctx, tenant, deployment)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got.Taints) != 1 || got.Taints[0] != labels.TaintSecret {
		t.Fatalf("taints = %v, want [%s]", got.Taints, labels.TaintSecret)
	}
	if len(got.Subjects) != 1 || got.Subjects[0] != "s-cost" {
		t.Fatalf("subjects = %v, want [s-cost]", got.Subjects)
	}

	// A second write replaces: the record states the CURRENT classification, so a
	// corrected classification must not leave the old taint behind.
	if err := store.Set(ctx, tenant, deployment, labels.Set{}, "test-correction"); err != nil {
		t.Fatalf("replace: %v", err)
	}
	got, err = store.LabelsFor(ctx, tenant, deployment)
	if err != nil {
		t.Fatalf("read after replace: %v", err)
	}
	if len(got.Taints) != 0 {
		t.Fatalf("a replaced record still carries %v -- a corrected classification would keep routing as if the correction never happened", got.Taints)
	}
}

func TestDeploymentLabelsAreScopedPerTenant(t *testing.T) {
	store := labelStore(t)
	ctx := context.Background()
	a, b := costTenant("lblA"), costTenant("lblB")
	deployment := "d-same-name"

	if err := store.Set(ctx, a, deployment, labels.Set{Taints: []string{labels.TaintSecret}}, "test"); err != nil {
		t.Fatalf("set a: %v", err)
	}
	other, err := store.LabelsFor(ctx, b, deployment)
	if err != nil {
		t.Fatalf("read b: %v", err)
	}
	if len(other.Taints) != 0 {
		t.Fatalf("tenant %s inherited tenant %s's classification: %v", b, a, other.Taints)
	}
	mine, err := store.LabelsFor(ctx, a, deployment)
	if err != nil {
		t.Fatalf("read a: %v", err)
	}
	if len(mine.Taints) != 1 {
		t.Fatalf("tenant %s lost its own classification: %v", a, mine.Taints)
	}
}

func TestDeploymentLabelsMigrateIsAdditive(t *testing.T) {
	// The accepted accounting must be untouched by this table's existence: an
	// operator who classifies deployments still gets the same budget read.
	pool := costPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store := NewDeploymentLabelStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	budgets := NewBudgetStore(pool)
	if err := budgets.Migrate(ctx); err != nil {
		t.Fatalf("budgets migrate: %v", err)
	}
	scope := costScope(costTenant("lblmix"))
	root, err := budgets.Root(ctx, scope, "a2a-task:label-mix", budgetKeyTaskTokenRoot, 500)
	if err != nil {
		t.Fatalf("root after the labels table exists: %v", err)
	}
	reserveAndSettle(t, budgets, scope, root.ID, "gen:1", 200, 150)
	trees, err := budgets.TreeCostByTask(ctx, scope, "label-mix")
	if err != nil {
		t.Fatalf("tree cost: %v", err)
	}
	if trees[0].Spent != 150 {
		t.Fatalf("tree spent = %d, want 150 -- the labels table must not move money", trees[0].Spent)
	}
	if _, err := store.RecordCount(ctx); err != nil {
		t.Fatalf("count: %v", err)
	}
}
