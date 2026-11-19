package execbridge

import (
	"context"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/state"
	"strings"
	"testing"
	"time"
)

func TestCompletedTaskReturnsOnlySettledReserveAndDeletionDoesNotRefund(t *testing.T) {
	ctx := context.Background()
	pool := dbPool(t)
	states := state.New(pool)
	if err := states.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, pool, Config{Inference: &fakeInference{}, TaskTokenLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	b.WithBudgetCompletion(func(ctx context.Context, task model.BudgetTask) (bool, error) {
		return states.TaskTerminal(ctx, task.Scope, strings.TrimPrefix(task.Task, "a2a-task:"))
	})
	scope := uniqueScope()
	rootRef := model.BudgetTask{Scope: scope, Task: "a2a-task:root"}
	childRef := model.BudgetTask{Scope: scope, Task: "a2a-task:child"}
	root, err := b.budgets.EnsureTaskTree(ctx, []model.BudgetTask{rootRef}, RootBudgetKey, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	child, err := b.budgets.EnsureTaskTree(ctx, []model.BudgetTask{rootRef, childRef}, RootBudgetKey, 1000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.budgets.Reserve(ctx, scope, child.ID, "inflight", 300); err != nil {
		t.Fatal(err)
	}
	thread, err := states.CreateThread(ctx, scope, "ctx", "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := states.CreateRun(ctx, scope, thread.ID, "child", "run", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := states.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = states.Release(ctx, scope, run.ID, "worker", lease.Epoch, state.RunDone); err != nil {
		t.Fatal(err)
	}
	if err = b.ReconcileBudgets(ctx); err != nil {
		t.Fatal(err)
	}
	held, err := b.budgets.Get(ctx, scope, root.ID)
	if err != nil || held.Reserved != 1000 {
		t.Fatalf("terminal with open effect refunded: %+v err=%v", held, err)
	}
	if _, err = b.budgets.Settle(ctx, scope, child.ID, "inflight", 200); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = b.ReconcileBudgets(ctx); err != nil {
			t.Fatal(err)
		}
	}
	got, err := b.budgets.Get(ctx, scope, root.ID)
	if err != nil || got.Spent != 200 || got.Reserved != 0 {
		t.Fatalf("completion did not return exactly 800: %+v err=%v", got, err)
	}
	deletedRef := model.BudgetTask{Scope: scope, Task: "a2a-task:deleted"}
	if _, err = b.budgets.EnsureTaskTree(ctx, []model.BudgetTask{rootRef, deletedRef}, RootBudgetKey, 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err = states.CreateRun(ctx, scope, thread.ID, "deleted", "deleted", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err = states.DeleteThread(ctx, scope, thread.ID); err != nil {
		t.Fatal(err)
	}
	if err = b.ReconcileBudgets(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = b.budgets.Get(ctx, scope, root.ID)
	if err != nil || got.Reserved != 800 || got.Spent != 200 {
		t.Fatalf("deleted task refunded without terminal proof: %+v err=%v", got, err)
	}
}
