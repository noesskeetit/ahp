package model

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"sync"
	"testing"
)

func TestBudgetAliasCannotRebindOrCrossOwner(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "a2a-task:root", "execbridge-task-root", 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = e.budget.BindTaskAlias(ctx, e.scope, "a2a-task:alias", "execbridge-task-root", root.ID); err != nil {
			t.Fatal(err)
		}
	}
	other, err := e.budget.Root(ctx, e.scope, "a2a-task:other", "execbridge-task-root", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.budget.BindTaskAlias(ctx, e.scope, "a2a-task:alias", "execbridge-task-root", other.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("alias rebound: %v", err)
	}
	foreign := e.scope
	foreign.Subject = "another"
	if err = e.budget.BindTaskAlias(ctx, foreign, "a2a-task:foreign", "execbridge-task-root", root.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign alias accepted: %v", err)
	}
	var n int
	if err = e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.budgets WHERE tenant=$1`, e.scope.Tenant).Scan(&n); err != nil || n != 2 {
		t.Fatalf("alias created capacity: nodes=%d err=%v", n, err)
	}
}

func TestBudgetOwnerCannotChangeAfterAdmission(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	chain := []BudgetTask{{Scope: e.scope, Task: "root", Owner: BudgetOwnerState}}
	if _, err := e.budget.EnsureTaskTree(ctx, chain, "owner", 100); err != nil {
		t.Fatal(err)
	}
	chain[0].Owner = BudgetOwnerGateway
	if _, err := e.budget.EnsureTaskTree(ctx, chain, "owner", 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("lifecycle owner rebound: %v", err)
	}
}

func TestAliasPreventsNewRootAndChildBudgetInItsNamespace(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.budget.BindTaskAlias(ctx, e.scope, "alias", "token", root.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = e.budget.Root(ctx, e.scope, "alias", "token", 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("alias got independent root: %v", err)
	}
	if _, err = e.budget.EnsureTaskTree(ctx, []BudgetTask{{Scope: e.scope, Task: "alias"}}, "token", 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("alias got independent task root: %v", err)
	}
	if _, err = e.budget.EnsureTaskTree(ctx, []BudgetTask{{Scope: e.scope, Task: "different"}, {Scope: e.scope, Task: "alias"}}, "token", 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("alias got independent child: %v", err)
	}
}

func TestConcurrentAliasAndChildHaveOnlyOneBinding(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	ref := BudgetTask{Scope: e.scope, Task: "root"}
	root, err := e.budget.EnsureTaskTree(ctx, []BudgetTask{ref}, "token", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			results <- e.budget.BindTaskAlias(ctx, e.scope, task, "token", root.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, err := e.budget.EnsureTaskTree(ctx, []BudgetTask{ref, {Scope: e.scope, Task: task}}, "token", 100, 10)
			results <- err
		}()
		close(start)
		wg.Wait()
		close(results)
		success := 0
		for err := range results {
			if err == nil {
				success++
			} else if contracts.Code(err) != contracts.ErrConflict {
				t.Fatalf("namespace race: %v", err)
			}
		}
		if success != 1 {
			t.Fatalf("task %s has %d accepted bindings, want1", task, success)
		}
	}
}
