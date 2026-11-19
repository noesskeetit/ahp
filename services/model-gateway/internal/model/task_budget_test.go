package model

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"sync"
	"testing"
)

func TestDefaultTaskBudgetPolicyAllowsConcurrentSiblings(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	root := BudgetTask{Scope: e.scope, Task: "root"}
	var wg sync.WaitGroup
	for _, name := range []string{"child-a", "child-b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			child, err := e.budget.EnsureTaskTree(ctx, []BudgetTask{root, {Scope: e.scope, Task: name}}, "fanout", 100)
			if err != nil || child.Limit != 50 {
				t.Errorf("sibling %s: limit=%d err=%v", name, child.Limit, err)
			}
		}(name)
	}
	wg.Wait()
	got, err := e.budget.TaskBudget(ctx, e.scope, "root", "fanout")
	if err != nil || got.Reserved != 100 {
		t.Fatalf("sibling tree=%+v err=%v", got, err)
	}
}

func TestTaskBudgetInheritsPersistentParentCapacity(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	rootRef := BudgetTask{Scope: e.scope, Task: "a2a-task:root"}
	childScope := e.scope
	childScope.Deployment = "child"
	childRef := BudgetTask{Scope: childScope, Task: "a2a-task:child"}
	root, err := e.budget.EnsureTaskTree(ctx, []BudgetTask{rootRef}, "execbridge-task-root", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.budget.Reserve(ctx, e.scope, root.ID, "own", 30); err != nil {
		t.Fatal(err)
	}
	if _, err = e.budget.Settle(ctx, e.scope, root.ID, "own", 30); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := NewBudgetStore(e.pool).EnsureTaskTree(ctx, []BudgetTask{rootRef, childRef}, "execbridge-task-root", 100, 100)
			if err != nil || b.Parent != root.ID || b.Limit != 70 {
				t.Errorf("child=%+v err=%v", b, err)
			}
		}()
	}
	wg.Wait()
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Reserved != 70 {
		t.Fatalf("parent=%+v err=%v", got, err)
	}
	if _, err = e.budget.EnsureTaskTree(ctx, []BudgetTask{childRef}, "execbridge-task-root", 100, 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("child rebound as root: %v", err)
	}
	if _, err = e.budget.Reserve(ctx, childScope, mustTaskBudget(t, e.budget, ctx, []BudgetTask{rootRef, childRef}).ID, "spend", 71); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("child exceeded inherited budget: %v", err)
	}
}

func mustTaskBudget(t *testing.T, s *BudgetStore, ctx context.Context, chain []BudgetTask) Budget {
	t.Helper()
	b, err := s.EnsureTaskTree(ctx, chain, "execbridge-task-root", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTaskBudgetConcurrentRootsReplayOneBudget(t *testing.T) {
	ctx := context.Background()
	e := newModelEnv(t)
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := e.budget.Root(ctx, e.scope, "root", "root", 100)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- b.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if first != id {
			t.Fatal("duplicate root")
		}
	}
}
