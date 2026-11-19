package model

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func newStore(t *testing.T) *BudgetStore {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	s := NewBudgetStore(pool)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func scopeWith(deployment string) contracts.Scope {
	return contracts.Scope{Tenant: "t1", Subject: "s1", Deployment: deployment}
}

func TestBudgetLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	scope := scopeWith("dep-a")
	u := fmt.Sprint(time.Now().UnixNano())
	rootKey, childKey := "root-"+u, "child-"+u

	if _, err := s.Get(ctx, scope, "missing-"+u); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("get missing: %v", err)
	}
	if _, err := s.Root(ctx, scope, "task1", rootKey, 0); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("root zero limit: %v", err)
	}
	if _, err := s.Root(ctx, scope, "task1", rootKey, maxAmount+1); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("root huge limit: %v", err)
	}
	root, err := s.Root(ctx, scope, "task1", rootKey, 1000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	if root.Parent != "" || root.Limit != 1000 {
		t.Fatalf("root = %+v", root)
	}
	replay, err := s.Root(ctx, scope, "task1", rootKey, 1000)
	if err != nil || replay.ID != root.ID {
		t.Fatalf("root replay: %v %+v", err, replay)
	}
	if _, err := s.Root(ctx, scope, "task1", rootKey, 500); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("root replay mismatch: %v", err)
	}

	childScope := scopeWith("dep-b")
	foreign := contracts.Scope{Tenant: "t1", Subject: "other", Deployment: "dep-b"}
	if _, err := s.Allocate(ctx, scope, root.ID, foreign, "task2", childKey, 100); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("cross-subject allocate: %v", err)
	}
	if _, err := s.Allocate(ctx, scope, root.ID, childScope, "task2", childKey, 1001); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("over-capacity allocate: %v", err)
	}
	child, err := s.Allocate(ctx, scope, root.ID, childScope, "task2", childKey, 400)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}
	if child.Parent != root.ID || child.Limit != 400 {
		t.Fatalf("child = %+v", child)
	}
	if replay, err := s.Allocate(ctx, scope, root.ID, childScope, "task2", childKey, 400); err != nil || replay.ID != child.ID {
		t.Fatalf("allocate replay: %v %+v", err, replay)
	}
	if _, err := s.Allocate(ctx, scope, root.ID, childScope, "task2", childKey, 300); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("allocate replay mismatch: %v", err)
	}
	got, err := s.Get(ctx, scope, root.ID)
	if err != nil || got.Reserved != 400 {
		t.Fatalf("root after allocate: %+v err=%v", got, err)
	}

	if _, err := s.Reserve(ctx, childScope, child.ID, "r1", 250); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := s.Reserve(ctx, childScope, child.ID, "r1", 250); err != nil {
		t.Fatalf("reserve replay: %v", err)
	}
	if _, err := s.Reserve(ctx, childScope, child.ID, "r1", 100); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("reserve replay mismatch: %v", err)
	}
	if _, err := s.Reserve(ctx, childScope, child.ID, "r2", 200); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("overdraft reserve: %v", err)
	}
	if _, err := s.Reserve(ctx, scopeWith("dep-c"), child.ID, "rx", 1); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment reserve: %v", err)
	}
	if err := s.Close(ctx, childScope, child.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("close with open reservation: %v", err)
	}

	if _, err := s.Settle(ctx, childScope, child.ID, "r1", -1); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("settle negative usage: %v", err)
	}
	res, err := s.Settle(ctx, childScope, child.ID, "r1", 150)
	if err != nil || !res.Settled || res.Actual != 150 {
		t.Fatalf("settle: %+v err=%v", res, err)
	}
	if _, err := s.Settle(ctx, childScope, child.ID, "r1", 150); err != nil {
		t.Fatalf("settle replay: %v", err)
	}
	if _, err := s.Settle(ctx, childScope, child.ID, "r1", 100); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("settle mismatch: %v", err)
	}
	if _, err := s.Reserve(ctx, childScope, child.ID, "r3", 100); err != nil {
		t.Fatalf("reserve after settle: %v", err)
	}
	if _, err := s.Settle(ctx, childScope, child.ID, "r3", 0); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got, err = s.Get(ctx, childScope, child.ID)
	if err != nil || got.Spent != 150 || got.Reserved != 0 {
		t.Fatalf("child after settle: %+v err=%v", got, err)
	}

	if err := s.Close(ctx, childScope, child.ID); err != nil {
		t.Fatalf("close child: %v", err)
	}
	if err := s.Close(ctx, childScope, child.ID); err != nil {
		t.Fatalf("close child idempotent: %v", err)
	}
	if _, err := s.Reserve(ctx, childScope, child.ID, "r4", 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("reserve on closed: %v", err)
	}
	got, err = s.Get(ctx, scope, root.ID)
	if err != nil || got.Reserved != 0 || got.Spent != 150 {
		t.Fatalf("root after child close: %+v err=%v", got, err)
	}
	if err := s.Close(ctx, scope, root.ID); err != nil {
		t.Fatalf("close root: %v", err)
	}
	if _, err := s.Allocate(ctx, scope, root.ID, childScope, "task9", "k9-"+u, 10); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("allocate from closed: %v", err)
	}
}

func TestSameKeyAcrossTenantsIndependent(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(time.Now().UnixNano())
	key := "shared-" + u
	task := "task-shared"
	scA := contracts.Scope{Tenant: "tA-" + u, Subject: "s1", Deployment: "dep-a"}
	scB := contracts.Scope{Tenant: "tB-" + u, Subject: "s1", Deployment: "dep-a"}

	a, err := s.Root(ctx, scA, task, key, 100)
	if err != nil {
		t.Fatalf("root tenant A: %v", err)
	}
	b, err := s.Root(ctx, scB, task, key, 100)
	if err != nil {
		t.Fatalf("root tenant B: %v", err)
	}
	if a.ID == b.ID {
		t.Fatalf("tenants share budget id %q", a.ID)
	}
	if _, err := s.Get(ctx, scA, b.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant get: %v", err)
	}
	// Replay stays scoped: same key, same tenant returns the original.
	if replay, err := s.Root(ctx, scA, task, key, 100); err != nil || replay.ID != a.ID {
		t.Fatalf("scoped replay: %v %+v", err, replay)
	}
}

func TestAllocateSameKeyDifferentParents(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(time.Now().UnixNano())
	scope := scopeWith("dep-parents")
	r1, err := s.Root(ctx, scope, "task-p1", "r1-"+u, 1000)
	if err != nil {
		t.Fatalf("root 1: %v", err)
	}
	r2, err := s.Root(ctx, scope, "task-p2", "r2-"+u, 1000)
	if err != nil {
		t.Fatalf("root 2: %v", err)
	}
	key := "shared-alloc-" + u
	c1, err := s.Allocate(ctx, scope, r1.ID, scope, "task-c", key, 100)
	if err != nil {
		t.Fatalf("allocate under parent 1: %v", err)
	}
	c2, err := s.Allocate(ctx, scope, r2.ID, scope, "task-c", key, 100)
	if err != nil {
		t.Fatalf("same key under parent 2 must succeed: %v", err)
	}
	if c1.ID == c2.ID || c1.Parent != r1.ID || c2.Parent != r2.ID {
		t.Fatalf("children not parent-scoped: %+v %+v", c1, c2)
	}
}

func TestConcurrentReserveNoOverdraft(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(time.Now().UnixNano())
	scope := scopeWith("dep-conc")
	root, err := s.Root(ctx, scope, "task-conc", "root-conc-"+u, 100)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	const n = 10
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Reserve(ctx, scope, root.ID, fmt.Sprintf("conc-%s-%d", u, i), 20)
			mu.Lock()
			if err == nil {
				ok++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if ok != 5 {
		t.Fatalf("granted %d, want 5", ok)
	}
	got, err := s.Get(ctx, scope, root.ID)
	if err != nil || got.Reserved != 100 {
		t.Fatalf("root = %+v err=%v", got, err)
	}
}
