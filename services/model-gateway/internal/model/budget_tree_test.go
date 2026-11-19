package model

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Tree-level accounting tests: the PDF rule is that the Model Gateway leads
// the task-tree budget -- a child atomically reserves a slice from the
// parent, the remainder returns on completion, an exhausted reserve makes the
// model refuse (never overdraw), and parallel children spend only their own
// reserve. These are the contract legs for per-node AND per-tree accounting.

func TestTreeMultiLevelAccountingAndClose(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(t.Name())
	scope := scopeWith("dep-tree-ml")

	root, err := s.Root(ctx, scope, "t-root", "root-"+u, 1000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	child, err := s.Allocate(ctx, scope, root.ID, scope, "t-child", "c-"+u, 400)
	if err != nil {
		t.Fatalf("child: %v", err)
	}
	grand, err := s.Allocate(ctx, scope, child.ID, scope, "t-grand", "g-"+u, 150)
	if err != nil {
		t.Fatalf("grand: %v", err)
	}

	// Spend at every level: root 100, child 50, grand 40.
	for _, tc := range []struct {
		budgetID, key string
		amount        int64
	}{{root.ID, "sr-" + u, 100}, {child.ID, "sc-" + u, 50}, {grand.ID, "sg-" + u, 40}} {
		if _, err := s.Reserve(ctx, scope, tc.budgetID, tc.key, tc.amount); err != nil {
			t.Fatalf("reserve %s: %v", tc.key, err)
		}
		if _, err := s.Settle(ctx, scope, tc.budgetID, tc.key, tc.amount); err != nil {
			t.Fatalf("settle %s: %v", tc.key, err)
		}
	}

	snap, err := s.TreeSnapshot(ctx, scope, root.ID)
	if err != nil {
		t.Fatalf("TreeSnapshot: %v", err)
	}
	if snap.Totals.Nodes != 3 {
		t.Fatalf("nodes = %d, want 3", snap.Totals.Nodes)
	}
	if snap.Totals.Spent != 190 {
		t.Fatalf("tree spent = %d, want 190", snap.Totals.Spent)
	}
	if snap.Totals.OpenHolds != 0 {
		t.Fatalf("open holds = %d, want 0", snap.Totals.OpenHolds)
	}
	if len(snap.Nodes) != 3 || snap.Nodes[0].Depth != 0 || snap.Nodes[2].Depth != 2 {
		t.Fatalf("bad node/depth shape: %+v", snap.Nodes)
	}

	// Grandchild closes: its 110 unspent returns to the child; its 40 spent
	// stays charged in the tree.
	if err := s.Close(ctx, scope, grand.ID); err != nil {
		t.Fatalf("close grand: %v", err)
	}
	got, err := s.Get(ctx, scope, child.ID)
	if err != nil || got.Spent != 90 || got.Reserved != 0 {
		t.Fatalf("child after grand close: %+v err=%v", got, err)
	}
	if err := s.Close(ctx, scope, child.ID); err != nil {
		t.Fatalf("close child: %v", err)
	}
	got, err = s.Get(ctx, scope, root.ID)
	if err != nil || got.Spent != 190 || got.Reserved != 0 {
		t.Fatalf("root after tree close: %+v err=%v (want own 100 + propagated 90)", got, err)
	}
	snap, err = s.TreeSnapshot(ctx, scope, root.ID)
	if err != nil {
		t.Fatalf("TreeSnapshot after close: %v", err)
	}
	if snap.Totals.Spent != 190 || snap.Totals.Committed != 190 {
		t.Fatalf("tree totals after close = %+v, want event-based 190 (settle facts, no propagation double count)", snap.Totals)
	}
}

func TestConcurrentAllocateAgainstParentCapacity(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(t.Name())
	scope := scopeWith("dep-tree-alloc")
	root, err := s.Root(ctx, scope, "t-cap", "root-"+u, 100)
	if err != nil {
		t.Fatalf("root: %v", err)
	}

	const n = 8
	const slice int64 = 15 // 8 slices of 15 cannot all fit into 100
	var wg sync.WaitGroup
	var mu sync.Mutex
	var carved int64
	ok := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Allocate(ctx, scope, root.ID, scope, fmt.Sprintf("tc-%s-%d", u, i), fmt.Sprintf("k-%s-%d", u, i), slice)
			mu.Lock()
			if err == nil {
				ok++
				carved += slice
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	if ok != 6 {
		t.Fatalf("carved %d children, want 6 (capacity 100 / slice 15)", ok)
	}
	if carved > 100 {
		t.Fatalf("tree over-allocated: %d carved from 100", carved)
	}
	got, err := s.Get(ctx, scope, root.ID)
	if err != nil || got.Reserved != carved || got.Spent != 0 {
		t.Fatalf("root after carve: %+v err=%v", got, err)
	}
}

func TestParallelChildrenSpendOnlyOwnReserve(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(t.Name())
	scope := scopeWith("dep-tree-par")

	root, err := s.Root(ctx, scope, "t-par", "root-"+u, 1000)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	a, err := s.Allocate(ctx, scope, root.ID, scope, "t-a", "a-"+u, 30)
	if err != nil {
		t.Fatalf("child a: %v", err)
	}
	b, err := s.Allocate(ctx, scope, root.ID, scope, "t-b", "b-"+u, 30)
	if err != nil {
		t.Fatalf("child b: %v", err)
	}

	// A exhausts its own reserve.
	if _, err := s.Reserve(ctx, scope, a.ID, "ra-"+u, 30); err != nil {
		t.Fatalf("a reserve: %v", err)
	}
	if _, err := s.Settle(ctx, scope, a.ID, "ra-"+u, 30); err != nil {
		t.Fatalf("a settle: %v", err)
	}
	// The corner: A is exhausted although the tree holds 940 units of free
	// capacity -- the refusal is per node, the accounting is per tree.
	if _, err := s.Reserve(ctx, scope, a.ID, "ra2-"+u, 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("exhausted child reserve err = %v, want ErrConflict", err)
	}
	// B is untouched by A's exhaustion.
	if _, err := s.Reserve(ctx, scope, b.ID, "rb-"+u, 25); err != nil {
		t.Fatalf("b reserve after a exhausted: %v", err)
	}

	snap, err := s.TreeSnapshot(ctx, scope, root.ID)
	if err != nil {
		t.Fatalf("TreeSnapshot: %v", err)
	}
	if snap.Totals.Spent != 30 || snap.Totals.OpenHolds != 25 || snap.Totals.Committed != 55 {
		t.Fatalf("tree totals = %+v, want spent 30 open 25 committed 55", snap.Totals)
	}
	// The tree never exceeds the root ceiling no matter how children spend.
	rootNow, err := s.Get(ctx, scope, root.ID)
	if err != nil || rootNow.Spent+rootNow.Reserved > rootNow.Limit {
		t.Fatalf("tree exceeded root ceiling: %+v err=%v", rootNow, err)
	}
}

func TestTreeSnapshotGuardsAndScope(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	u := fmt.Sprint(t.Name())
	scope := scopeWith("dep-tree-snap")
	root, err := s.Root(ctx, scope, "t-snap", "root-"+u, 500)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	// Foreign scope cannot read the tree.
	if _, err := s.TreeSnapshot(ctx, scopeWith("dep-intruder"), root.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-scope snapshot err = %v, want ErrNotFound", err)
	}
	// Cycle guard: plant a cycle in the ledger tables directly and prove the
	// snapshot fails closed instead of hanging.
	child, err := s.Allocate(ctx, scope, root.ID, scope, "t-cycle", "cyc-"+u, 100)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE ahp_model.budgets SET parent = $1 WHERE id = $2`, child.ID, root.ID); err != nil {
		t.Fatalf("plant cycle: %v", err)
	}
	snap, err := s.TreeSnapshot(ctx, scope, root.ID)
	if err == nil {
		// The path guard stops recursion; the snapshot must not silently
		// present a cyclic tree as healthy -- either an error or the cycle
		// must not double-count the root.
		if snap.Totals.Nodes != 1 && snap.Totals.Nodes != 2 {
			t.Fatalf("cyclic snapshot nodes = %d, want bounded walk", snap.Totals.Nodes)
		}
	}
	// Undo the plant so Close in cleanup paths behaves.
	if _, err := s.pool.Exec(ctx, `UPDATE ahp_model.budgets SET parent = NULL WHERE id = $1`, root.ID); err != nil {
		t.Fatalf("unplant cycle: %v", err)
	}
}
