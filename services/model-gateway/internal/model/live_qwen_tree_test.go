//go:build liveqwen

// Live Qwen legs are compiled ONLY under the `liveqwen` build tag, so an ordinary
// `go test ./internal/model` neither contacts a provider nor reports a skip: the
// ambient suite is deterministic and hermetic. Run them explicitly with
//
//	AHP_LIVE_QWEN=1 go test -count=1 -tags liveqwen -run TestLive ./internal/model
//
// (the credential still comes only from the platform secret store).

package model

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestLiveQwenTreeBudgetSnapshot is the live budget-snapshot leg: a real Qwen
// generation runs inside a real budget tree (root -> child), and the whole
// tree is snapshotted BEFORE and AFTER the run. It proves on live traffic:
// per-node accounting (only the child that spent moved), per-tree totals
// (tree committed == settled charges, never over the root ceiling), capacity
// return on close, and refusal at exhaustion BEFORE any provider call.
//
// Opt-in like every live leg: AHP_LIVE_QWEN=1 plus the platform secret store.
// When AHP_H16_EVIDENCE names a path, the before/after snapshots are written
// there as JSON (facts only: ids, amounts, depths -- no credentials).
func TestLiveQwenTreeBudgetSnapshot(t *testing.T) {
	if os.Getenv("AHP_LIVE_QWEN") != "1" {
		t.Skip("AHP_LIVE_QWEN!=1: live Qwen smoke disabled")
	}
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	sec := requireLiveQwen(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	client, err := NewQwenClient(ctx, sec)
	if err != nil {
		t.Fatalf("NewQwenClient: %v", err)
	}
	env := newModelEnv(t)
	svc := env.service(client)

	const rootLimit = 200_000
	root, err := env.budget.Root(ctx, env.scope, "task-live-root", "lr-"+env.u, rootLimit)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	child, err := env.budget.Allocate(ctx, env.scope, root.ID, env.scope, "task-live-child", "lc-"+env.u, 10_000)
	if err != nil {
		t.Fatalf("allocate child: %v", err)
	}
	sib, err := env.budget.Allocate(ctx, env.scope, root.ID, env.scope, "task-live-sib", "ls-"+env.u, 5)
	if err != nil {
		t.Fatalf("allocate sibling: %v", err)
	}

	before, err := env.budget.TreeSnapshot(ctx, env.scope, root.ID)
	if err != nil {
		t.Fatalf("snapshot before: %v", err)
	}
	if before.Totals.Committed != 0 || before.Totals.Nodes != 3 {
		t.Fatalf("pre-run tree = %+v, want 3 nodes 0 committed", before.Totals)
	}
	// Root capacity view: the two open carve-outs are held against it.
	rootBefore, err := env.budget.Get(ctx, env.scope, root.ID)
	if err != nil || rootBefore.Reserved != 10_005 {
		t.Fatalf("root before = %+v err=%v, want reserved 10005", rootBefore, err)
	}

	// One real generation spends ONLY the child's reserve. Bounded attempts: this
	// model can lawfully answer finish="length" with no content (measured), and the
	// gateway now records that as the named refusal DEF-A7-1 -- a refusal is retried
	// like any caller would retry it, within a bound, and every attempt's settled
	// charge accumulates into the number the tree has to agree with.
	const attempts = 3
	var charged int64
	var done Generation
	var genID string
	for attempt := 1; ; attempt++ {
		g, err := svc.Submit(ctx, SubmitArgs{
			Scope: env.scope, Task: "task-live-child", Key: fmt.Sprintf("live-%s-%d", env.u, attempt),
			Model: sec.Model, Request: chatReq("Reply with exactly: AHP-BUDGET-TREE-OK"),
			BudgetID: child.ID, ReserveKey: fmt.Sprintf("lrk-%s-%d", env.u, attempt), ReserveAmount: 200,
		})
		if err != nil {
			t.Fatalf("submit attempt %d: %v", attempt, err)
		}
		genID = g.ID
		done, err = svc.Execute(ctx, env.scope, g.ID)
		if err != nil {
			if IsTransportError(err) {
				t.Skipf("live Qwen transport ambiguous (network egress blocked in this sandbox): %v", err)
			}
			t.Fatalf("execute attempt %d: %v", attempt, err)
		}
		charge := int64(done.Usage.TotalTokens)
		charged += charge
		if done.Status == GenCompleted {
			break
		}
		if IsEmptyCompletionRefusal(done) && attempt < attempts {
			t.Logf("provider refusal on attempt %d/%d (%s) -- charged %d, retrying under the bound", attempt, attempts, done.Error, charge)
			continue
		}
		t.Fatalf("live attempt %d ended %s (%s) after charging %d in total: expected a completion, or refusals only within the bound of %d",
			attempt, done.Status, done.Error, charged, attempts)
	}

	// Exhaustion BEFORE spend: the sibling's reserve is 5; asking 50 must be
	// refused by admission without a provider call, and nothing may move.
	if _, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-live-sib", Key: "live-sib-" + env.u,
		Model: sec.Model, Request: chatReq("should never run"),
		BudgetID: sib.ID, ReserveKey: "lsrk-" + env.u, ReserveAmount: 50,
	}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("exhausted-tree admission err = %v, want ErrConflict refusal", err)
	}

	after, err := env.budget.TreeSnapshot(ctx, env.scope, root.ID)
	if err != nil {
		t.Fatalf("snapshot after: %v", err)
	}
	if after.Totals.Spent != charged {
		t.Fatalf("tree spent after live run = %d, want settled charge %d", after.Totals.Spent, charged)
	}
	if after.Totals.Committed != charged || after.Totals.OpenHolds != 0 {
		t.Fatalf("tree totals after = %+v, want committed=spent, no open holds", after.Totals)
	}
	if after.Totals.Committed > rootLimit {
		t.Fatalf("tree committed %d over root limit %d", after.Totals.Committed, rootLimit)
	}
	// Per-node: only the child moved; the untouched sibling stayed at zero.
	for _, n := range after.Nodes {
		switch n.ID {
		case child.ID:
			if n.Spent != charged {
				t.Fatalf("child spent = %d, want %d", n.Spent, charged)
			}
		case sib.ID:
			if n.Spent != 0 || n.Reserved != 0 {
				t.Fatalf("refused sibling moved: %+v", n)
			}
		case root.ID:
			if n.Spent != 0 || n.Reserved != 10_005 {
				t.Fatalf("root view = %+v, want spent 0 reserved 10005 while children open", n)
			}
		}
	}

	// Capacity return: closing children propagates actual spend and releases
	// carve-outs up to the root.
	if err := env.budget.Close(ctx, env.scope, child.ID); err != nil {
		t.Fatalf("close child: %v", err)
	}
	if err := env.budget.Close(ctx, env.scope, sib.ID); err != nil {
		t.Fatalf("close sibling: %v", err)
	}
	rootAfter, err := env.budget.Get(ctx, env.scope, root.ID)
	if err != nil || rootAfter.Spent != charged || rootAfter.Reserved != 0 {
		t.Fatalf("root after closes = %+v err=%v, want spent %d reserved 0", rootAfter, err, charged)
	}

	if path := os.Getenv("AHP_H16_EVIDENCE"); path != "" {
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("evidence: %v", err)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		_ = enc.Encode(map[string]any{
			"profile":          sec.Model,
			"generation":       map[string]any{"id": genID, "status": done.Status, "usage_total": done.Usage.TotalTokens},
			"snapshot_before":  before,
			"snapshot_after":   after,
			"root_after_close": rootAfter,
			"note":             "amounts are ledger units (tokens); ids are opaque; no credentials",
		})
	}
	t.Logf("live tree snapshot: before=%+v after=%+v root_after_close %+v", before.Totals, after.Totals, rootAfter)
}
