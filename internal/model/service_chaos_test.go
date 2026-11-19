package model

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Chaos legs for the no-double-spend corner (PDF rev3.2, 2.4 + 9.2 + рис. 8):
// an ack lost after the side effect must converge on retry -- the same
// operation key returns the earlier fact, and the budget is charged exactly
// once no matter which window the previous attempt died in.

func chaosSubmit(t *testing.T, e *modelEnv, svc *Service, key, budgetID, reserveKey string, reserve int64) Generation {
	t.Helper()
	g, err := svc.Submit(context.Background(), SubmitArgs{
		Scope: e.scope, Task: "task-" + key, Key: key, Model: "fake-chaos",
		Request: chatReq("go"), BudgetID: budgetID, ReserveKey: reserveKey, ReserveAmount: reserve,
	})
	if err != nil {
		t.Fatalf("submit %s: %v", key, err)
	}
	return g
}

// TestChaosCrashAfterSettleConvergesSingleCharge reproduces the nastiest
// window: the reservation was settled (the charge committed) and the process
// died BEFORE the ledger completion and the terminal write. A replay used to
// wedge forever on a settlement mismatch; it must now adopt the recorded
// charge and converge to a terminal, conservative outcome -- never a second
// charge, never a silent provider replay.
func TestChaosCrashAfterSettleConvergesSingleCharge(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "fake-chaos"}
	inf.streamText([]string{"hello ", "world"}, Usage{TotalTokens: 40})
	svc := e.service(inf)

	root, err := e.budget.Root(ctx, e.scope, "t-chaos", "root-"+e.u, 1000)
	if err != nil {
		t.Fatalf("root budget: %v", err)
	}
	g := chaosSubmit(t, e, svc, "g-crash", root.ID, "res-"+e.u, 100)

	// Attempt #1 as observed from the outside: dispatch right taken, charge of
	// 40 committed -- then death. Nothing after the settle exists.
	if _, err := e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := e.budget.Settle(ctx, e.scope, root.ID, "res-"+e.u, 40); err != nil {
		t.Fatalf("settle before crash: %v", err)
	}

	// Replay through the real service path.
	res, err := svc.Execute(ctx, e.scope, g.ID)
	if err != nil {
		t.Fatalf("replay after settle-window crash: %v", err)
	}
	if !terminal(res.Status) {
		t.Fatalf("generation not terminal after replay: %+v", res)
	}
	if res.Status != GenFailed {
		t.Fatalf("ambiguous outcome must be terminal failure, got %q", res.Status)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 40 || got.Reserved != 0 {
		t.Fatalf("budget after replay: %+v err=%v (want spent 40, reserved 0)", got, err)
	}
	op, err := e.ledger.Get(ctx, e.scope, g.EffectKey)
	if err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("ledger op = %+v err=%v, want SIDE_EFFECT_UNKNOWN for reconciliation", op, err)
	}
	// Further replays are stable: same terminal fact, same single charge.
	for i := 0; i < 3; i++ {
		again, err := svc.Execute(ctx, e.scope, g.ID)
		if err != nil || again.Status != res.Status {
			t.Fatalf("replay %d: %+v err=%v", i, again, err)
		}
	}
	got, err = e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 40 || got.Reserved != 0 {
		t.Fatalf("budget drifted under replays: %+v err=%v", got, err)
	}
	if inf.count() != 0 {
		t.Fatalf("replay must not call the provider again, calls=%d", inf.count())
	}
}

// TestChaosCrashAfterLedgerCompleteConverges covers the window between the
// ledger completion and the terminal generation write: the result IS known,
// so the replay must adopt the stored result and settle exactly once.
func TestChaosCrashAfterLedgerCompleteConverges(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "fake-chaos"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 40})
	svc := e.service(inf)

	root, err := e.budget.Root(ctx, e.scope, "t-chaos2", "root-"+e.u, 1000)
	if err != nil {
		t.Fatalf("root budget: %v", err)
	}
	g := chaosSubmit(t, e, svc, "g-crash2", root.ID, "res-"+e.u, 100)

	if _, err := e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	resJSON, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: Usage{TotalTokens: 40}})
	if _, err := e.ledger.Complete(ctx, e.scope, g.EffectKey, resJSON, false); err != nil {
		t.Fatalf("ledger complete: %v", err)
	}

	res, err := svc.Execute(ctx, e.scope, g.ID)
	if err != nil {
		t.Fatalf("replay after complete-window crash: %v", err)
	}
	if res.Status != GenCompleted {
		t.Fatalf("status = %q, want COMPLETED from stored result", res.Status)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Spent != 40 || got.Reserved != 0 {
		t.Fatalf("budget after recovery: %+v err=%v", got, err)
	}
	// And a fully settled + finalized generation replayed again: still 40.
	if _, err := svc.Execute(ctx, e.scope, g.ID); err != nil {
		t.Fatalf("terminal replay: %v", err)
	}
	got, _ = e.budget.Get(ctx, e.scope, root.ID)
	if got.Spent != 40 || got.Reserved != 0 {
		t.Fatalf("budget drifted: %+v", got)
	}
	if inf.count() != 0 {
		t.Fatalf("stored result must not re-call the provider, calls=%d", inf.count())
	}
}

// TestChaosReserveAckLostSingleHold: a lost ack means the client retries the
// reservation, not that the tree gains a second hold.
func TestChaosReserveAckLostSingleHold(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	root, err := e.budget.Root(ctx, e.scope, "t-hold", "root-"+e.u, 100)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	r1, err := e.budget.Reserve(ctx, e.scope, root.ID, "rk-"+e.u, 50)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Retry after a lost ack: same key, same request -> same hold.
	r2, err := e.budget.Reserve(ctx, e.scope, root.ID, "rk-"+e.u, 50)
	if err != nil || r2.ID != r1.ID {
		t.Fatalf("retry: %+v vs %+v err=%v", r2, r1, err)
	}
	got, _ := e.budget.Get(ctx, e.scope, root.ID)
	if got.Reserved != 50 {
		t.Fatalf("double hold: %+v", got)
	}
	// A different amount under the same key is a reuse violation, not a
	// second hold.
	if _, err := e.budget.Reserve(ctx, e.scope, root.ID, "rk-"+e.u, 60); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("mismatch retry err = %v, want ErrKeyReuse", err)
	}
}

// TestChaosRetryStormUnderLimit proves the concurrency corner end-to-end at
// the generation layer: N parallel Executes of the SAME generation. Exactly
// one provider call may happen (the dispatch right), the generation ends
// terminal, and the budget is charged exactly once, never past the
// reservation -- whichever path (winner's usage or conservative full hold)
// the storm settles on.
func TestChaosRetryStormUnderLimit(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "fake-chaos"}
	slow := make(chan struct{})

	inf.newStr = func(ChatRequest) (Stream, error) {
		<-slow
		fs := &fakeStream{errAt: -1, usage: Usage{TotalTokens: 33}}
		fs.deltas = []Delta{{Text: "done"}}
		return fs, nil
	}
	svc := e.service(inf)

	root, err := e.budget.Root(ctx, e.scope, "t-storm", "root-"+e.u, 100)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	g := chaosSubmit(t, e, svc, "g-storm", root.ID, "res-"+e.u, 80)

	const n = 8
	var wg sync.WaitGroup
	results := make([]Generation, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = svc.Execute(ctx, e.scope, g.ID)
		}(i)
	}
	// Let all Executes enter, then release the provider stream.
	time.Sleep(150 * time.Millisecond)
	close(slow)
	wg.Wait()

	terminalCount := 0
	for i := 0; i < n; i++ {
		if errs[i] == nil && terminal(results[i].Status) {
			terminalCount++
		}
	}
	if terminalCount == 0 {
		t.Fatalf("no Execute reached terminal: errs=%v", errs)
	}
	inf.mu.Lock()
	calls := inf.calls
	inf.mu.Unlock()
	if calls != 1 {
		t.Fatalf("provider called %d times, dispatch right must allow exactly 1", calls)
	}
	final, err := svc.Usage(ctx, e.scope, g.ID)
	if err != nil || !terminal(final.Status) {
		t.Fatalf("generation not terminal after storm: %+v err=%v", final, err)
	}
	got, err := e.budget.Get(ctx, e.scope, root.ID)
	if err != nil || got.Reserved != 0 || got.Spent <= 0 || got.Spent > 80 {
		t.Fatalf("budget after storm: %+v err=%v (want single charge in (0,80])", got, err)
	}
	snap, err := e.budget.TreeSnapshot(ctx, e.scope, root.ID)
	if err != nil || snap.Totals.Committed != got.Spent {
		t.Fatalf("tree snapshot after storm: %+v err=%v", snap, err)
	}
}
