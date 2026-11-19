package gateway_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
)

// DEF-H22-1: a task whose executor died with an unprovable side effect must not
// stay WORKING forever.
//
// The scenario is the crash-in-window one: the first attempt dispatches, the
// process dies, the ledger row becomes SIDE_EFFECT_UNKNOWN and the guard refuses
// every later attempt. Before the fix the worker handed the lease back and the
// task was re-claimed silently and unboundedly -- attempts and fencing tokens
// climbed while the task sat in WORKING with nothing written for an operator.
//
// What this test proves, and nothing more:
//   - the task reaches a TERMINAL outcome within a bounded number of claims
//     (exactly the configured attempt ceiling), with an explicit reason;
//   - reaching that verdict never replays the ambiguous effect: the executor ran
//     exactly once, and the ledger row is preserved as evidence;
//   - a terminal task is no longer claimable, so the loop cannot restart.
func TestUnrecoverableTaskReachesTerminalOutcomeBounded(t *testing.T) {
	e := newEnv(t)
	const ceiling = 3

	var calls atomic.Int64
	exec := gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		calls.Add(1)
		// The effect may or may not have escaped: the gateway must never guess.
		return gateway.ExecResult{}, gateway.ErrAmbiguous
	})
	svc := e.service(t, exec, func(c *gateway.Config) {
		c.MaxAttempts = ceiling
		// The park window is what a human gets; shrink it so the bounded
		// re-claim sequence is observable inside one test run.
		c.ParkLease = 5 * time.Millisecond
	})
	task := mustSubmit(t, svc, e.scope, e.caller, "msg-def-h22-1", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)

	waitFor(t, 30*time.Second, func() bool {
		row, err := e.store.Task(ctx, e.scope, string(task.ID))
		return err == nil && row.Terminal()
	}, "task never reached a terminal outcome: it is still drifting in a non-terminal state (DEF-H22-1)")

	row, err := e.store.Task(ctx, e.scope, string(task.ID))
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if row.State != a2a.TaskStateFailed {
		t.Fatalf("terminal state = %q, want FAILED: an unrecoverable task must end with an explicit failure, not a fabricated completion", row.State)
	}
	if row.Attempts > ceiling {
		t.Fatalf("task was claimed %d times with MaxAttempts=%d: the resume loop is not bounded", row.Attempts, ceiling)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor ran %d times, want exactly 1: a terminal verdict must not replay an effect whose outcome is unknown", got)
	}
	if row.Metadata["close_reason"] != gateway.CloseReasonAttemptCeiling {
		t.Fatalf("close_reason = %v, want %q so the give-up is attributable", row.Metadata["close_reason"], gateway.CloseReasonAttemptCeiling)
	}
	if row.Metadata["reconcile_required"] != true {
		t.Fatalf("metadata %v: an abandoned task must announce that a reconcile decision is owed", row.Metadata)
	}
	if reason, _ := row.Metadata["abandon_reason"].(string); reason == "" {
		t.Fatalf("metadata %v: the terminal verdict must carry the reason it stopped", row.Metadata)
	}

	// The evidence a human reconcile needs is untouched: attempt 1 still reads
	// SIDE_EFFECT_UNKNOWN, not SUCCEEDED, not FAILED, not erased.
	op, err := e.ledger.Get(ctx, e.scope, "exec:"+string(task.ID)+":1")
	if err != nil {
		t.Fatalf("ledger.Get attempt-1 row: %v", err)
	}
	if op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("attempt-1 ledger status = %q, want SIDE_EFFECT_UNKNOWN: the abandon verdict must not rewrite what actually escaped", op.Status)
	}

	// And the loop cannot restart: a terminal task is not claimable any more.
	for i := 0; i < 20; i++ {
		if claim, err := e.store.ClaimTask(ctx, e.worker+"-probe", 20*time.Millisecond); err != nil {
			t.Fatalf("ClaimTask: %v", err)
		} else if claim != nil {
			t.Fatalf("an abandoned task became claimable again (task %s): the bounded loop is not closed", claim.ID)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("executor ran %d times after the terminal verdict, want 1", got)
	}
}
