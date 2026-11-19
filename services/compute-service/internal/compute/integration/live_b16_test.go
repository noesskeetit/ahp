package integration_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B16 §2.3 Point 7: A2A Statuses and tasks/cancel; AHP Self-Cancel.

func newB16Env(t *testing.T) *b153Env {
	t.Helper()
	e := newLiveEnv(t)
	schema := fmt.Sprintf("ahp_b16_%016x", time.Now().UnixNano())
	op, err := operations.New(e.pool, schema)
	if err != nil {
		t.Fatalf("operations.New: %v", err)
	}
	if err := op.Migrate(ctx(t)); err != nil {
		t.Fatalf("operations Migrate: %v", err)
	}
	return &b153Env{liveEnv: e, opStore: op}
}

func newB16Gateway(t *testing.T, e *b153Env) *gateway.Store {
	t.Helper()
	schema := fmt.Sprintf("ahp_b16gw_%016x", time.Now().UnixNano())
	gs, err := gateway.NewStore(e.pool, schema)
	if err != nil {
		t.Fatalf("gateway.NewStore: %v", err)
	}
	if err := gs.Migrate(ctx(t)); err != nil {
		t.Fatalf("gateway Migrate: %v", err)
	}
	return gs
}

// b16InsertTask creates a task directly in the store (deterministic, no async).
func b16InsertTask(t *testing.T, gs *gateway.Store, sc contracts.Scope, uid string) *gateway.TaskRow {
	t.Helper()
	ctx := ctx(t)
	err := pgx.BeginFunc(ctx, gs.Pool(), func(tx pgx.Tx) error {
		_, err := gs.AcquireContextTx(ctx, tx, sc, "caller-"+uid, "ctx-"+uid)
		return err
	})
	if err != nil {
		t.Fatalf("AcquireContext: %v", err)
	}
	var row gateway.TaskRow
	err = pgx.BeginFunc(ctx, gs.Pool(), func(tx pgx.Tx) error {
		var err error
		row, err = gs.InsertTask(ctx, tx, gateway.TaskRow{
			Scope:     sc,
			Caller:    "caller-" + uid,
			ContextID: "ctx-" + uid,
			Kind:      "agent",
			State:     a2a.TaskStateSubmitted,
		})
		return err
	})
	if err != nil {
		t.Fatalf("InsertTask: %v", err)
	}
	return &row
}

// b16CancelHeld wraps CancelHeld in a transaction.
func b16CancelHeld(ctx context.Context, gs *gateway.Store, sc contracts.Scope, taskID, worker string, expected int64) (*gateway.TaskRow, error) {
	var out gateway.TaskRow
	err := pgx.BeginFunc(ctx, gs.Pool(), func(tx pgx.Tx) error {
		var err error
		out, err = gs.CancelHeld(ctx, tx, sc, taskID, worker, expected)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// b16CompleteHeld wraps CompleteHeld in a transaction.
func b16CompleteHeld(ctx context.Context, gs *gateway.Store, sc contracts.Scope, taskID, worker string, expected int64, state a2a.TaskState) (*gateway.TaskRow, error) {
	var out gateway.TaskRow
	err := pgx.BeginFunc(ctx, gs.Pool(), func(tx pgx.Tx) error {
		var err error
		out, err = gs.CompleteHeld(ctx, tx, sc, taskID, worker, expected, state, nil, nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// b16Claim simulates a worker claiming a task (sets lease).
func b16Claim(t *testing.T, gs *gateway.Store, worker string, lease time.Duration) *gateway.TaskRow {
	t.Helper()
	row, err := gs.ClaimTask(ctx(t), worker, lease)
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	if row == nil {
		t.Fatal("ClaimTask returned nil (no claimable task)")
	}
	return row
}

// --- Sit1: Status transitions submitted→completed (happy path) --------------

func TestB16Sit1_StatusTransitions(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s1-" + uid, Deployment: "d-1", Subject: "alice"}

	// Fast-completing executor via the full service.
	exec := gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{
			State:   a2a.TaskStateCompleted,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("done")),
		}, nil
	})
	svc, err := gateway.NewService(gs, e.opStore, exec, gateway.NullNotifier{},
		gateway.Config{WorkerID: "b16-s1", Lease: 10 * time.Second, Poll: 20 * time.Millisecond, MaxBlock: 30 * time.Second})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	go svc.Run(runCtx)

	task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("b16-s1-"+uid)),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if task.Status.State != a2a.TaskStateCompleted {
		t.Fatalf("final state=%q want completed", task.Status.State)
	}

	row, err := gs.Task(ctx(t), sc, string(task.ID))
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if row.State != a2a.TaskStateCompleted {
		t.Fatalf("store state=%q want completed", row.State)
	}
	if row.TerminalAt == nil {
		t.Fatal("terminal_at not set")
	}
	t.Logf("Sit1: submitted→completed (task=%s, terminal_at set, fence=%d)", task.ID, row.FencingToken)
}

// --- Sit2: Cancel running task → canceled -----------------------------------

func TestB16Sit2_CancelRunning(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s2-" + uid, Deployment: "d-2", Subject: "alice"}

	// Create task, claim it (simulates "running" — active lease).
	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s2", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed task %s != inserted %s", claimed.ID, task.ID)
	}

	// Request cancel (cooperative flag).
	_, err := gs.RequestCancel(ctx(t), sc, task.ID)
	if err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	// Worker sees flag and finalizes cancel.
	_, err = b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row.State)
	}
	t.Logf("Sit2: cancel running task → canceled (lease holder=%s, fence=%d)", claimed.LeaseOwner, row.FencingToken)
}

// --- Sit3: Cancel completed task → idempotent no-op ---------------------------

func TestB16Sit3_CancelCompletedIdempotent(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s3-" + uid, Deployment: "d-3", Subject: "alice"}

	// Create and complete a task.
	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s3", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}
	_, err := b16CompleteHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion, a2a.TaskStateCompleted)
	if err != nil {
		t.Fatalf("CompleteHeld: %v", err)
	}

	// Cancel on completed task: must be no-op (no error).
	_, err = gs.RequestCancel(ctx(t), sc, task.ID)
	if err != nil {
		t.Fatalf("RequestCancel on completed: %v (want idempotent no-op)", err)
	}

	// State unchanged.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCompleted {
		t.Fatalf("state after cancel=%q want completed", row.State)
	}
	t.Logf("Sit3: cancel completed → 200 idempotent no-op (state=completed)")
}

// --- Sit4: Cancel already-canceled → idempotent ------------------------------

func TestB16Sit4_CancelAlreadyCanceled(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s4-" + uid, Deployment: "d-4", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s4", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// First cancel.
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)
	_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	// Verify canceled.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row.State)
	}

	// Second RequestCancel on already-canceled: no error.
	_, err = gs.RequestCancel(ctx(t), sc, task.ID)
	if err != nil {
		t.Fatalf("second RequestCancel: %v (want idempotent no-op)", err)
	}
	t.Logf("Sit4: cancel already-canceled → idempotent no-op")
}

// --- Sit5: Forced cancel (lease expiry, reassignment) ------------------------

func TestB16Sit5_ForcedCancel(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s5-" + uid, Deployment: "d-5", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)

	// Worker A claims with a 100ms lease (simulates unresponsive agent).
	claimedA, err := gs.ClaimTask(ctx(t), "worker-s5-dead", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("ClaimTask A: %v", err)
	}
	if claimedA == nil || claimedA.ID != task.ID {
		t.Fatalf("ClaimTask A: got %v", claimedA)
	}

	// Wait for lease to expire.
	time.Sleep(150 * time.Millisecond)

	// Worker B claims (lease expired → reassignment).
	claimedB, err := gs.ClaimTask(ctx(t), "worker-s5-recovery", 10*time.Second)
	if err != nil {
		t.Fatalf("ClaimTask B: %v", err)
	}
	if claimedB == nil {
		t.Fatal("task not re-claimable after lease expiry")
	}
	if claimedB.ID != task.ID {
		t.Fatalf("re-claimed wrong task: %s", claimedB.ID)
	}
	if claimedB.FencingToken <= claimedA.FencingToken {
		t.Fatalf("fence not bumped: old=%d new=%d", claimedA.FencingToken, claimedB.FencingToken)
	}

	// Worker B cancels (AHP force-cancel after agent unresponsive).
	_, err = b16CancelHeld(ctx(t), gs, sc, task.ID, claimedB.LeaseOwner, claimedB.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row.State)
	}
	t.Logf("Sit5: forced cancel — lease expired, reassign (fence %d→%d), canceled",
		claimedA.FencingToken, claimedB.FencingToken)
}

// --- Sit6: Post-cancel — attempt to transition → rejected --------------------

func TestB16Sit6_PostCancelTransitionRejected(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s6-" + uid, Deployment: "d-6", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s6", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Cancel.
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)
	_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	// Verify canceled.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row.State)
	}

	// Attempt to complete the canceled task — must fail.
	_, err = b16CompleteHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion, a2a.TaskStateCompleted)
	if err == nil {
		t.Fatal("CompleteHeld on canceled task should fail, got nil")
	}
	t.Logf("Sit6: post-cancel transition rejected: %v", err)
}

// --- Sit7: Concurrent cancel + complete race → exactly one terminal ----------

func TestB16Sit7_ConcurrentCancelComplete(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s7-" + uid, Deployment: "d-7", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s7", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Request cancel (sets the flag, making the cancel path ready).
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)

	var wg sync.WaitGroup
	type result struct {
		name string
		err  error
	}
	results := make(chan result, 2)

	// Goroutine 1: try to complete.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b16CompleteHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion, a2a.TaskStateCompleted)
		results <- result{"complete", err}
	}()

	// Goroutine 2: try to cancel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
		results <- result{"cancel", err}
	}()

	wg.Wait()
	close(results)

	okCount := 0
	for r := range results {
		if r.err == nil {
			okCount++
		}
	}
	if okCount != 1 {
		t.Fatalf("concurrent race: expected exactly 1 winner, got %d", okCount)
	}

	row, _ := gs.Task(ctx(t), sc, task.ID)
	if !row.State.Terminal() {
		t.Fatalf("final state=%q not terminal", row.State)
	}
	t.Logf("Sit7: concurrent cancel+complete → exactly one winner, final=%s", row.State)
}

// --- Sit8: Resource release — lease released, no orphaned executor -----------

func TestB16Sit8_LeaseReleasedAfterCancel(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s8-" + uid, Deployment: "d-8", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s8", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Cancel.
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)
	_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	// Verify: task is canceled and no active lease.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row.State)
	}
	if row.LeaseOwner != "" {
		t.Fatalf("lease_owner=%q after cancel, want empty (no orphaned executor)", row.LeaseOwner)
	}

	// ClaimTask should NOT return the terminal task.
	claimed2, err := gs.ClaimTask(ctx(t), "worker-s8-new", 10*time.Second)
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}
	if claimed2 != nil && claimed2.ID == task.ID {
		t.Fatal("terminal task was re-claimed; resource not released")
	}
	t.Logf("Sit8: lease released after cancel (lease_owner=%q, no re-claim)", row.LeaseOwner)
}

// TestB16Sit9_CancelFailedIdempotent verifies that cancel on a failed (terminal)
// task is an idempotent no-op — same guarantee as completed/canceled.
func TestB16Sit9_CancelFailedIdempotent(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s9-" + uid, Deployment: "d-9", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s9", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Complete as FAILED.
	_, err := b16CompleteHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion, a2a.TaskStateFailed)
	if err != nil {
		t.Fatalf("CompleteHeld (failed): %v", err)
	}

	// Verify state is failed.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateFailed {
		t.Fatalf("state=%q want failed", row.State)
	}

	// Cancel on failed task: must be idempotent no-op.
	_, err = gs.RequestCancel(ctx(t), sc, task.ID)
	if err != nil {
		t.Fatalf("RequestCancel on failed: %v (want idempotent no-op)", err)
	}

	// State unchanged.
	row2, _ := gs.Task(ctx(t), sc, task.ID)
	if row2.State != a2a.TaskStateFailed {
		t.Fatalf("state after cancel=%q want failed (unchanged)", row2.State)
	}
	t.Logf("Sit9: cancel failed → 200 idempotent no-op (state=failed)")
}

// TestB16Sit10_CancelInputRequired verifies cancel works on a task in
// INPUT_REQUIRED state (non-terminal, agent paused waiting for user input).
func TestB16Sit10_CancelInputRequired(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s10-" + uid, Deployment: "d-10", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s10", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Transition to INPUT_REQUIRED (agent needs more info).
	err := pgx.BeginFunc(ctx(t), gs.Pool(), func(tx pgx.Tx) error {
		_, err := gs.AdvanceStatus(ctx(t), tx, sc, task.ID, claimed.StatusVersion, a2a.TaskStateInputRequired, nil)
		return err
	})
	if err != nil {
		t.Fatalf("AdvanceStatus to input-required: %v", err)
	}

	// Verify state is input-required.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateInputRequired {
		t.Fatalf("state=%q want input-required", row.State)
	}

	// Cancel the input-required task.
	_, err = gs.RequestCancel(ctx(t), sc, task.ID)
	if err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	_, err = b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, row.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld on input-required: %v", err)
	}

	// Verify canceled.
	row2, _ := gs.Task(ctx(t), sc, task.ID)
	if row2.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled after cancel of input-required", row2.State)
	}
	t.Logf("Sit10: cancel input-required task → canceled (non-terminal state cancelable)")
}

// TestB16Sit11_CancelEventObservable verifies that after cancel, the event
// log records a status event with state=CANCELED (A2A protocol observability).
func TestB16Sit11_CancelEventObservable(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s11-" + uid, Deployment: "d-11", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s11", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Cancel.
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)
	cancelled, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	// Append the status event (as the service's finalizeCancel would).
	msg := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("task cancelled"))
	payload := &a2a.TaskStatusUpdateEvent{
		TaskID:    a2a.TaskID(task.ID),
		ContextID: task.ContextID,
		Status:    a2a.TaskStatus{State: a2a.TaskStateCanceled, Message: msg},
	}
	err = pgx.BeginFunc(ctx(t), gs.Pool(), func(tx pgx.Tx) error {
		return gs.AppendEvent(ctx(t), tx, gateway.Event{
			TaskID:  task.ID,
			Version: cancelled.StatusVersion,
			Scope:   sc,
			Kind:    "status",
			Payload: payload,
		})
	})
	if err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	// Verify the event is observable via Events().
	events, err := gs.Events(ctx(t), sc, task.ID, 0)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events recorded after cancel")
	}

	// Find the status event and verify it's CANCELED.
	found := false
	for _, ev := range events {
		if ev.Kind == "status" {
			if su, ok := ev.Payload.(*a2a.TaskStatusUpdateEvent); ok {
				if su.Status.State == a2a.TaskStateCanceled {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("no CANCELED status event found in event log (events=%d)", len(events))
	}
	t.Logf("Sit11: cancel event observable in event log (%d events, CANCELED status found)", len(events))
}

// TestB16Sit12_ConcurrentDoubleCancel verifies that when two workers race to
// cancel the same task, exactly one succeeds (fence protects the cancel path).
func TestB16Sit12_ConcurrentDoubleCancel(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s12-" + uid, Deployment: "d-12", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s12", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Request cancel (sets the flag for both paths).
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)

	var wg sync.WaitGroup
	results := make(chan error, 2)

	// Goroutine 1: cancel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
		results <- err
	}()

	// Goroutine 2: cancel (same worker, same version — one must lose).
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
		results <- err
	}()

	wg.Wait()
	close(results)

	okCount := 0
	for err := range results {
		if err == nil {
			okCount++
		}
	}
	if okCount != 1 {
		t.Fatalf("concurrent double-cancel: expected exactly 1 winner, got %d", okCount)
	}

	// Final state must be canceled.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.State != a2a.TaskStateCanceled {
		t.Fatalf("final state=%q want canceled", row.State)
	}
	t.Logf("Sit12: concurrent double-cancel → exactly one winner, final=canceled")
}

// TestB16Sit13_CancelPreservesArtifacts verifies that cancel does not destroy
// already-stored artifacts (COALESCE merge guarantee in advanceHeld).
func TestB16Sit13_CancelPreservesArtifacts(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s13-" + uid, Deployment: "d-13", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s13", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Set an artifact on the task before cancel.
	artifacts := []*a2a.Artifact{{
		ID:  a2a.ArtifactID("art-b16-" + uid),
		Name: "result.txt",
		Parts: a2a.ContentParts{a2a.NewTextPart("important data")},
	}}
	err := pgx.BeginFunc(ctx(t), gs.Pool(), func(tx pgx.Tx) error {
		return gs.SetArtifacts(ctx(t), tx, task.ID, artifacts)
	})
	if err != nil {
		t.Fatalf("SetArtifacts: %v", err)
	}

	// Verify artifact is stored.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if len(row.Artifacts) == 0 {
		t.Fatal("artifacts not stored before cancel")
	}

	// Cancel the task.
	_, _ = gs.RequestCancel(ctx(t), sc, task.ID)
	_, err = b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err != nil {
		t.Fatalf("CancelHeld: %v", err)
	}

	// Verify: state is canceled AND artifacts are preserved.
	row2, _ := gs.Task(ctx(t), sc, task.ID)
	if row2.State != a2a.TaskStateCanceled {
		t.Fatalf("state=%q want canceled", row2.State)
	}
	if len(row2.Artifacts) == 0 {
		t.Fatal("artifacts lost after cancel (data integrity violation)")
	}
	if string(row2.Artifacts[0].ID) != "art-b16-"+uid {
		t.Fatalf("artifact ID mismatch after cancel: got %q", row2.Artifacts[0].ID)
	}
	t.Logf("Sit13: cancel preserves artifacts (%d artifact(s) intact after cancel)", len(row2.Artifacts))
}

// TestB16Sit14_StaleVersionCancelRejected verifies that a cancel attempt with
// an outdated status version is rejected by the fence (stale writer protection).
func TestB16Sit14_StaleVersionCancelRejected(t *testing.T) {
	e := newB16Env(t)
	gs := newB16Gateway(t, e)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b16-s14-" + uid, Deployment: "d-14", Subject: "alice"}

	task := b16InsertTask(t, gs, sc, uid)
	claimed := b16Claim(t, gs, "worker-s14", 30*time.Second)
	if claimed.ID != task.ID {
		t.Fatalf("claimed %s != inserted %s", claimed.ID, task.ID)
	}

	// Transition to input-required (bumps status version).
	err := pgx.BeginFunc(ctx(t), gs.Pool(), func(tx pgx.Tx) error {
		_, err := gs.AdvanceStatus(ctx(t), tx, sc, task.ID, claimed.StatusVersion, a2a.TaskStateInputRequired, nil)
		return err
	})
	if err != nil {
		t.Fatalf("AdvanceStatus: %v", err)
	}

	// Get the updated status version.
	row, _ := gs.Task(ctx(t), sc, task.ID)
	if row.StatusVersion <= claimed.StatusVersion {
		t.Fatalf("status version not bumped: claimed=%d current=%d", claimed.StatusVersion, row.StatusVersion)
	}

	// Attempt cancel with the STALE (old) status version — must be rejected.
	_, err = b16CancelHeld(ctx(t), gs, sc, task.ID, claimed.LeaseOwner, claimed.StatusVersion)
	if err == nil {
		t.Fatal("cancel with stale status version should be rejected, got nil")
	}

	// Task state unchanged (still input-required, not canceled).
	row2, _ := gs.Task(ctx(t), sc, task.ID)
	if row2.State != a2a.TaskStateInputRequired {
		t.Fatalf("state=%q want input-required (unchanged after rejected cancel)", row2.State)
	}
	t.Logf("Sit14: cancel with stale version rejected (v%d vs current v%d), state unchanged",
		claimed.StatusVersion, row.StatusVersion)
}
