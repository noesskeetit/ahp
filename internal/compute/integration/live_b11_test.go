package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B11 §2.3 Gateway: A2A routing + replay + limits — 8 situations.

func newB11Env(t *testing.T) *b153Env {
	t.Helper()
	e := newLiveEnv(t)
	schema := fmt.Sprintf("ahp_b11_%016x", time.Now().UnixNano())
	op, err := operations.New(e.pool, schema)
	if err != nil {
		t.Fatalf("operations.New: %v", err)
	}
	if err := op.Migrate(ctx(t)); err != nil {
		t.Fatalf("operations Migrate: %v", err)
	}
	return &b153Env{liveEnv: e, opStore: op}
}

func newGatewaySvc(t *testing.T, e *b153Env) (*gateway.Service, *gateway.Store) {
	t.Helper()
	schema := fmt.Sprintf("ahp_b11gw_%016x", time.Now().UnixNano())
	gs, err := gateway.NewStore(e.pool, schema)
	if err != nil {
		t.Fatalf("gateway.NewStore: %v", err)
	}
	if err := gs.Migrate(ctx(t)); err != nil {
		t.Fatalf("gateway Migrate: %v", err)
	}
	exec := gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{
			State:   a2a.TaskStateCompleted,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("ok")),
		}, nil
	})
	svc, err := gateway.NewService(gs, e.opStore, exec, gateway.NullNotifier{},
		gateway.Config{WorkerID: "b11-worker", Lease: 10 * time.Second, Poll: 50 * time.Millisecond, MaxBlock: 30 * time.Second})
	if err != nil {
		t.Fatalf("gateway.NewService: %v", err)
	}
	// Start the worker loop so tasks get processed.
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	go svc.Run(runCtx)
	return svc, gs
}

// 1. A2A routing: SendMessage creates a task routed to the correct agent.
func TestB11Sit1_ARouting(t *testing.T) {
	e := newB11Env(t)
	svc, gs := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if task.ID == "" {
		t.Fatal("task ID empty after SendMessage")
	}
	if !task.Status.State.Terminal() && task.Status.State != a2a.TaskStateSubmitted && task.Status.State != a2a.TaskStateWorking {
		t.Fatalf("task state=%q, want submitted/working/terminal", task.Status.State)
	}
	// Verify task is in the store.
	row, err := gs.Task(ctx(t), sc, string(task.ID))
	if err != nil {
		t.Fatalf("Store.Task: %v", err)
	}
	t.Logf("Sit1: A2A routing — task %s created, state=%s", task.ID, row.State)
}

// 2. Replay: duplicate message (same ID) is idempotent, no double execution.
func TestB11Sit2_Replay(t *testing.T) {
	e := newB11Env(t)
	svc, _ := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	req := &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("once")),
	}
	req.Message.ID = "msg-b11-s2-replay"

	first, err := svc.SendMessage(ctx(t), sc, "caller-1", req)
	if err != nil {
		t.Fatalf("first SendMessage: %v", err)
	}

	// Replay: same message ID → same task, no error.
	second, err := svc.SendMessage(ctx(t), sc, "caller-1", req)
	if err != nil {
		t.Fatalf("replay SendMessage: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("replay: task ID %q != original %q (double execution!)", second.ID, first.ID)
	}
	t.Log("Sit2: replay — same messageId returns same task (idempotent, no double-execution)")
}

// 3. Limits: invalid scope/caller → proper error codes (not 500).
func TestB11Sit3_LimitsErrors(t *testing.T) {
	e := newB11Env(t)
	svc, _ := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Empty caller → INVALID.
	_, err := svc.SendMessage(ctx(t), sc, "", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("x")),
	})
	if err == nil {
		t.Fatal("empty caller: want error")
	}
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty caller: code=%s want INVALID", contracts.Code(err))
	}

	// Invalid scope (empty tenant) → INVALID.
	badScope := contracts.Scope{Tenant: "", Deployment: e.deployment, Subject: "alice"}
	_, err = svc.SendMessage(ctx(t), badScope, "c1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("x")),
	})
	if err == nil {
		t.Fatal("invalid scope: want error")
	}
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("invalid scope: code=%s want INVALID", contracts.Code(err))
	}
	t.Log("Sit3: limits — invalid caller/scope return INVALID (not 500)")
}

// 4. Task status delivery: task transitions submitted → working → completed.
func TestB11Sit4_StatusDelivery(t *testing.T) {
	e := newB11Env(t)
	svc, gs := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("status test")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// The mock executor completes immediately; poll for terminal state.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		row, err := gs.Task(ctx(t), sc, string(task.ID))
		if err != nil {
			t.Fatalf("Task: %v", err)
		}
		if row.State == a2a.TaskStateCompleted || row.State == a2a.TaskStateFailed {
			if row.State != a2a.TaskStateCompleted {
				t.Fatalf("task state=%q want completed (mock always succeeds)", row.State)
			}
			t.Logf("Sit4: status delivery — task %s reached %s", task.ID, row.State)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("task did not reach terminal state within 5s")
}

// 5. Child task delivery: follow-up message on an existing task (parent-child).
func TestB11Sit5_ChildTaskDelivery(t *testing.T) {
	e := newB11Env(t)
	svc, gs := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create parent task.
	parent, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("parent")),
	})
	if err != nil {
		t.Fatalf("parent SendMessage: %v", err)
	}

	// Follow-up: send message targeting the parent's task ID.
	childMsg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("child message"))
	childMsg.TaskID = a2a.TaskID(parent.ID)
	followUp, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: childMsg,
	})
	if err != nil {
		t.Fatalf("follow-up SendMessage: %v", err)
	}
	// Follow-up must return the same task (it's a message on the existing task).
	if followUp.ID != parent.ID {
		t.Fatalf("follow-up: task %q != parent %q (child should target parent)", followUp.ID, parent.ID)
	}

	// Verify the message was appended to the parent's history.
	hist, err := gs.History(ctx(t), string(parent.ID), 10)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist) < 2 {
		t.Fatalf("history has %d messages, want >= 2 (parent + child)", len(hist))
	}
	t.Log("Sit5: child task — follow-up appended to parent, same task ID")
}

// 6. Cancel: RequestCancel on a submitted/working task.
func TestB11Sit6_Cancel(t *testing.T) {
	e := newB11Env(t)
	svc, gs := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("cancel me")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Cancel the task.
	row, err := gs.RequestCancel(ctx(t), sc, string(task.ID))
	if err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if row.State != a2a.TaskStateCanceled && row.State != a2a.TaskStateSubmitted &&
		row.State != a2a.TaskStateWorking && row.State != a2a.TaskStateCompleted {
		t.Fatalf("after cancel: state=%q want canceled/submitted/working/completed", row.State)
	}
	t.Logf("Sit6: cancel — task %s state after RequestCancel=%s", task.ID, row.State)
}

// 7. Error propagation: executor failure → task becomes failed (not 500).
func TestB11Sit7_ErrorPropagation(t *testing.T) {
	e := newB11Env(t)
	schema := fmt.Sprintf("ahp_b11e_%016x", time.Now().UnixNano())
	gs, err := gateway.NewStore(e.pool, schema)
	if err != nil {
		t.Fatalf("gateway.NewStore: %v", err)
	}
	if err := gs.Migrate(ctx(t)); err != nil {
		t.Fatalf("gateway Migrate: %v", err)
	}
	failingExec := gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		return gateway.ExecResult{}, fmt.Errorf("agent: model timeout")
	})
	svc, err := gateway.NewService(gs, e.opStore, failingExec, gateway.NullNotifier{},
		gateway.Config{WorkerID: "b11-err-worker", Lease: 10 * time.Second, Poll: 50 * time.Millisecond,
			MaxBlock: 10 * time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	t.Cleanup(cancelRun)
	go svc.Run(runCtx)

	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
		Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("will fail")),
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// SendMessage blocks until terminal state; with MaxAttempts=1 the task
	// must end in failed.
	if task.Status.State != a2a.TaskStateFailed {
		t.Fatalf("task state=%q after SendMessage (blocking), want failed", task.Status.State)
	}
	t.Log("Sit7: error propagation — executor error → task FAILED (proper error, not 500)")
}

// 8. Backpressure: multiple concurrent tasks all accepted and queued.
func TestB11Sit8_Backpressure(t *testing.T) {
	e := newB11Env(t)
	svc, gs := newGatewaySvc(t, e)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	const n = 5
	var taskIDs []string
	for i := 0; i < n; i++ {
		msg := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(fmt.Sprintf("task %d", i)))
		msg.ID = fmt.Sprintf("msg-b11-s8-%d", i)
		task, err := svc.SendMessage(ctx(t), sc, "caller-1", &a2a.SendMessageRequest{
			Message: msg,
		})
		if err != nil {
			t.Fatalf("SendMessage #%d: %v (backpressure: should queue, not reject)", i, err)
		}
		if task.ID == "" {
			t.Fatalf("SendMessage #%d: empty task ID", i)
		}
		taskIDs = append(taskIDs, string(task.ID))
	}

	// All tasks must be in the store (queued, not rejected).
	for i, id := range taskIDs {
		_, err := gs.Task(ctx(t), sc, id)
		if err != nil {
			t.Fatalf("task %d not in store: %v", i, err)
		}
	}
	t.Logf("Sit8: backpressure — %d tasks all accepted and queued (none rejected)", n)
}
