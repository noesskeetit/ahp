package gateway_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// TestSubmittedRowDurableBeforeSubmitResponse pins the create/commit/response
// boundary of the execution plane, which is what a delegated hop is measured
// against from outside (PDF rev3.2, 6: subtree effects must be auditable).
//
// This is a REGRESSION PIN, not a red test: DEF-H10-17 was measured and retracted, so
// the boundary described here is the product's current contract. A failing run of this
// test means the contract has regressed, not that the defect is still open.
//
// Shape under test: message/send must INSERT and COMMIT the task row before the
// submit response is produced, and the executor must run in a LATER
// transaction. So while the executor is inside its step -- before any response
// has been written -- a reader in a different transaction must already see the
// row in a non-terminal state. If the row were only materialised at settle time
// (the shape alleged by DEF-H10-17), this read would find nothing.
func TestSubmittedRowDurableBeforeSubmitResponse(t *testing.T) {
	e := newEnv(t)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	svc := e.service(t, gateway.ExecutorFunc(func(ctx context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return echoExecutor()(ctx, in)
		case <-ctx.Done():
			return gateway.ExecResult{}, ctx.Err()
		}
	}), nil)

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go svc.Run(runCtx)

	type sendResult struct {
		task *a2a.Task
		err  error
	}
	done := make(chan sendResult, 1)
	go func() {
		task, err := submit(t, svc, e.scope, e.caller, "msg-visible-before-response", func(r *a2a.SendMessageRequest) {
			r.Config.ReturnImmediately = false
		})
		done <- sendResult{task, err}
	}()

	select {
	case <-started:
	case <-time.After(20 * time.Second):
		t.Fatal("executor never entered a step: the task was never claimed, so the boundary is not observable")
	}

	select {
	case s := <-done:
		t.Fatalf("submit response was produced before the visibility read (task=%v err=%v): the row is not durable before the response", s.task, s.err)
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, _, _, err := e.store.ListTasks(ctx, e.scope, "", "", 10, "")
	if err != nil {
		t.Fatalf("out-of-band ListTasks while the executor is running: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected exactly 1 task row while the task is running, got %d", len(rows))
	}
	row := rows[0]
	if row.Terminal() {
		t.Fatalf("row already terminal while the executor is still inside its step: state=%s", row.State)
	}
	t.Logf("BOUNDARY_VISIBLE task_id=%s state=%s status_version=%d (executor still running, no response written yet)", row.ID, row.State, row.StatusVersion)

	close(release)

	var res sendResult
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("submit never returned after the executor was released")
	}
	if res.err != nil {
		t.Fatalf("awaiting send failed: %v", res.err)
	}
	if res.task == nil || res.task.ID == "" {
		t.Fatal("awaiting send returned no task")
	}
	if string(res.task.ID) != row.ID {
		t.Fatalf("response task id %s differs from the row observed before the response %s", res.task.ID, row.ID)
	}
	t.Logf("BOUNDARY_RESPONSE task_id=%s state=%s (arrived after the out-of-band read)", res.task.ID, res.task.Status.State)
}
