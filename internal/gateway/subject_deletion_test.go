package gateway_test

import (
	"context"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/operations"
	"testing"
	"time"
)

func TestSubjectDeletionGatewayKeepsForeignContext(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	owner, ok := any(e.store).(interface {
		FenceSubject(context.Context, string, string) error
		DeleteSubject(context.Context, string, string) error
	})
	if !ok {
		t.Fatal("gateway subject owner missing")
	}
	other := e.scope
	other.Subject += "-other"
	if _, err := e.store.AcquireContext(ctx, e.scope, e.caller, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.AcquireContext(ctx, other, e.caller, ""); err != nil {
		t.Fatal(err)
	}
	if err := owner.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.AcquireContext(ctx, e.scope, e.caller, ""); err == nil {
		t.Fatal("late context admitted")
	}
	if err := owner.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	var own, foreign int
	if err := e.pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE subject=$2),count(*) FILTER(WHERE subject=$3) FROM "+e.schema+".contexts WHERE tenant=$1", e.scope.Tenant, e.scope.Subject, other.Subject).Scan(&own, &foreign); err != nil || own != 0 || foreign != 1 {
		t.Fatalf("own=%d foreign=%d err=%v", own, foreign, err)
	}
}

func TestSubjectDeletionGatewayKeepsTerminalReceiptUntilTaskFinalization(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	svc := e.service(t, echoExecutor(), nil)
	task := mustSubmit(t, svc, e.scope, e.caller, "subject-receipt", nil)
	claimed, err := e.store.ClaimTask(ctx, e.worker, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimed == nil || claimed.ID != string(task.ID) {
		t.Fatal("task not claimed")
	}
	req := operations.Request{Key: "terminal-effect", Kind: "model", Target: "fixture", Arguments: []byte(`{}`)}
	if _, err = e.ledger.Accept(ctx, e.scope, req); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ledger.Dispatch(ctx, e.scope, req.Key); err != nil {
		t.Fatal(err)
	}
	if _, err = e.ledger.Complete(ctx, e.scope, req.Key, []byte(`{"known":true}`), false); err != nil {
		t.Fatal(err)
	}
	if err = e.store.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err = e.store.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err == nil {
		t.Fatal("active task erased")
	}
	if _, err = e.ledger.Get(ctx, e.scope, req.Key); err != nil {
		t.Fatal("terminal recovery receipt lost", err)
	}
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = e.store.CancelHeld(ctx, tx, e.scope, claimed.ID, e.worker, claimed.StatusVersion); err != nil {
		t.Fatal(err)
	}
	if err = e.store.AppendEvent(ctx, tx, gateway.Event{TaskID: claimed.ID, Version: claimed.StatusVersion + 1, Scope: e.scope, Kind: "status", Payload: &a2a.TaskStatusUpdateEvent{TaskID: a2a.TaskID(claimed.ID), ContextID: claimed.ContextID, Status: a2a.TaskStatus{State: a2a.TaskStateCanceled}}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = e.store.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
}
