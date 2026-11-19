package operations_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestTenantDeletionFencesAdmissionAndPreservesUnknownEffect(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	owner, ok := any(e.store).(interface {
		FenceTenant(context.Context, string) error
		DeleteTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("effect ledger has no tenant deletion barrier")
	}
	req := operations.Request{Key: "sent", Kind: "test", Target: "external", Arguments: json.RawMessage(`{}`)}
	if _, err := e.store.Accept(ctx, e.scope, req); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Dispatch(ctx, e.scope, req.Key); err != nil {
		t.Fatal(err)
	}
	req.Key = "unsent"
	if _, err := e.store.Accept(ctx, e.scope, req); err != nil {
		t.Fatal(err)
	}
	if err := owner.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	late := e.scope
	late.Subject = "unknown-subject"
	late.Deployment = "unknown-deployment"
	req.Key = "late"
	if _, err := e.store.Accept(ctx, late, req); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late effect admitted: %v", err)
	}
	if _, err := e.store.Dispatch(ctx, e.scope, "unsent"); err == nil {
		t.Fatal("unsent effect dispatched after tenant fence")
	}
	if err := owner.DeleteTenant(ctx, e.scope.Tenant); err == nil {
		t.Fatal("unknown external effect was silently erased")
	}
	op, err := e.store.Get(ctx, e.scope, "sent")
	if err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("lost ambiguity: %+v %v", op, err)
	}
	if _, err = e.store.Reconcile(ctx, e.scope, "sent", json.RawMessage(`{"confirmed":true}`), false); err != nil {
		t.Fatal(err)
	}
	if err = owner.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.Get(ctx, e.scope, "sent"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("data survived: %v", err)
	}
	if _, err = e.store.Accept(ctx, late, req); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("tenant resurrected: %v", err)
	}
}

func TestTenantFenceDrainsAdmittedTransaction(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = e.store.CheckTenantWritable(ctx, tx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- e.store.FenceTenant(ctx, e.scope.Tenant) }()
	select {
	case err := <-finished:
		t.Fatalf("barrier passed an admitted transaction: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("barrier did not drain")
	}
	if _, err = e.store.Accept(ctx, e.scope, operations.Request{Key: "late", Kind: "test", Target: "external"}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late accept: %v", err)
	}
}

func TestTenantCleanupRacesReplaysWithoutResurrection(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	req := operations.Request{Key: "replay", Kind: "test", Target: "external"}
	if _, err := e.store.Accept(ctx, e.scope, req); err != nil {
		t.Fatal(err)
	}
	if err := e.store.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	var racers sync.WaitGroup
	for i := 0; i < 8; i++ {
		racers.Add(1)
		go func() {
			defer racers.Done()
			for j := 0; j < 4; j++ {
				if _, err := e.store.Accept(ctx, e.scope, req); contracts.Code(err) != contracts.ErrForbidden {
					t.Errorf("replay bypassed fence: %v", err)
				}
				if err := e.store.DeleteTenant(ctx, e.scope.Tenant); err != nil {
					t.Errorf("cleanup failed: %v", err)
				}
			}
		}()
	}
	racers.Wait()
	if _, err := e.store.Get(ctx, e.scope, req.Key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("operation resurrected: %v", err)
	}
	if _, err := e.store.Accept(ctx, e.scope, req); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("tombstone disappeared: %v", err)
	}
}
