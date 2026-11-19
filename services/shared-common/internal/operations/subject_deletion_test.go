package operations_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"testing"
	"time"
)

func TestSubjectFenceDrainsDispatchAndAtomicErase(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := e.store.Accept(ctx, e.scope, operations.Request{Key: "sent", Kind: "model", Target: "provider"}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	dispatched := make(chan error, 1)
	go func() {
		_, err := e.store.DispatchChecked(ctx, e.scope, "sent", func(context.Context, pgx.Tx) error { close(entered); <-release; return nil })
		dispatched <- err
	}()
	<-entered
	fenced := make(chan error, 1)
	go func() { fenced <- e.store.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject) }()
	select {
	case err := <-fenced:
		t.Fatalf("barrier passed live dispatch: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-dispatched; err != nil {
		t.Fatal(err)
	}
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if err := e.store.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err == nil {
		t.Fatal("unknown effect erased")
	}
	other := e.scope
	other.Subject = "other"
	if _, err := e.store.Accept(ctx, other, operations.Request{Key: "other", Kind: "model", Target: "provider"}); err != nil {
		t.Fatal("foreign subject blocked", err)
	}
	other = e.scope
	other.Deployment = "new-deployment"
	if _, err := e.store.Accept(ctx, other, operations.Request{Key: "late", Kind: "model", Target: "provider"}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("late deployment admitted", err)
	}
	if _, err := e.store.Reconcile(ctx, e.scope, "sent", json.RawMessage(`{"confirmed":true}`), false); err != nil {
		t.Fatal(err)
	}
	aborted := errors.New("content transaction interrupted")
	if err := e.store.DeleteSubjectChecked(ctx, e.scope.Tenant, e.scope.Subject, func(context.Context, pgx.Tx) error { return aborted }); !errors.Is(err, aborted) {
		t.Fatal(err)
	}
	if _, err := e.store.Get(ctx, e.scope, "sent"); err != nil {
		t.Fatal("receipt lost on content rollback", err)
	}
	if err := e.store.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.Get(ctx, e.scope, "sent"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("receipt remains", err)
	}
}
