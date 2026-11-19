package gateway_test

import (
	"context"
	"testing"
)

func TestTenantDeletionRejectsLateGatewayContext(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	if _, err := e.store.AcquireContext(ctx, e.scope, e.caller, ""); err != nil {
		t.Fatal(err)
	}
	owner, ok := any(e.store).(interface {
		FenceTenant(context.Context, string) error
		DeleteTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("gateway owner has no tenant barrier")
	}
	if err := owner.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.AcquireContext(ctx, e.scope, e.caller, ""); err == nil {
		t.Fatal("late context admitted")
	}
	if err := owner.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM `+e.schema+`.contexts WHERE tenant=$1`, e.scope.Tenant).Scan(&n); err != nil || n != 0 {
		t.Fatalf("context survived: %d %v", n, err)
	}
}
