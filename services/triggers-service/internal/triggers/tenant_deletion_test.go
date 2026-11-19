package triggers

import (
	"context"
	"testing"
	"time"
)

func TestTenantDeletionStopsSchedulesAndLateCreation(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, time.Now().Add(-time.Minute), time.Minute, 3, true)
	owner, ok := any(e.svc).(interface {
		FenceTenant(context.Context, string) error
		DeleteTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("triggers owner has no tenant barrier")
	}
	if err := owner.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.CreateSchedule(ctx, e.scope, CreateScheduleSpec{ParentTask: "parent", DelegationRef: "ref", Target: "agent", Interval: time.Minute, MaxFires: 3, Delegated: true}); err == nil {
		t.Fatal("late schedule admitted")
	}
	if err := owner.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM `+quoteIdent(e.schema)+`.triggers WHERE trigger_id=$1`, tr.TriggerID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("schedule survived: %d %v", n, err)
	}
}
