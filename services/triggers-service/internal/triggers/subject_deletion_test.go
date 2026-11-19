package triggers

import (
	"context"
	"testing"
	"time"
)

func TestSubjectDeletionTriggersStopsSources(t *testing.T) {
	e, ctx := newTestService(t)
	owner, ok := any(e.svc).(interface {
		FenceSubject(context.Context, string, string) error
		DeleteSubject(context.Context, string, string) error
	})
	if !ok {
		t.Fatal("triggers subject owner missing")
	}
	tr := e.mkSchedule(t, ctx, time.Now().Add(-time.Minute), time.Minute, 3, true)
	other := e.scope
	other.Subject += "-other"
	spec := CreateScheduleSpec{ParentTask: "parent", DelegationRef: "ref", Target: "agent", Interval: time.Minute, MaxFires: 3, Delegated: true}
	foreign, err := e.svc.CreateSchedule(ctx, other, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.CreateSchedule(ctx, e.scope, spec); err == nil {
		t.Fatal("late source admitted")
	}
	if err = owner.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = e.pool.QueryRow(ctx, "SELECT count(*) FROM "+quoteIdent(e.schema)+".triggers WHERE trigger_id=$1", tr.TriggerID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("source survives: %d %v", n, err)
	}
	if err = e.pool.QueryRow(ctx, "SELECT count(*) FROM "+quoteIdent(e.schema)+".triggers WHERE trigger_id=$1", foreign.TriggerID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("foreign source harmed: %d %v", n, err)
	}
}

func TestSubjectFenceCannotBeUndoneByLateFireAcknowledgement(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, time.Now().Add(-time.Minute), time.Minute, 3, true)
	if err := e.svc.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.advance(ctx, tr, 1); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := e.pool.QueryRow(ctx, "SELECT status FROM "+quoteIdent(e.schema)+".triggers WHERE trigger_id=$1", tr.TriggerID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != StatusRevoked {
		t.Fatalf("late receipt resurrected source: %s", status)
	}
}
