package tools

import (
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestToolsTenantFenceStopsApprovalAndPolicyResurrection(t *testing.T) {
	e, ctx := newTestService(t)
	if _, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "before", "echo", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.policy.GrantOperatorPolicy(ctx, e.scope, "c", "echo", "operator"); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.FenceTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "after", "echo", []byte(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late approval: %v", err)
	}
	if err := e.svc.policy.GrantOperatorPolicy(ctx, e.scope, "c", "echo", "operator"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late policy: %v", err)
	}
	if err := e.svc.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"operations", "approvals", "guardian_log", "tool_policy", "tool_policy_audit"} {
		var n int
		if err := e.svc.pool.QueryRow(ctx, "SELECT count(*) FROM "+e.svc.schema+"."+table+" WHERE tenant=$1", e.scope.Tenant).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("table %s has %d rows", table, n)
		}
	}
	if _, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "after-delete", "echo", []byte(`{}`)); err == nil {
		t.Fatal("deleted tenant recreated")
	}
}
