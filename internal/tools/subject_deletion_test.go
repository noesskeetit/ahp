package tools

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestSubjectDeletionToolsFencePreservesOtherOwner(t *testing.T) {
	e, ctx := newTestService(t)
	owner, ok := any(e.svc).(interface {
		FenceSubject(context.Context, string, string) error
		DeleteSubject(context.Context, string, string) error
	})
	if !ok {
		t.Fatal("tools subject owner missing")
	}
	other := e.scope
	other.Subject += "-other"
	for _, scope := range []contracts.Scope{e.scope, other} {
		if _, _, err := e.svc.CreateApprovalRequest(ctx, scope, "before", "echo", []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "late", "echo", []byte(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late write: %v", err)
	}
	if err := owner.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	var own, foreign int
	if err := e.svc.pool.QueryRow(ctx, "SELECT count(*) FILTER(WHERE subject=$2),count(*) FILTER(WHERE subject=$3) FROM "+e.schema+".approvals WHERE tenant=$1", e.scope.Tenant, e.scope.Subject, other.Subject).Scan(&own, &foreign); err != nil || own != 0 || foreign != 1 {
		t.Fatalf("own=%d foreign=%d err=%v", own, foreign, err)
	}
	if err := owner.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
}

func TestSubjectDeletionToolsCannotRecreateErasedApproverOnForeignRequest(t *testing.T) {
	e, ctx := newTestService(t)
	other := e.scope
	other.Subject += "-other"
	e.svc.cfg.ApprovalAuthorize = func(context.Context, contracts.Scope, string) error { return nil }
	id, digest, err := e.svc.CreateApprovalRequest(ctx, other, "assigned", "echo", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = e.svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.ApproveRequest(ctx, other, id, digest, "echo", e.humanTok); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("erased approver was able to sign foreign request: %v", err)
	}
	if ok, err := e.svc.HasApproval(ctx, other, "assigned", digest, "echo"); err != nil || ok {
		t.Fatalf("foreign approval changed: %v %v", ok, err)
	}
}
