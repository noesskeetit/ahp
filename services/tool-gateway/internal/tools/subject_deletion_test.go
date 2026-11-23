package tools

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
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

func TestToolLeaseErasureRetainsPendingAndOtherOwnerRows(t *testing.T) {
	e, ctx := newTestService(t)
	if err := e.svc.MigrateToolLeases(ctx); err != nil {
		t.Fatal(err)
	}
	localSubject := e.scope
	localSubject.Subject += "-other"
	foreign := e.scope
	foreign.Tenant += "-other"
	target := ToolLease{SessionID: "target", ContextID: "target-context", Epoch: 1, ExpiresAt: time.Now().Add(time.Minute)}
	for i, peer := range []contracts.Scope{localSubject, foreign} {
		lease := target
		if i == 0 {
			lease.SessionID = "other-subject"
		}
		if _, err := e.svc.FenceAndLookup(ctx, peer, "effect", lease); err != nil {
			t.Fatal(err)
		}
	}
	args := []byte(`{"target":"ambiguous"}`)
	digest, err := ApprovalDigest("errwrite", args)
	if err != nil {
		t.Fatal(err)
	}
	e.g.AllowDigest(digest)
	op, err := e.svc.CallWithToolLease(ctx, e.scope, "effect", "errwrite", args,
		CallProvenance{TaskID: "worker", ContextID: target.ContextID}, target)
	if err == nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("expected pending external effect: %v, %v", op, err)
	}

	assertRows := func(tenant string, want int) {
		t.Helper()
		for _, table := range []string{"gateway_operations", "gateway_session_epochs"} {
			var rows int
			if err := e.svc.pool.QueryRow(ctx, "SELECT count(*) FROM "+e.schema+"."+table+" WHERE tenant_id=$1", tenant).Scan(&rows); err != nil || rows != want {
				t.Fatalf("%s for %q has %d rows, want %d: %v", table, tenant, rows, want, err)
			}
		}
	}
	if err := e.svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("subject erasure must await settlement: %v", err)
	}
	if err := e.svc.DeleteTenant(ctx, e.scope.Tenant); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("tenant erasure must await settlement: %v", err)
	}
	assertRows(e.scope.Tenant, 2)
	key, err := leasedOperationKey(target, "effect")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Reconcile(ctx, e.scope, key, []byte(`{"settled":true}`), false); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.DeleteSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	assertRows(e.scope.Tenant, 1)
	assertRows(foreign.Tenant, 1)
	if err := e.svc.DeleteTenant(ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	assertRows(e.scope.Tenant, 0)
	assertRows(foreign.Tenant, 1)
}
