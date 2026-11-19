package tools

import (
	"encoding/json"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
	"time"
)

func TestApprovalDoesNotSignOtherPendingOwner(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"amount":7}`)
	_, digest, err := e.svc.CreateApprovalRequest(ctx, e.scope, "mine", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	foreign := e.scope
	foreign.Subject = "another-owner"
	_, _, err = e.svc.CreateApprovalRequest(ctx, foreign, "theirs", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.svc.HasApproval(ctx, foreign, "theirs", digest, "write"); err != nil || ok {
		t.Fatalf("foreign approval eligibility=%v err=%v", ok, err)
	}
}

func TestApprovalRequestRetryCannotExtendExpiry(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"amount":8}`)
	_, digest, err := e.svc.CreateApprovalRequest(ctx, e.scope, "ttl", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	_, err = e.svc.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s.approvals SET expires_at=now()-interval '1 second' WHERE op_key='ttl'`, e.schema))
	if err != nil {
		t.Fatal(err)
	}
	var before time.Time
	if err = e.svc.pool.QueryRow(ctx, fmt.Sprintf(`SELECT expires_at FROM %s.approvals WHERE op_key='ttl'`, e.schema)).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.svc.CreateApprovalRequest(ctx, e.scope, "ttl", "write", args); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.svc.HasApproval(ctx, e.scope, "ttl", digest, "write"); err != nil || ok {
		t.Fatalf("expired approval revived=%v err=%v", ok, err)
	}
}

func TestApprovalRejectsAmbiguousLegacyAndForeignRequestID(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"amount":9}`)
	first, digest, err := e.svc.CreateApprovalRequest(ctx, e.scope, "one", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := e.svc.CreateApprovalRequest(ctx, e.scope, "two", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("ambiguous legacy approve: %v", err)
	}
	if _, err = e.svc.ApproveRequest(ctx, e.scope, first, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	if ok, _ := e.svc.HasApproval(ctx, e.scope, "two", digest, "write"); ok {
		t.Fatal("request ID signed second operation")
	}
	foreign := e.scope
	foreign.Subject = "other-owner"
	theirs, _, err := e.svc.CreateApprovalRequest(ctx, foreign, "foreign", "write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.ApproveRequest(ctx, e.scope, theirs, digest, "write", e.humanTok); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign request signed: %v", err)
	}
	if _, err = e.svc.ApproveRequest(ctx, e.scope, second, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyApprovalMigrationDoesNotGrantKeyedAccess(t *testing.T) {
	e, ctx := newTestService(t)
	if _, err := e.svc.pool.Exec(ctx, fmt.Sprintf(`DROP TABLE %s.approvals; CREATE TABLE %s.approvals (
 tenant text NOT NULL,digest text NOT NULL,request_id text NOT NULL,tool text NOT NULL,approved_by text,created_at timestamptz NOT NULL DEFAULT now(),approved_at timestamptz,PRIMARY KEY(tenant,digest))`, e.schema, e.schema)); err != nil {
		t.Fatal(err)
	}
	digest, _ := ApprovalDigest("write", json.RawMessage(`{"amount":3}`))
	if _, err := e.svc.pool.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.approvals(tenant,digest,request_id,tool,approved_by) VALUES($1,$2,'legacy','write','prior-human')`, e.schema), e.scope.Tenant, digest); err != nil {
		t.Fatal(err)
	}
	if err := e.svc.applyDDL(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.svc.HasApproval(ctx, e.scope, "new-operation", digest, "write"); err != nil || ok {
		t.Fatalf("legacy grant authorized a keyed call: %v %v", ok, err)
	}
	if err := e.svc.applyDDL(ctx); err != nil {
		t.Fatalf("migration retry: %v", err)
	}
}
