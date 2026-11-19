package tools

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

// Isolated audit regression: the expected contract is per-operation approval.
func TestRootAuditApprovalCannotAuthorizeDifferentOperationOrSubject(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["write"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["write"] = spec
	args := json.RawMessage(`{"amount":7,"recipient":"audit-sink"}`)
	if _, err := e.svc.Call(ctx, e.scope, "approved-operation", "write", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("initial call: expected approval required, got %v", err)
	}
	digest, err := ApprovalDigest("write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	if _, err = e.svc.Call(ctx, e.scope, "approved-operation", "write", args); err != nil {
		t.Fatal(err)
	}
	t.Logf("after approved operation: effects=%d", atomic.LoadInt32(e.effCount))
	if _, err = e.svc.Call(ctx, e.scope, "unapproved-second-operation", "write", args); err == nil {
		t.Errorf("CONTRACT VIOLATION: new operation key reused old approval; effects=%d", atomic.LoadInt32(e.effCount))
	}
	foreign := e.scope
	foreign.Subject = "another-subject"
	foreign.Deployment = "another-deployment"
	if _, err = e.svc.Call(ctx, foreign, "unapproved-foreign-operation", "write", args); err == nil {
		t.Errorf("CONTRACT VIOLATION: another subject and deployment reused old approval; effects=%d", atomic.LoadInt32(e.effCount))
	}
}
