package tools

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// GAP-DOTE2E-7: the HITL face must let the employee DECLINE as well as approve,
// with the refusal attributed and journalled, and the declined operation must
// never execute or silently re-prompt.

// (D1) A declined request funds nothing: the call is refused with a distinct
// sentinel and no operation is journalled at all.
func TestDeclineBlocksTheEffect(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	if _, err := e.svc.CallWithProvenance(ctx, e.scope, "k-dec-1", "gated", args,
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"}); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("first call: err=%v, want ErrApprovalRequired", err)
	}
	reqs, err := e.svc.ListPendingApprovals(ctx, e.scope)
	if err != nil || len(reqs) != 1 {
		t.Fatalf("pending after refusal = %+v err=%v, want one request", reqs, err)
	}
	if _, err := e.svc.DeclineRequest(ctx, e.scope, reqs[0].RequestID, reqs[0].Digest, "gated", e.humanTok); err != nil {
		t.Fatalf("decline: %v", err)
	}
	if _, err := e.svc.CallWithProvenance(ctx, e.scope, "k-dec-1", "gated", args,
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"}); !errors.Is(err, ErrApprovalDeclined) {
		t.Fatalf("call after decline: err=%v, want ErrApprovalDeclined", err)
	}
	if _, err := e.svc.Get(ctx, e.scope, "k-dec-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("a declined operation must not be journalled: err=%v", err)
	}
}

// (D2) Approving after a decline is a conflict, not a second bite.
func TestApproveAfterDeclineConflicts(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	reqID, dig, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-dec-2", "gated", args)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if _, err := e.svc.DeclineRequest(ctx, e.scope, reqID, dig, "gated", e.humanTok); err != nil {
		t.Fatalf("decline: %v", err)
	}
	if _, err := e.svc.ApproveRequest(ctx, e.scope, reqID, dig, "gated", e.humanTok); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("approve after decline: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrConflict)
	}
	if ok, err := e.svc.HasApproval(ctx, e.scope, "k-dec-2", dig, "gated"); err != nil || ok {
		t.Fatalf("gate after decline: eligible=%v err=%v, want false", ok, err)
	}
}

// (D3) Decline is not a revocation of a signature already given (Fence stops the
// effect for that); signing twice in opposite directions is a conflict.
func TestDeclineAfterApproveConflicts(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	reqID, dig, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-dec-3", "gated", args)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if _, err := e.svc.ApproveRequest(ctx, e.scope, reqID, dig, "gated", e.humanTok); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if _, err := e.svc.DeclineRequest(ctx, e.scope, reqID, dig, "gated", e.humanTok); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("decline after approve: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrConflict)
	}
	op, err := e.svc.CallWithProvenance(ctx, e.scope, "k-dec-3", "gated", args,
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"})
	if err != nil || op.Status != "SUCCEEDED" {
		t.Fatalf("the signed approval must still fund exactly its own effect: op=%+v err=%v", op, err)
	}
}

// (D4) Decline is owner-scoped: a human cannot decline another subject's
// request by digest, and cannot consume their own by guessing a foreign id.
func TestDeclineIsOwnerScoped(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	other := contracts.Scope{Tenant: e.scope.Tenant, Deployment: "d" + randHex(6), Subject: "u" + randHex(6)}
	otherReq, dig, err := e.svc.CreateApprovalRequest(ctx, other, "k-dec-4", "gated", args)
	if err != nil {
		t.Fatalf("other tenant request: %v", err)
	}
	// Same tenant, different subject: the digest selector sees nothing of mine.
	if _, err := e.svc.DeclineRequest(ctx, e.scope, "", dig, "gated", e.humanTok); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("decline by digest with nothing of mine pending: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrNotFound)
	}
	// Guessing the other subject's request id is refused by the owner check and
	// must not touch their row -- the same accepted semantics as ApproveRequest
	// (TestApprovalRejectsAmbiguousLegacyAndForeignRequestID).
	if _, err := e.svc.DeclineRequest(ctx, e.scope, otherReq, dig, "gated", e.humanTok); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("decline by foreign request id: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	// Their row is untouched: still unsigned and still on their own worklist.
	if ok, err := e.svc.HasApproval(ctx, other, "k-dec-4", dig, "gated"); err != nil || ok {
		t.Fatalf("their request must stay unsigned: eligible=%v err=%v", ok, err)
	}
	list, err := e.svc.ListPendingApprovals(ctx, other)
	if err != nil || len(list) != 1 || list[0].RequestID != otherReq {
		t.Fatalf("their worklist = %+v err=%v, want their request still pending", list, err)
	}
}

// (D5) A declined operation is not silently re-raised as a fresh request: the
// worklist stays empty and the refusal names the decline.
func TestDeclineIsNotReraisedAsNewRequest(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	reqID, dig, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-dec-5", "gated", args)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if _, err := e.svc.DeclineRequest(ctx, e.scope, reqID, dig, "gated", e.humanTok); err != nil {
		t.Fatalf("decline: %v", err)
	}
	if _, err := e.svc.CallWithProvenance(ctx, e.scope, "k-dec-5", "gated", args,
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"}); !errors.Is(err, ErrApprovalDeclined) {
		t.Fatalf("call after decline: err=%v, want ErrApprovalDeclined", err)
	}
	list, err := e.svc.ListPendingApprovals(ctx, e.scope)
	if err != nil || len(list) != 0 {
		t.Fatalf("worklist after a declined call = %+v err=%v, want empty", list, err)
	}
}

// (D6) Only a human can decline: a task bearer is refused at the service too.
func TestDeclineRejectsTaskBearer(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	reqID, dig, err := e.svc.CreateApprovalRequest(ctx, e.scope, "k-dec-6", "gated", args)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	if _, err := e.svc.DeclineRequest(ctx, e.scope, reqID, dig, "gated", e.taskTok); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("task bearer decline: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
}
