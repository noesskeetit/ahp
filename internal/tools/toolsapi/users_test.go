package toolsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestHumanBrowserApprove (DEF-TGW-2, director contract 2026-11-02): on an
// Identity-composed mount the human legs (Approve, Approvals) run under
// transport.TaskOrUser with the browser user audience. The red state this
// proves fixed: the sanctioned human token (aud=ahp-browser) used to be
// refused at the edge with 403, making /v1/tools/Approve unreachable. Green:
// a permissioned browser human approves, the gated tool executes EXACTLY once
// (second approve 404, effect counter frozen), a task token never approves,
// and the pending worklist is human-only and tenant-scoped.
func TestHumanBrowserApprove(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// A dedicated service+face with the browser user audience wired, mirroring
	// what tryMountTools composes when an Identity permission store is present.
	guardian := tools.NewPolicyGuardian("test-users")
	svc2, err := tools.New(ctx, e.pool, "ahp_tools_u"+randomHex(t, 6), tools.Config{
		Guardian:     guardian,
		Credentials:  fakeCreds{bearer: "cred-" + randomHex(t, 4)},
		Connections:  map[string]tools.Connector{"c": e.fixture.connector()},
		Tools:        map[string]tools.ToolSpec{"gated": {Name: "gated", Connection: "c", RequiresApproval: true}},
		Verifier:     e.verifier,
		Audience:     tAudience,
		UserAudience: "ahp-browser",
	})
	if err != nil {
		t.Fatalf("tools.New(users): %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = svc2.Drop(dctx)
	})

	const approver = tSubject
	perms := map[string]bool{}
	humanAuthorize := func(_ context.Context, claims auth.Claims, capability string) error {
		if capability != ApproveCapability {
			return contracts.NewError(contracts.ErrForbidden, "unexpected capability "+capability)
		}
		if claims.Kind != auth.KindHuman {
			return contracts.NewError(contracts.ErrForbidden, "approvals plane is human-only")
		}
		if !perms[claims.Subject] {
			return contracts.NewError(contracts.ErrForbidden, "approver lacks "+ApproveCapability)
		}
		return nil
	}
	taskAuthorize := func(context.Context, auth.Claims, string) error { return nil }
	h2, err := NewHandlerWithUsers(svc2, e.verifier, taskAuthorize, "ahp-browser", humanAuthorize)
	if err != nil {
		t.Fatalf("NewHandlerWithUsers: %v", err)
	}
	e.srv.Config.Handler = h2

	// A human browser token (aud ahp-browser) for the env tenant.
	humanTok, err := e.signer.Sign(auth.Claims{
		Audience: "ahp-browser", Kind: auth.KindHuman,
		Tenant: e.tenant, Subject: approver,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	taskTok := e.signTask(t)
	args := `{"action":"send-users"}`

	// Gated call: 409 registers the pending request.
	status, body := e.call(t, "Call", taskTok, `{"key":"k-u-1","tool":"gated","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusConflict, body)
	requestID, digest, err := svc2.CreateApprovalRequest(ctx,
		contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}, "k-u-2", "gated", json.RawMessage(args))
	if err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}

	// (1) Task bearer can NEVER approve, even though TaskOrUser verifies it.
	if s, _ := e.call(t, "Approve", taskTok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`); s == http.StatusOK {
		t.Fatal("task token approved a human approval")
	}
	// (2) Browser human WITHOUT the tools:approve permission is refused.
	if s, _ := e.call(t, "Approve", humanTok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`); s == http.StatusOK {
		t.Fatal("human without permission approved")
	}
	// (3) GREEN line: the same browser token WITH the permission approves 200.
	perms[approver] = true
	status, body = e.call(t, "Approve", humanTok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`)
	mustStatus(t, status, http.StatusOK, body)
	if body["approved_by"] != approver {
		t.Fatalf("approved_by=%v, want %q", body["approved_by"], approver)
	}

	// (4) Exactly-once: gated call SUCCEEDS once; a second Approve cannot
	// re-bind (404) and cannot produce a second effect.
	status, body = e.call(t, "Call", taskTok, `{"key":"k-u-2","tool":"gated","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusOK, body)
	if body["status"] != operations.StatusSucceeded {
		t.Fatalf("gated call after human approve = %v, want SUCCEEDED", body)
	}
	effAfterFirst := e.fixture.effCount
	if s, _ := e.call(t, "Approve", humanTok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`); s != http.StatusNotFound {
		t.Fatalf("second Approve status=%d, want 404 (already bound)", s)
	}
	if s, _ := e.call(t, "Call", taskTok, `{"key":"k-u-2","tool":"gated","arguments":`+args+`}`); s != http.StatusOK {
		t.Fatalf("replay same key status=%d, want 200 terminal replay", s)
	}
	if e.fixture.effCount != effAfterFirst {
		t.Fatalf("repeat approve/call produced a second effect: %d -> %d", effAfterFirst, e.fixture.effCount)
	}

	// (5) Approvals worklist: human sees the UNBOUND digest, not the bound one;
	// a task token is refused.
	_, digest2, err := svc2.CreateApprovalRequest(ctx,
		contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}, "", "gated", json.RawMessage(`{"action":"send-pending"}`))
	if err != nil {
		t.Fatal(err)
	}
	s, listBody := e.call(t, "Approvals", humanTok, `{}`)
	mustStatus(t, s, http.StatusOK, listBody)
	items, _ := listBody["approvals"].([]any)
	var sawPending, sawBound bool
	for _, raw := range items {
		it, _ := raw.(map[string]any)
		switch it["digest"] {
		case digest2:
			sawPending = true
		case digest:
			sawBound = it["request_id"] == requestID
		}
	}
	if !sawPending || sawBound {
		t.Fatalf("pending list wrong: pending=%v bound=%v items=%d", sawPending, sawBound, len(items))
	}
	if s, _ := e.call(t, "Approvals", taskTok, `{}`); s == http.StatusOK {
		t.Fatal("task token read the human approvals worklist")
	}
}
