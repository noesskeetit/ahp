package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestBrowserScopeUsesVerifiedExchangeBeforeColdTurnOwnership(t *testing.T) {
	h := newParentHarness(t)
	for _, tc := range []struct {
		name, subject, tenant, deployment string
		missing                           bool
		want                              int
	}{
		{name: "owner", want: 204},
		{name: "different_subject", subject: "mallory", want: 403},
		{name: "different_tenant", tenant: "other", want: 403},
		{name: "foreign_workload", deployment: "other", want: 403},
		{name: "missing_exchange", missing: true, want: 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := transport.Principal{Claims: auth.Claims{Kind: auth.KindHuman, Tenant: h.real.Tenant, Subject: h.real.Subject}, Scope: contracts.Scope{Tenant: h.real.Tenant, Subject: h.real.Subject}}
			ctx := transport.WithPrincipal(t.Context(), p)
			if !tc.missing {
				ctx = space.WithScopedTask(ctx, h.runToken(t, h.grantTask, auth.Claims{Subject: tc.subject, Tenant: tc.tenant, Deployment: tc.deployment}))
			}
			req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			rec := httptest.NewRecorder()
			h.att.bindHumanScope(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ := transport.FromContext(r.Context())
				if got.Scope != h.real || got.Claims.Kind != auth.KindHuman {
					t.Errorf("bound principal=%+v", got)
				}
				w.WriteHeader(204)
			})).ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status=%d want=%d", rec.Code, tc.want)
			}
		})
	}
}

type capturedCoreRun struct {
	request *space.CoreRequest
	calls   int
}

func (c *capturedCoreRun) Run(_ context.Context, req *space.CoreRequest, _ chan<- space.StepResult) error {
	c.request = req
	c.calls++
	return nil
}

func TestCoreReceivesAttestedScopeWithoutChangingCoordinatorRequest(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("scoped-core", "k", key)
	own := contracts.Scope{Tenant: "tenant", Subject: "alice", Deployment: "space"}
	token, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: own.Tenant, Subject: own.Subject, Deployment: own.Deployment, Task: "identity-task", Root: "root", Context: "context", Grant: "grant"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	att := runScopeAttester{verifier: auth.NewVerifier("scoped-core", map[string]ed25519.PublicKey{"k": pub}), taskAudience: "ahp", workload: auth.WorkloadID(own)}
	inner := &capturedCoreRun{}
	runner := scopedCoreRunner{runner: inner, authority: att}
	req := &space.CoreRequest{RunID: "different-state-run", ContextID: "context", TaskToken: token, Scope: contracts.Scope{Tenant: own.Tenant, Subject: own.Subject}}
	if err := runner.Run(context.Background(), req, nil); err != nil {
		t.Fatal(err)
	}
	if inner.request.Scope != own || inner.request.RunID != req.RunID {
		t.Fatal("Core did not receive attested deployment and original State run")
	}
	if req.Scope.Deployment != "" || inner.request == req {
		t.Fatal("shared coordinator request mutated")
	}
	for _, mutate := range []func(*space.CoreRequest){func(r *space.CoreRequest) { r.Scope.Subject = "other" }, func(r *space.CoreRequest) { r.Scope.Tenant = "other" }, func(r *space.CoreRequest) { r.Scope.Deployment = "other" }, func(r *space.CoreRequest) { r.ContextID = "other" }, func(r *space.CoreRequest) { r.TaskToken += "tampered" }} {
		bad := *req
		mutate(&bad)
		if err := runner.Run(context.Background(), &bad, nil); err == nil {
			t.Fatal("unattested Core authority accepted")
		}
	}
	if inner.calls != 1 {
		t.Fatal("denied request entered Core")
	}
}

// TestRunGrantClaimMustBeTheConversationNotTheThread is the consumer half of DEF-DOTE2E-1.
//
// The guard is not the defect - it is the thing that caught it. A Dot turn presents the
// conversation it runs in (threads.context_id); the grant Space minted carried the thread
// row's id, and this comparison refused it, so every browser turn died before the model.
// Pinning both directions here means the fix cannot be made by loosening the guard: the
// thread id is refused precisely because it is not a conversation, and a request that names
// no context is admitted only to be bound to the claim the credential actually carries.
func TestRunGrantClaimMustBeTheConversationNotTheThread(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("scoped-core", "k", key)
	own := contracts.Scope{Tenant: "tenant", Subject: "alice", Deployment: "space"}
	att := runScopeAttester{verifier: auth.NewVerifier("scoped-core", map[string]ed25519.PublicKey{"k": pub}), taskAudience: "ahp", workload: auth.WorkloadID(own)}
	sign := func(contextClaim string) string {
		t.Helper()
		tok, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: own.Tenant, Subject: own.Subject,
			Deployment: own.Deployment, Task: "rtask-alice:k1", Root: "root", Context: contextClaim, Grant: "grant"}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}

	// What Space mints today: the claim is the conversation, the request names it, Core runs.
	granted := &capturedCoreRun{}
	if err := (scopedCoreRunner{runner: granted, authority: att}).Run(context.Background(),
		&space.CoreRequest{ThreadID: "thr-1", ContextID: "c1", RunID: "run-1", TaskToken: sign("c1"),
			Scope: contracts.Scope{Tenant: own.Tenant, Subject: own.Subject}}, nil); err != nil {
		t.Fatalf("a run grant claiming its conversation was refused: %v", err)
	}
	if granted.request.ContextID != "c1" || granted.request.ThreadID != "thr-1" {
		t.Fatalf("Core received context %q for thread %q", granted.request.ContextID, granted.request.ThreadID)
	}

	// What Space minted before the fix: same turn, claim bound to the thread row. Refused,
	// because a thread is not the conversation whose artifacts and memory this run writes to.
	before := &capturedCoreRun{}
	err := (scopedCoreRunner{runner: before, authority: att}).Run(context.Background(),
		&space.CoreRequest{ThreadID: "thr-1", ContextID: "c1", RunID: "run-1", TaskToken: sign("thr-1"),
			Scope: contracts.Scope{Tenant: own.Tenant, Subject: own.Subject}}, nil)
	if err == nil {
		t.Fatal("a run grant claiming the THREAD id was admitted; DEF-DOTE2E-1 would be back")
	}
	if before.calls != 0 {
		t.Fatal("the refused turn reached Core")
	}
}
