package artifacts

// C3 legs (reviewer condition, review reports/mac-director/artifacts-read-parity-27b16124-review-20261110.md):
// the two lifecycle READ verbs must apply the conversation read gate identically.
//
// Readiness called Store.checkReadScope; GetLifecycle did not, so a credential
// bound to one conversation could read the deletion/retention/hold state of an
// artifact belonging to another conversation of its own subject and tenant. The
// gate is a READ gate (read_scope.go: "it runs inside every read path"), so the
// asymmetry is a defect, not a design: both reads are bounded, and only reads -
// the lifecycle ACTIONS (retain, hold, purge, derive) stay subject-level, exactly
// like Delete, which is also not conversation-bound.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestGetLifecycleIsConversationScopedLikeReadiness(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-lc-home"}, "k-lc-scope", "scoped-lifecycle-body")

	other := WithCallerContext(ctx, "ctx-lc-visitor")
	_, lcErr := e.store.GetLifecycle(other, e.scope, ref.ID)
	_, rdErr := e.store.Readiness(other, e.scope, ref.ID)
	if lcErr == nil {
		t.Fatal("GetLifecycle let a conversation-bound credential read another conversation's artifact")
	}
	codeIs(t, lcErr, contracts.ErrForbidden)
	codeIs(t, rdErr, contracts.ErrForbidden)
	// One gate, one answer: the two read verbs must not be distinguishable by code.
	if contracts.Code(lcErr) != contracts.Code(rdErr) {
		t.Fatalf("lifecycle read verbs disagree on the scope refusal: %q vs %q",
			contracts.Code(lcErr), contracts.Code(rdErr))
	}
	// The refusal must not be masked into NOT_FOUND: the caller owns this artifact,
	// only its conversation claim differs, and read parity masks LABEL refusals.
	if lcErr.Error() == "NOT_FOUND: artifact not found" {
		t.Fatal("scope refusal must stay FORBIDDEN, only label refusals are masked")
	}

	// The widening the owner itself granted hydrates BOTH read verbs, the same way
	// it hydrates the content read.
	visitor := "ctx-lc-visitor"
	if err := e.store.GrantContext(WithCallerContext(ctx, "ctx-lc-home"), e.scope, ref.ID, visitor, "op-lc-scope"); err != nil {
		t.Fatalf("GrantContext: %v", err)
	}
	if _, err := e.store.GetLifecycle(WithCallerContext(ctx, visitor), e.scope, ref.ID); err != nil {
		t.Fatalf("granted conversation must read lifecycle: %v", err)
	}
	if _, err := e.store.Readiness(WithCallerContext(ctx, visitor), e.scope, ref.ID); err != nil {
		t.Fatalf("granted conversation must read readiness: %v", err)
	}
}

func TestLifecycleScopeGateLeavesOwnerAndActionsAlone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-lc-owner"}, "k-lc-scope-2", "owner-side-body")

	// The owning conversation reads both verbs.
	home := WithCallerContext(ctx, "ctx-lc-owner")
	if _, err := e.store.GetLifecycle(home, e.scope, ref.ID); err != nil {
		t.Fatalf("owning conversation must read lifecycle: %v", err)
	}
	if _, err := e.store.Readiness(home, e.scope, ref.ID); err != nil {
		t.Fatalf("owning conversation must read readiness: %v", err)
	}
	// An unbound credential (no context claim) is not conversation-scoped at all.
	if _, err := e.store.GetLifecycle(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("unbound credential must read lifecycle: %v", err)
	}
	// Lifecycle actions are subject-level, like Delete: the conversation claim does
	// not disarm the owner's own retention/hold authority.
	visitor := WithCallerContext(ctx, "ctx-lc-visitor-2")
	if err := e.store.SetLegalHold(visitor, e.scope, ref.ID, true); err != nil {
		t.Fatalf("a lifecycle action must stay subject-level, not conversation-bound: %v", err)
	}
	if err := e.store.SetLegalHold(visitor, e.scope, ref.ID, false); err != nil {
		t.Fatalf("release hold: %v", err)
	}
}
