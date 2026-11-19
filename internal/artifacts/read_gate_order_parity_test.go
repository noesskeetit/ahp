package artifacts

// C3b legs. Measured on 34aed0fe (logs/c3b-probe.log in
// private/pdf-closure-20261102/gauss-h10-7-artifacts-acl-20261110T0500Z/):
//
//	foreign subject + a conversation claim that is not the artifact's -> FORBIDDEN
//	foreign subject + no claim                                        -> NOT_FOUND
//	foreign subject + the artifact's own claim                        -> NOT_FOUND
//	missing id + a claim                                              -> NOT_FOUND
//
// So the conversation gate, which Store.load ran BEFORE label admission, answered
// for a caller the artifact's labels already exclude, and its unmasked FORBIDDEN
// told that caller the id exists. Same defect class as the lifecycle read parity
// just merged. The task-token boundary itself must not move: an OWNER whose labels
// admit it still gets FORBIDDEN across conversations.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestForeignSubjectWithForeignClaimIsMaskedLikeMissing(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-rg-home"}, "k-rg-1", "read-gate-order-body")
	e.foreign.Tenant = e.scope.Tenant

	visitor := WithCallerContext(ctx, "ctx-rg-visitor")
	_, foreignErr := e.store.Get(visitor, e.foreign, ref.ID)
	_, missingErr := e.store.Get(visitor, e.foreign, "art-rg-absent")
	codeIs(t, missingErr, contracts.ErrNotFound)
	codeIs(t, foreignErr, contracts.ErrNotFound)
	if foreignErr.Error() != missingErr.Error() {
		t.Fatalf("a foreign subject carrying another conversation's claim must get the missing-id answer byte for byte: foreign=%q missing=%q",
			foreignErr.Error(), missingErr.Error())
	}
	// OpenRange goes through the same gate and must not answer differently.
	_, _, rangeErr := e.store.OpenRange(visitor, e.foreign, ref.ID, 0, 4)
	codeIs(t, rangeErr, contracts.ErrNotFound)
	if rangeErr.Error() != missingErr.Error() {
		t.Fatalf("range read differs from the missing-id answer: %q vs %q", rangeErr.Error(), missingErr.Error())
	}
}

func TestConversationGateStillBoundsTheOwnerItOwns(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-rg-owner"}, "k-rg-2", "task-token-boundary-body")

	// The owner is admitted by its labels, so the conversation boundary applies
	// and stays visible as FORBIDDEN: this is the task-token fence, not a secret.
	_, err := e.store.Get(WithCallerContext(ctx, "ctx-rg-visitor"), e.scope, ref.ID)
	codeIs(t, err, contracts.ErrForbidden)
	if _, err := e.store.Get(WithCallerContext(ctx, "ctx-rg-owner"), e.scope, ref.ID); err != nil {
		t.Fatalf("the owning conversation must read its own artifact: %v", err)
	}
	// And the widening the owner granted still hydrates the read.
	if err := e.store.GrantContext(WithCallerContext(ctx, "ctx-rg-owner"), e.scope, ref.ID, "ctx-rg-visitor", "op-rg"); err != nil {
		t.Fatalf("GrantContext: %v", err)
	}
	if _, err := e.store.Get(WithCallerContext(ctx, "ctx-rg-visitor"), e.scope, ref.ID); err != nil {
		t.Fatalf("a granted conversation must read: %v", err)
	}
}
