package artifacts

// C1' (kept open in review reports/mac-director/artifacts-read-parity-c1-f4311971-review-20261110.md).
//
// The read-parity mask and the conversation gate are order-sensitive, and the order was
// pinned only on the content face (read_gate_order_parity_test.go). Reviewer mutation M-C
// runs checkReadScope BEFORE label admission on the lifecycle face: the observable answer
// changes -- a same-tenant subject that the artifact's labels already exclude receives the
// unmasked FORBIDDEN instead of the missing-id answer, and learns that the id exists --
// while no leg in the repository reddens. These legs close that gap for both lifecycle
// read verbs, and keep the fence that the order is supposed to preserve: an owner the
// labels DO admit is still bounded by its conversation.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestLifecycleReadMasksForeignSubjectBeforeTheConversationGate(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-mg-home"}, "k-mg-1", "mask-before-gate-body")
	e.foreign.Tenant = e.scope.Tenant
	visitor := WithCallerContext(ctx, "ctx-mg-visitor")

	for _, verb := range []string{"GetLifecycle", "Readiness"} {
		call := func(id string) error {
			if verb == "GetLifecycle" {
				_, err := e.store.GetLifecycle(visitor, e.foreign, id)
				return err
			}
			_, err := e.store.Readiness(visitor, e.foreign, id)
			return err
		}
		missingErr := call("art-mg-absent")
		foreignErr := call(ref.ID)
		codeIs(t, missingErr, contracts.ErrNotFound)
		if foreignErr == nil {
			t.Fatalf("%s: a same-tenant subject read an artifact its labels exclude", verb)
		}
		if got := contracts.Code(foreignErr); got != contracts.ErrNotFound {
			t.Fatalf("%s: the label mask must decide before the conversation gate - got %q (%v) where a "+
				"missing id answers %q", verb, got, foreignErr, contracts.Code(missingErr))
		}
		if foreignErr.Error() != missingErr.Error() {
			t.Fatalf("%s: the label mask must decide before the conversation gate - answers differ: %q vs %q",
				verb, foreignErr.Error(), missingErr.Error())
		}
	}
}

func TestLifecycleReadKeepsTheConversationFenceForAnAdmittedOwner(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-mg-owner"}, "k-mg-2", "fence-body")

	// The mask must not swallow the fence: this caller owns the artifact and its
	// labels admit it, so only the conversation claim differs and the refusal stays
	// visible as FORBIDDEN on both read verbs.
	for _, verb := range []string{"GetLifecycle", "Readiness"} {
		var err error
		if verb == "GetLifecycle" {
			_, err = e.store.GetLifecycle(WithCallerContext(ctx, "ctx-mg-visitor"), e.scope, ref.ID)
		} else {
			_, err = e.store.Readiness(WithCallerContext(ctx, "ctx-mg-visitor"), e.scope, ref.ID)
		}
		codeIs(t, err, contracts.ErrForbidden)
	}
	// And the owning conversation reads both.
	home := WithCallerContext(ctx, "ctx-mg-owner")
	if _, err := e.store.GetLifecycle(home, e.scope, ref.ID); err != nil {
		t.Fatalf("owning conversation must read lifecycle: %v", err)
	}
	if _, err := e.store.Readiness(home, e.scope, ref.ID); err != nil {
		t.Fatalf("owning conversation must read readiness: %v", err)
	}
}
