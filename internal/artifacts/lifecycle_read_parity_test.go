package artifacts

// RED legs for the read-parity gap on the lifecycle face.
//
// The plane's own contract for reads is stated on Store.get/Store.load (B114 read
// parity): a subject the artifact's labels exclude must get exactly the answer a
// missing id gets, so existence cannot be probed across subjects inside one
// tenant. Store.load honours that; loadLifecycle returns the admission refusal
// as FORBIDDEN even for the two READ verbs (GetLifecycle, Readiness), which makes
// "someone else's artifact with this id exists" a distinguishable answer.
//
// These legs pin the contract, not the current answer.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestLifecycleReadIsIndistinguishableFromMissing(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-lc-parity", "private-lifecycle-payload")
	e.foreign.Tenant = e.scope.Tenant // same tenant, different subject

	absent := "art-lc-parity-absent"
	_, missingLCErr := e.store.GetLifecycle(ctx, e.foreign, absent)
	_, ownLCErr := e.store.GetLifecycle(ctx, e.foreign, ref.ID)
	codeIs(t, missingLCErr, contracts.ErrNotFound)
	codeIs(t, ownLCErr, contracts.ErrNotFound)
	if missingLCErr.Error() != ownLCErr.Error() {
		t.Fatalf("lifecycle read must answer byte-for-byte like a missing id: missing=%q foreign=%q",
			missingLCErr.Error(), ownLCErr.Error())
	}

	_, missingRDErr := e.store.Readiness(ctx, e.foreign, absent)
	_, ownRDErr := e.store.Readiness(ctx, e.foreign, ref.ID)
	codeIs(t, missingRDErr, contracts.ErrNotFound)
	codeIs(t, ownRDErr, contracts.ErrNotFound)
	if missingRDErr.Error() != ownRDErr.Error() {
		t.Fatalf("readiness read must answer byte-for-byte like a missing id: missing=%q foreign=%q",
			missingRDErr.Error(), ownRDErr.Error())
	}
}

func TestLifecycleReadParityKeepsActionsAndPolicyRefusals(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-lc-parity-2", "private-lifecycle-payload-2")
	e.foreign.Tenant = e.scope.Tenant

	// Positive control: the owner still reads its own lifecycle and readiness.
	if _, err := e.store.GetLifecycle(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("owner must read its own lifecycle: %v", err)
	}
	if _, err := e.store.Readiness(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("owner must read its own readiness: %v", err)
	}
	// A masking fix must not swallow a policy refusal on a READ: a revoked token
	// still answers FORBIDDEN, not NOT_FOUND.
	e.allow.Store(false)
	if _, err := e.store.GetLifecycle(ctx, e.scope, ref.ID); err == nil {
		t.Fatal("a revoked authorization must still refuse a lifecycle read")
	} else {
		codeIs(t, err, contracts.ErrForbidden)
	}
	e.allow.Store(true)
	// And actions of a foreign subject stay FORBIDDEN: read parity covers reads only.
	codeIs(t, e.store.SetLegalHold(ctx, e.foreign, ref.ID, true), contracts.ErrForbidden)
	codeIs(t, e.store.SetRetention(ctx, e.foreign, ref.ID, nil), contracts.ErrForbidden)
}
