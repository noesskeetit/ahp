package artifacts

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// An artifact-scoped pin names ONE artifact, so the caller must own it. A
// reference-level pin (context/thread/task/run) names a reference the caller's
// own tenant will be swept against, so it needs no artifact to exist yet -- that
// asymmetry is deliberate and is asserted here too, so the fix cannot be applied
// one level too broadly and break pinning before seal.
//
// Without the ownership check the durable pin registry accepts another tenant's
// artifact id: the row lands under the writer's tenant, so it neither protects
// nor consumes the victim's bytes, but the registry and its audit trail then
// assert something false about an object the writer has no rights to. Pin and
// unpin are exactly the controls the retention story is audited through, so a
// false row there is a defect, not a cosmetic one.
func TestPinArtifactRefusesAnotherTenantsArtifact(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	body := "pin-scope-payload-with-enough-length-for-seal"
	ref, _ := e.sealBoundWithKey(t, Binding{ContextID: "ctx-pin-scope", TaskID: "task-pin-scope"},
		"k-pin-scope", body)

	other := contracts.Scope{Tenant: "t-pinscope-other", Deployment: "d1", Subject: "alice"}
	err := e.store.PinArtifact(ctx, other, ref.ID, PinKindTask, "ref-pin-scope")
	if code := contracts.Code(err); code != contracts.ErrNotFound {
		t.Fatalf("cross-tenant PinArtifact: want NOT_FOUND (the same opaque answer an unknown id gets), got %v", err)
	}
	// Out of the store's own answer: no row was written for the foreign tenant.
	if n := pinRows(t, ctx, other.Tenant, ref.ID); n != 0 {
		t.Fatalf("a refused pin must not exist in the durable registry: rows=%d", n)
	}
	// The reference-level form keeps its semantics: a pin may be taken before any
	// artifact exists, and it is still bounded to the writer's own tenant.
	if err := e.store.PinRef(ctx, other, PinKindContext, "ctx-pin-scope"); err != nil {
		t.Fatalf("reference-level pin must stay unconditional: %v", err)
	}
	if err := e.store.UnpinRef(ctx, other, PinKindContext, "ctx-pin-scope"); err != nil {
		t.Fatalf("reference-level unpin: %v", err)
	}
	// The owner pins their own artifact, and the row is durable.
	if err := e.store.PinArtifact(ctx, e.scope, ref.ID, PinKindTask, "ref-pin-scope"); err != nil {
		t.Fatalf("owner PinArtifact: %v", err)
	}
	if n := pinRows(t, ctx, e.scope.Tenant, ref.ID); n != 1 {
		t.Fatalf("owner pin must be a durable row: rows=%d", n)
	}
	// An id that exists nowhere is refused the same way as another tenant's id:
	// no existence oracle on this face either.
	if code := contracts.Code(e.store.PinArtifact(ctx, e.scope, "no-such-artifact-id", PinKindTask, "ref-pin-scope-2")); code != contracts.ErrNotFound {
		t.Fatalf("unknown id PinArtifact: want NOT_FOUND, got %v", code)
	}
	// Unpinning must never require the artifact row to still exist, or a pin whose
	// artifact was already purged could never be withdrawn.
	if err := e.store.UnpinArtifact(ctx, e.scope, "no-such-artifact-id", PinKindTask, "ref-pin-scope"); err != nil {
		t.Fatalf("UnpinArtifact of a name with no row must be a no-op: %v", err)
	}
}

// pinRows counts pin rows directly in the run database: the registry is a table,
// and that is where the claim is checked.
func pinRows(t *testing.T, ctx context.Context, tenant, artifactID string) int {
	t.Helper()
	// The same run pool the fixture writes through: the claim is about the row,
	// and reading a different connection string could make it vacuously true.
	pool := isolatedPool(t)
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_artifacts.gc_pins WHERE tenant = $1 AND artifact_id = $2`,
		tenant, artifactID).Scan(&n); err != nil {
		t.Fatalf("read gc_pins: %v", err)
	}
	return n
}
