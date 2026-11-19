package identity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestCancelSubtreeAuthorizationBoundary proves, against real PostgreSQL, that
// cancellation cannot be aimed at another subject and does not escalate a
// generic workload into an administrator:
//
//   - a workload cancels exactly its OWN subtree through the lineage route,
//     where the scope and task come from the verified task token, not a body;
//   - aiming that token at a foreign subject's tree is refused;
//   - cancelling a child revokes its whole descendant subtree but leaves the
//     parent and the child's siblings untouched;
//   - an ordinary workload cert may NOT use the operator control/cancel-subtree
//     route; only a ControlPlaneDeployments deployment may, and only within its
//     own certificate tenant.
func TestCancelSubtreeAuthorizationBoundary(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)

	scopeA := uniqueScope(t)
	h.createRootStore(t, scopeA, "a-root", true, nil)
	// PDF rev3.2 (стр.30/64) нормативен: «актор-цепочка: повтор деплоя — отказ»,
	// а разрешение на self-call (allow_self) наследуется только как потолок.
	// Дерево одной и той же задачи строится в одном деплое, значит каждый УЗЕЛ,
	// у которого сам есть ребёнок в том же деплое, обязан нести это разрешение,
	// иначе второй уровень делегирования отказан по правилу повторения актора.
	// a-sibling deliberately stays WITHOUT the allowance: it is a leaf, and the
	// negatives below pin that a leaf cannot grow one.
	for _, c := range []struct {
		parent, task string
		allowSelf    bool
	}{
		{"a-root", "a-child", true}, {"a-root", "a-sibling", false}, {"a-child", "a-grandchild", false},
	} {
		if _, err := h.store.Delegate(ctx, scopeA, c.parent, identity.DelegateRequest{
			Task: c.task, Context: ctxID(scopeA, c.task), Deployment: scopeA.Deployment,
			Capabilities: fullCaps, TTL: time.Hour, AllowSelf: c.allowSelf,
		}); err != nil {
			t.Fatalf("delegate %s: %v", c.task, err)
		}
	}

	// Negative 1 (the rule, not the fixture): a level that was not given the
	// self-call allowance cannot delegate deeper inside the same deployment.
	// Without this assertion the actor-chain rule could be "fixed" by allowing
	// everything, which is exactly what the PDF forbids.
	if _, err := h.store.Delegate(ctx, scopeA, "a-sibling", identity.DelegateRequest{
		Task: "a-sibling-child", Context: ctxID(scopeA, "a-sibling-child"), Deployment: scopeA.Deployment,
		Capabilities: fullCaps, TTL: time.Hour,
	}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("a level without the self-call allowance must refuse a deeper child: %v", err)
	}
	// Negative 2: the allowance itself may only narrow. A child may not hand
	// itself the right to repeat the actor when its parent never had it.
	if _, err := h.store.Delegate(ctx, scopeA, "a-sibling", identity.DelegateRequest{
		Task: "a-sibling-self", Context: ctxID(scopeA, "a-sibling-self"), Deployment: scopeA.Deployment,
		Capabilities: fullCaps, TTL: time.Hour, AllowSelf: true,
	}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("a child may not widen allow_self: %v", err)
	}
	// Both refusals must not have persisted anything.
	for _, task := range []string{"a-sibling-child", "a-sibling-self"} {
		if _, err := h.store.Get(ctx, scopeA, task); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("refused delegation %s must not persist a grant: err=%v", task, err)
		}
	}

	// A different subject (and tenant) whose tree must be unreachable to A.
	scopeB := uniqueScope(t)
	h.createRootStore(t, scopeB, "b-root", true, nil)

	clientA := h.taskClient(t, scopeA)
	clientB := h.taskClient(t, scopeB)
	tokChild := h.mint(t, scopeA, "a-child")
	tokGrand := h.mint(t, scopeA, "a-grandchild")

	// Precondition: the grandchild is live.
	if s := h.do(t, clientA, http.MethodGet, "/v1/identity/roots/a-grandchild", tokGrand, nil).status; s != http.StatusOK {
		t.Fatalf("grandchild should be live before cancel: %d", s)
	}

	// Attack 1: use A's token to cancel B's tree through the lineage route. The
	// path names a task A does not hold, so the token cannot authorise it.
	if r := h.do(t, clientA, http.MethodPost, "/v1/identity/roots/b-root/cancel", tokChild, nil); r.status != http.StatusForbidden {
		t.Fatalf("foreign-subject lineage cancel must be denied: %d %s", r.status, r.body)
	}
	// Attack 2: an ordinary workload reaches for the operator control route.
	if r := h.do(t, clientA, http.MethodPost, "/v1/identity/control/cancel-subtree", "",
		map[string]string{"tenant": scopeB.Tenant, "deployment": scopeB.Deployment,
			"subject": scopeB.Subject, "task": "b-root"}); r.status != http.StatusForbidden {
		t.Fatalf("ordinary workload must not reach operator cancel: %d %s", r.status, r.body)
	}

	// B is untouched by both failed attacks.
	tokB := h.mint(t, scopeB, "b-root")
	if s := h.do(t, clientB, http.MethodGet, "/v1/identity/roots/b-root", tokB, nil).status; s != http.StatusOK {
		t.Fatalf("foreign tree must be untouched: %d", s)
	}

	// Legitimate: A cancels its own child subtree via the lineage route.
	if r := h.do(t, clientA, http.MethodPost, "/v1/identity/roots/a-child/cancel", tokChild, nil); r.status != http.StatusNoContent {
		t.Fatalf("own lineage cancel: %d %s", r.status, r.body)
	}

	// Child and its descendant are revoked; parent and sibling are intact.
	if s := h.do(t, clientA, http.MethodGet, "/v1/identity/roots/a-grandchild", tokGrand, nil).status; s != http.StatusForbidden {
		t.Fatalf("descendant of cancelled child must be denied: %d", s)
	}
	if g, err := h.store.Get(ctx, scopeA, "a-child"); err != nil || g.Status != identity.StatusRevoked {
		t.Fatalf("cancelled child must be revoked: status=%v err=%v", g.Status, err)
	}
	tokSib := h.mint(t, scopeA, "a-sibling")
	if s := h.do(t, clientA, http.MethodGet, "/v1/identity/roots/a-sibling", tokSib, nil).status; s != http.StatusOK {
		t.Fatalf("sibling of cancelled child must survive: %d", s)
	}
	tokRootA := h.mint(t, scopeA, "a-root")
	if s := h.do(t, clientA, http.MethodGet, "/v1/identity/roots/a-root", tokRootA, nil).status; s != http.StatusOK {
		t.Fatalf("parent of cancelled child must survive: %d", s)
	}

	// The operator kill switch is legitimate but tenant-bound and allowlisted:
	// a ControlPlane deployment may cancel B's tree inside its own tenant.
	op := h.controlClient(t, scopeB.Tenant)
	if r := h.do(t, op, http.MethodPost, "/v1/identity/control/cancel-subtree", "",
		map[string]string{"tenant": scopeB.Tenant, "deployment": scopeB.Deployment,
			"subject": scopeB.Subject, "task": "b-root"}); r.status != http.StatusNoContent {
		t.Fatalf("operator cancel within tenant: %d %s", r.status, r.body)
	}
	if s := h.do(t, clientB, http.MethodGet, "/v1/identity/roots/b-root", tokB, nil).status; s != http.StatusForbidden {
		t.Fatalf("operator-cancelled tree must be denied: %d", s)
	}

	// A control-plane operator is still bounded to its certificate tenant: it
	// may not cancel a subtree belonging to another tenant.
	other := h.controlClient(t, "some-other-tenant")
	if r := h.do(t, other, http.MethodPost, "/v1/identity/control/cancel-subtree", "",
		map[string]string{"tenant": scopeA.Tenant, "deployment": scopeA.Deployment,
			"subject": scopeA.Subject, "task": "a-root"}); r.status != http.StatusForbidden {
		t.Fatalf("operator must not cancel across tenants: %d %s", r.status, r.body)
	}
}
