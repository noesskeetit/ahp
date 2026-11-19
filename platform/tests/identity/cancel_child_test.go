package identity_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
)

func TestParentCancelsOnlyDescendantAndCanRetry(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "root", true, nil)
	for _, pair := range [][2]string{{"root", "parent"}, {"root", "sibling"}, {"parent", "child"}, {"child", "grandchild"}} {
		if _, err := h.store.Delegate(ctx, scope, pair[0], identity.DelegateRequest{Task: pair[1], Context: ctxID(scope, pair[1]), Deployment: scope.Deployment, Capabilities: fullCaps, TTL: time.Hour, AllowSelf: true}); err != nil {
			t.Fatal(err)
		}
	}
	client := h.taskClient(t, scope)
	token := h.mint(t, scope, "parent")
	for _, target := range []string{"root", "parent", "sibling"} {
		r := h.do(t, client, http.MethodPost, "/v1/identity/roots/parent/children/"+target+"/cancel", token, nil)
		if r.status != http.StatusForbidden {
			t.Fatalf("target %s: %d %s", target, r.status, r.body)
		}
	}
	other := scope
	other.Subject = "foreign"
	h.createRootStore(t, other, "foreign-root", true, nil)
	if r := h.do(t, client, http.MethodPost, "/v1/identity/roots/parent/children/foreign-root/cancel", token, nil); r.status != http.StatusForbidden {
		t.Fatalf("foreign subject: %d %s", r.status, r.body)
	}
	if r := h.do(t, client, http.MethodPost, "/v1/identity/roots/root/children/child/cancel", token, nil); r.status != http.StatusForbidden {
		t.Fatalf("forged parent: %d %s", r.status, r.body)
	}
	for i := 0; i < 2; i++ {
		r := h.do(t, client, http.MethodPost, "/v1/identity/roots/parent/children/child/cancel", token, nil)
		if r.status != http.StatusNoContent {
			t.Fatalf("cancel attempt %d: %d %s", i, r.status, r.body)
		}
	}
	for _, task := range []string{"child", "grandchild"} {
		g, err := h.store.Get(ctx, scope, task)
		if err != nil || g.Status != identity.StatusRevoked {
			t.Fatalf("%s not revoked: %+v %v", task, g, err)
		}
	}
	for _, task := range []string{"root", "parent", "sibling"} {
		g, err := h.store.Get(ctx, scope, task)
		if err != nil || g.Status != identity.StatusActive {
			t.Fatalf("%s changed: %+v %v", task, g, err)
		}
	}
}
