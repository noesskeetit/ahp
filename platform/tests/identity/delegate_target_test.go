package identity_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestDelegateToTargetRetainsLineage(t *testing.T) {
	h := newHarness(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	root := h.createRootStore(t, scope, "root", false, nil)
	if _, err := h.store.AddLabels(ctx, scope, root.Task, labels.Set{Workspaces: []string{"private-parent"}}); err != nil {
		t.Fatal(err)
	}
	target := scope
	target.Deployment = "custom-worker"
	policy := identity.DelegationTarget{Tenant: target.Tenant, Deployment: target.Deployment, Capabilities: fullCaps}
	req := identity.DelegateRequest{Task: "child", Context: ctxID(scope, "target-child"), Deployment: target.Deployment, Capabilities: fullCaps, TTL: time.Hour}
	// Agent-facing delegation keeps its original same-deployment restriction.
	if _, err := h.store.Delegate(ctx, scope, root.Task, req); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("generic Delegate crossed deployment: %v", err)
	}
	child, err := h.store.DelegateToTarget(ctx, scope, root.Task, req, policy)
	if err != nil {
		t.Fatal(err)
	}
	if child.Scope() != target || child.Parent != root.ID || child.Root != root.Root || child.Depth != 1 || len(child.Actors) != 2 || child.Actors[1] != target.Deployment {
		t.Fatalf("child binding: %+v", child)
	}
	if !contains(child.Labels.Workspaces, "private-parent") {
		t.Fatalf("lost parent journal: %+v", child.Labels)
	}
	if _, err := h.store.Get(ctx, scope, child.Task); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("child visible under source deployment: %v", err)
	}
	replay, err := h.store.DelegateToTarget(ctx, scope, root.Task, req, policy)
	if err != nil || replay.ID != child.ID {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	if _, err := h.store.AddLabels(ctx, target, child.Task, labels.Set{Taints: []string{labels.TaintSecret}}); err != nil {
		t.Fatal(err)
	}
	parent, err := h.store.Get(ctx, scope, root.Task)
	if err != nil || !contains(parent.Labels.Taints, labels.TaintSecret) {
		t.Fatalf("ancestor journal not tainted: %+v %v", parent, err)
	}
	grandTarget := target
	grandTarget.Deployment = "custom-grandchild"
	grandPolicy := policy
	grandPolicy.Deployment = grandTarget.Deployment
	grandReq := req
	grandReq.Task = "grandchild"
	grandReq.Context = ctxID(scope, "target-grandchild")
	grandReq.Deployment = grandTarget.Deployment
	grand, err := h.store.DelegateToTarget(ctx, target, child.Task, grandReq, grandPolicy)
	if err != nil || grand.Root != root.Root || grand.Depth != 2 {
		t.Fatalf("grandchild lineage: %+v %v", grand, err)
	}
	cyclePolicy := policy
	cyclePolicy.Deployment = scope.Deployment
	cycleReq := grandReq
	cycleReq.Deployment = scope.Deployment
	cycleReq.Task = "cycle"
	cycleReq.Context = ctxID(scope, "cycle")
	if _, err := h.store.DelegateToTarget(ctx, target, child.Task, cycleReq, cyclePolicy); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("actor cycle accepted: %v", err)
	}
	// Same child task key in a different deployment must not cancel ambiguously.
	siblingReq := req
	siblingReq.Deployment = "custom-sibling"
	siblingReq.Context = ctxID(scope, "sibling")
	siblingPolicy := policy
	siblingPolicy.Deployment = siblingReq.Deployment
	sibling, err := h.store.DelegateToTarget(ctx, scope, root.Task, siblingReq, siblingPolicy)
	if err != nil {
		t.Fatal(err)
	}
	token := h.mint(t, scope, root.Task)
	client := h.taskClient(t, scope)
	path := "/v1/identity/roots/root/children/child/cancel"
	if got := h.do(t, client, http.MethodPost, path, token, nil); got.status != http.StatusForbidden {
		t.Fatalf("ambiguous cancel: %d", got.status)
	}
	for i := 0; i < 2; i++ {
		got := h.do(t, client, http.MethodPost, path, token, map[string]string{"deployment": target.Deployment})
		if got.status != http.StatusNoContent {
			t.Fatalf("cross-deployment cancel/retry: %d %s", got.status, got.body)
		}
	}
	if _, err := h.store.Evaluate(ctx, grandTarget, grand.Task, "identity:read"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked descendant authorized: %v", err)
	}
	if _, err := h.store.Evaluate(ctx, sibling.Scope(), sibling.Task, "identity:read"); err != nil {
		t.Fatalf("sibling revoked: %v", err)
	}
}

func TestTrustedTargetDelegateHTTPBoundary(t *testing.T) {
	scope := uniqueScope(t)
	targetDeployment := "runtime-target"
	brokerDeployment := "trusted-gateway"
	h := newHarnessConfig(t, testAudience, func(cfg *identityapi.Config) {
		cfg.DelegatorDeployments = []string{brokerDeployment}
		cfg.ResolveDelegationTarget = func(_ context.Context, parent contracts.Scope, target string) (identity.DelegationTarget, error) {
			if parent != scope || target != targetDeployment {
				return identity.DelegationTarget{}, contracts.NewError(contracts.ErrForbidden, "target not installed for parent")
			}
			return identity.DelegationTarget{Tenant: scope.Tenant, Deployment: target, Capabilities: fullCaps, MaxDepth: 3, MaxFanout: 2, MaxActive: 8}, nil
		}
	})
	root := h.createRootStore(t, scope, "parent", false, nil)
	token := h.mint(t, scope, root.Task)
	brokerScope := scope
	brokerScope.Deployment = brokerDeployment
	broker := h.taskClient(t, brokerScope)
	path := "/v1/identity/roots/parent/delegate-to-target"
	body := map[string]any{"parent_deployment": scope.Deployment, "deployment": targetDeployment, "task": "http-child", "context": ctxID(scope, "http-child"), "capabilities": fullCaps, "ttl": "20m"}
	for name, client := range map[string]*http.Client{"ordinary": h.taskClient(t, scope), "no-mtls": h.human} {
		if got := h.do(t, client, http.MethodPost, path, token, body); got.status != http.StatusForbidden {
			t.Fatalf("%s broker accepted: %d %s", name, got.status, got.body)
		}
	}
	wrongTenant := brokerScope
	wrongTenant.Tenant = "foreign-tenant"
	if got := h.do(t, h.taskClient(t, wrongTenant), http.MethodPost, path, token, body); got.status != http.StatusForbidden {
		t.Fatalf("foreign broker tenant: %d", got.status)
	}
	if got := h.do(t, broker, http.MethodPost, "/v1/identity/roots/forged/delegate-to-target", token, body); got.status != http.StatusForbidden {
		t.Fatalf("forged parent: %d", got.status)
	}
	if got := h.do(t, broker, http.MethodPost, path, h.signHuman(scope), body); got.status != http.StatusForbidden {
		t.Fatalf("human used as task: %d", got.status)
	}
	h.createRootStore(t, scope, "limited-parent", false, func(r *identity.RootRequest) { r.Capabilities = []string{"identity:read"} })
	limited := h.mint(t, scope, "limited-parent")
	if got := h.do(t, broker, http.MethodPost, "/v1/identity/roots/limited-parent/delegate-to-target", limited, body); got.status != http.StatusForbidden {
		t.Fatalf("parent without delegation permission accepted: %d", got.status)
	}
	for name, value := range map[string]any{"deployment": "not-installed", "parent_deployment": "forged-source", "capabilities": append(append([]string{}, fullCaps...), "model:external"), "max_fanout": 3, "allow_self": true} {
		changed := map[string]any{}
		for k, v := range body {
			changed[k] = v
		}
		changed[name] = value
		if got := h.do(t, broker, http.MethodPost, path, token, changed); got.status != http.StatusForbidden {
			t.Fatalf("%s widened: %d %s", name, got.status, got.body)
		}
	}
	got := h.do(t, broker, http.MethodPost, path, token, body)
	if got.status != http.StatusCreated {
		t.Fatalf("trusted cross-deployment: %d %s", got.status, got.body)
	}
	child := grantFrom(t, got.body)
	var envelope struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(got.body, &envelope); err != nil {
		t.Fatal(err)
	}
	targetScope := scope
	targetScope.Deployment = targetDeployment
	claims, err := h.verifier.Verify(envelope.Token, testAudience, auth.KindTask, auth.WorkloadID(targetScope))
	if err != nil || claims.Task != child.Task || claims.Root != root.ID || claims.Subject != scope.Subject {
		t.Fatalf("target token binding: %+v %v", claims, err)
	}
	if child.MaxFanout != 2 || child.MaxActive != 8 {
		t.Fatalf("manifest limits not applied: %+v", child)
	}
	if err := h.store.Authorize(testContext(t), claims, "identity:read"); err != nil {
		t.Fatal(err)
	}
	readPath := "/v1/identity/roots/" + child.Task
	if got := h.do(t, h.taskClient(t, targetScope), http.MethodGet, readPath, envelope.Token, nil); got.status != http.StatusOK {
		t.Fatalf("target workload refused: %d %s", got.status, got.body)
	}
	if got := h.do(t, h.taskClient(t, scope), http.MethodGet, readPath, envelope.Token, nil); got.status != http.StatusForbidden {
		t.Fatalf("source used target token: %d", got.status)
	}
	// A revoked live parent is rejected before replay or a fresh child can mint.
	if err := h.store.CancelSubtree(testContext(t), scope, root.Task); err != nil {
		t.Fatal(err)
	}
	if got := h.do(t, broker, http.MethodPost, path, token, body); got.status != http.StatusForbidden {
		t.Fatalf("revoked parent replayed: %d %s", got.status, got.body)
	}
}

func TestTargetDelegationRequiresPolicyAndParentCapabilities(t *testing.T) {
	h := newHarnessConfig(t, testAudience, func(cfg *identityapi.Config) { cfg.DelegatorDeployments = []string{"broker"} })
	scope := uniqueScope(t)
	h.createRootStore(t, scope, "root", false, nil)
	tok := h.mint(t, scope, "root")
	broker := scope
	broker.Deployment = "broker"
	if got := h.do(t, h.taskClient(t, broker), http.MethodPost, "/v1/identity/roots/root/delegate-to-target", tok, map[string]any{}); got.status != http.StatusForbidden {
		t.Fatalf("nil target policy accepted: %d", got.status)
	}
	req := identity.DelegateRequest{Task: "child", Context: ctxID(scope, "child"), Deployment: "other", Capabilities: []string{"not-in-parent"}, TTL: time.Minute}
	policy := identity.DelegationTarget{Tenant: scope.Tenant, Deployment: "other", Capabilities: []string{"not-in-parent"}}
	if _, err := h.store.DelegateToTarget(testContext(t), scope, "root", req, policy); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("parent capability widened: %v", err)
	}
	policy.Tenant = "foreign"
	if _, err := h.store.DelegateToTarget(testContext(t), scope, "root", req, policy); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign manifest tenant: %v", err)
	}
}
