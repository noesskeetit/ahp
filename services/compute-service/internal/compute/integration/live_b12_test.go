package integration_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B12 §2.3 Identity: human/agent/grant token types + exchange — 8 situations.

func newB12Identity(t *testing.T) *identity.Store {
	t.Helper()
	e := newLiveEnv(t)
	store := identity.New(e.pool)
	if err := store.Migrate(ctx(t)); err != nil {
		t.Fatalf("identity Migrate: %v", err)
	}
	return store
}

// 1. Three token kinds: root (human), child level 1 (agent), child level 2 (grant).
func TestB12Sit1_ThreeTokenKinds(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s1-" + uid, Deployment: "dep-1", Subject: "alice"}
	rootTask := "root-task-1-" + uid
	agentTask := "agent-task-1-" + uid
	grantTask := "grant-task-1-" + uid

	// Create root grant (human token).
	root, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-1-" + uid, Deployment: "dep-1",
		Capabilities: []string{"compute", "state", "artifacts"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if root.Depth != 0 {
		t.Fatalf("root depth=%d want 0", root.Depth)
	}

	// Delegate to agent (level 1).
	agent, err := s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: agentTask, Context: "ctx-1-" + uid, Deployment: "dep-1",
		Capabilities: []string{"compute", "state"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate (agent): %v", err)
	}
	if agent.Depth != 1 {
		t.Fatalf("agent depth=%d want 1", agent.Depth)
	}

	// Delegate to grant (level 2).
	grant, err := s.Delegate(ctx(t), sc, agentTask, identity.DelegateRequest{
		Task: grantTask, Context: "ctx-1-" + uid, Deployment: "dep-1",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate (grant): %v", err)
	}
	if grant.Depth != 2 {
		t.Fatalf("grant depth=%d want 2", grant.Depth)
	}
	t.Log("Sit1: three kinds — root(depth=0)→agent(depth=1)→grant(depth=2), caps narrow")
}

// 2. Token exchange: root → agent (scoped to task, narrower TTL).
func TestB12Sit2_TokenExchange(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s2-" + uid, Deployment: "dep-2", Subject: "bob"}
	rootTask := "exchange-root-" + uid
	childTask := "exchange-agent-" + uid

	root, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-2-" + uid, Deployment: "dep-2",
		Capabilities: []string{"compute", "state", "artifacts", "workspace"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Exchange: root → agent with narrower scope and shorter TTL.
	child, err := s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: childTask, Context: "ctx-2-" + uid, Deployment: "dep-2",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	// Child TTL must be less than parent (scoped exchange).
	if !child.ExpiresAt.Before(root.ExpiresAt) {
		t.Fatalf("child expiry=%v not before parent=%v (exchange must narrow)",
			child.ExpiresAt, root.ExpiresAt)
	}
	// Child capabilities must be a subset.
	if len(child.Capabilities) > len(root.Capabilities) {
		t.Fatal("child has more capabilities than parent (widening)")
	}
	t.Log("Sit2: exchange — root→agent narrows TTL and capabilities")
}

// 3. Kind enforcement: Evaluate rejects capability not in child's set.
func TestB12Sit3_KindEnforcement(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s3-" + uid, Deployment: "dep-3", Subject: "carol"}
	rootTask := "enforce-root-" + uid
	childTask := "enforce-child-" + uid

	// Root has all capabilities.
	_, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-3-" + uid, Deployment: "dep-3",
		Capabilities: []string{"compute", "state", "artifacts"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Child only has "compute".
	_, err = s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: childTask, Context: "ctx-3-" + uid, Deployment: "dep-3",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}

	// Child CAN use "compute".
	if _, err := s.Evaluate(ctx(t), sc, childTask, "compute"); err != nil {
		t.Fatalf("Evaluate(compute): %v (should be allowed)", err)
	}

	// Child CANNOT use "state" (not in its capability set).
	_, err = s.Evaluate(ctx(t), sc, childTask, "state")
	if err == nil {
		t.Fatal("Evaluate(state) on child: want FORBIDDEN, got nil")
	}
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("Evaluate(state): code=%s want FORBIDDEN", contracts.Code(err))
	}
	t.Log("Sit3: kind enforcement — child without 'state' cap gets FORBIDDEN on Evaluate")
}

// 4. Scope narrowing: delegating with wider capabilities is rejected.
func TestB12Sit4_ScopeNarrowing(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s4-" + uid, Deployment: "dep-4", Subject: "dave"}
	rootTask := "narrow-root-" + uid
	childTask := "narrow-child-" + uid

	// Root only has "compute".
	_, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-4-" + uid, Deployment: "dep-4",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Try to delegate with "compute" + "state" (wider than parent) → rejected.
	_, err = s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: childTask, Context: "ctx-4-" + uid, Deployment: "dep-4",
		Capabilities: []string{"compute", "state"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err == nil {
		t.Fatal("Delegate with wider caps: want error, got nil (widening allowed!)")
	}
	if contracts.Code(err) != contracts.ErrInvalid && contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("widening: code=%s want INVALID or FORBIDDEN", contracts.Code(err))
	}
	t.Log("Sit4: scope narrowing — widening capabilities is rejected")
}

// 5. Token lifetime: child TTL must be shorter than parent remaining.
func TestB12Sit5_TokenLifetime(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s5-" + uid, Deployment: "dep-5", Subject: "erin"}
	rootTask := "life-root-" + uid
	agentTask := "life-agent-" + uid
	grantTask := "life-grant-" + uid

	// Root: 1h.
	root, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-5-" + uid, Deployment: "dep-5",
		Capabilities: []string{"compute", "state"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Agent: 30m (≤ parent 1h).
	agent, err := s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: agentTask, Context: "ctx-5-" + uid, Deployment: "dep-5",
		Capabilities: []string{"compute", "state"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate (agent): %v", err)
	}

	// Grant: 5m (≤ agent 30m).
	grant, err := s.Delegate(ctx(t), sc, agentTask, identity.DelegateRequest{
		Task: grantTask, Context: "ctx-5-" + uid, Deployment: "dep-5",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate (grant): %v", err)
	}

	// Verify lifetime ordering: grant < agent < root.
	if !grant.ExpiresAt.Before(agent.ExpiresAt) {
		t.Fatalf("grant expiry=%v not before agent=%v", grant.ExpiresAt, agent.ExpiresAt)
	}
	if !agent.ExpiresAt.Before(root.ExpiresAt) {
		t.Fatalf("agent expiry=%v not before root=%v", agent.ExpiresAt, root.ExpiresAt)
	}
	t.Logf("Sit5: lifetime — root(1h)>agent(30m)>grant(5m) all enforced")
}

// 6. Revocation: RevokeSubject makes all grants for that subject rejected.
func TestB12Sit6_Revocation(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s6-" + uid, Deployment: "dep-6", Subject: "frank-" + uid}
	rootTask := "revoke-root-" + uid

	// Create a root grant.
	_, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-6-" + uid, Deployment: "dep-6",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Verify it's active before revocation.
	g, err := s.Get(ctx(t), sc, rootTask)
	if err != nil {
		t.Fatalf("Get before revoke: %v", err)
	}
	if g.Status != identity.StatusActive {
		t.Fatalf("status before revoke=%q want active", g.Status)
	}

	// Revoke the subject.
	err = s.RevokeSubject(ctx(t), sc.Tenant, sc.Subject)
	if err != nil {
		t.Fatalf("RevokeSubject: %v", err)
	}

	// After revocation, the grant must be revoked.
	g2, err := s.Get(ctx(t), sc, rootTask)
	if err != nil {
		t.Fatalf("Get after revoke: %v", err)
	}
	if g2.Status != identity.StatusRevoked {
		t.Fatalf("status after revoke=%q want revoked", g2.Status)
	}
	t.Log("Sit6: revocation — RevokeSubject flips grant to revoked (immediately effective)")
}

// 7. Cross-tenant: token from tenant A cannot be used in tenant B.
func TestB12Sit7_CrossTenant(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	tenantA := "t-b12-tenant-a-" + uid
	tenantB := "t-b12-tenant-b-" + uid
	scA := contracts.Scope{Tenant: tenantA, Deployment: "dep-7", Subject: "grace-" + uid}
	scB := contracts.Scope{Tenant: tenantB, Deployment: "dep-7", Subject: "grace-" + uid}
	rootTask := "cross-root-" + uid

	// Create root in tenant A.
	_, err := s.CreateRoot(ctx(t), scA, identity.RootRequest{
		Task: rootTask, Context: "ctx-7-" + uid, Deployment: "dep-7",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot (tenant A): %v", err)
	}

	// Tenant B cannot see tenant A's grant.
	_, err = s.Get(ctx(t), scB, rootTask)
	if err == nil {
		t.Fatal("cross-tenant Get: want error, got nil (isolation violated!)")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit7: cross-tenant — tenant B cannot access tenant A's grants (NOT_FOUND)")
}

// 8. Audit: delegation creates a child grant with full provenance.
func TestB12Sit8_AuditLog(t *testing.T) {
	s := newB12Identity(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b12-s8-" + uid, Deployment: "dep-8", Subject: "heidi"}
	rootTask := "audit-root-" + uid
	childTask := "audit-child-" + uid

	// Create root.
	root, err := s.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: rootTask, Context: "ctx-8-" + uid, Deployment: "dep-8",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	// Delegate (audit trail: child references parent).
	child, err := s.Delegate(ctx(t), sc, rootTask, identity.DelegateRequest{
		Task: childTask, Context: "ctx-8-" + uid, Deployment: "dep-8",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          30 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}

	// Verify audit trail: child has parent reference and issued_at.
	if child.Parent != root.ID {
		t.Fatalf("child parent=%q want root ID %s", child.Parent, root.ID)
	}
	if child.Root == "" {
		t.Fatal("child root is empty (no provenance)")
	}
	if child.ExpiresAt.IsZero() {
		t.Fatal("child ExpiresAt is zero (no expiry set)")
	}

	// Verify via Get that the full record is queryable.
	got, err := s.Get(ctx(t), sc, childTask)
	if err != nil {
		t.Fatalf("Get child: %v", err)
	}
	if got.Task != childTask || got.Parent != root.ID {
		t.Fatalf("audit record mismatch: task=%q parent=%q", got.Task, got.Parent)
	}
	t.Logf("Sit8: audit — child {task=%s, parent=%s, root=%s, expires=%s} fully traceable",
		child.Task, child.Parent, child.Root, child.ExpiresAt.Format(time.RFC3339))
}
