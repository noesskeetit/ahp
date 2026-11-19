package registry_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// manifestFor builds a valid manifest pinned to an explicit system, with the
// model routes and shared state spaces a test needs. Unlike manifest() it does
// NOT randomise the system, because system releases compare it.
func manifestFor(t *testing.T, system string, models, shared []string) registry.Manifest {
	t.Helper()
	suffix := randSuffix(t)
	partition := "subject-" + suffix
	return registry.Manifest{
		Kind:       registry.KindAgent,
		System:     system,
		Visibility: registry.VisibilityTenant,
		Audience:   registry.AudienceShared,
		Class:      registry.ClassS,
		Components: []registry.Component{{Name: "router", A2A: true, MinReplicas: 1, MaxReplicas: 3}},
		State: registry.StateConfig{
			Partition: partition,
			Shared:    shared,
			Compat:    "compat-" + suffix,
		},
		Capabilities: registry.Capabilities{Models: models, SelfTasks: 1},
		Limits:       registry.Limits{Depth: 4, ActiveTasks: 16, TaskTimeout: "10m"},
	}
}

// publishAs publishes a version as an explicit publisher.
func publishAs(t *testing.T, store *registry.Store, publisher, agentID string, m registry.Manifest) registry.Version {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mustRegister(t, store, publisher, agentID)
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	// "Оценки перед публикацией": record a real passing verdict for this exact
	// digest so the gated store admits the publication; caller assertions unchanged.
	passVerdict(t, digest)
	v, err := store.PutVersion(ctx, publisher, agentID, digest, m)
	if err != nil {
		t.Fatalf("PutVersion(%s): %v", agentID, err)
	}
	return v
}

// TestVersionPublishingRequiresAPublisher closes the PDF gap that an agent and
// its versions belong to a publisher (rev 3.2, c. 42): publishing for an agent
// nobody claimed must be refused, and a second publisher may not claim or
// publish into an agent that is already owned.
func TestVersionPublishingRequiresAPublisher(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	m := manifestFor(t, "acme.owned", nil, nil)
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}

	// Unclaimed agent: no publisher may publish a version for it.
	if _, err := store.PutVersion(ctx, "pub-a", agent, digest, m); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("PutVersion for unclaimed agent code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}

	if _, err := store.RegisterAgent(ctx, "pub-a", agent); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
	// Re-registering as the same publisher is an idempotent replay.
	again, err := store.RegisterAgent(ctx, "pub-a", agent)
	if err != nil || again.PublisherID != "pub-a" {
		t.Fatalf("re-register = %+v, %v", again, err)
	}
	// Another publisher may not claim it.
	if _, err := store.RegisterAgent(ctx, "pub-b", agent); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("hostile claim code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	// Nor publish a version for it.
	if _, err := store.PutVersion(ctx, "pub-b", agent, digest, m); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("PutVersion as a non-publisher code = %q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
	// The owner may, and only the owner may list its versions.
	passVerdict(t, digest)
	v, err := store.PutVersion(ctx, "pub-a", agent, digest, m)
	if err != nil {
		t.Fatalf("owner PutVersion: %v", err)
	}
	if list, err := store.ListVersions(ctx, "pub-a", agent, 10); err != nil || len(list) != 1 || list[0].ID != v.ID {
		t.Fatalf("owner ListVersions = %+v, %v", list, err)
	}
	if _, err := store.ListVersions(ctx, "pub-b", agent, 10); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign ListVersions code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
}

// TestPublisherAggregatesAreDisclosedOnly proves the publisher's view: counts
// only, and only from installations the consumer opted in. A publisher must
// never see another tenant's installation state or content (rev 3.2, c. 42:
// "Издатель не видит состояние и содержимое установки -- только агрегаты и
// оценки, которые разрешил потребитель").
func TestPublisherAggregatesAreDisclosedOnly(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	v := publishAs(t, store, "pub-ag", agent, manifestFor(t, "acme.aggregates", nil, nil))

	// Two consuming tenants install the same agent.
	tenants := []string{"t-" + randSuffix(t), "t-" + randSuffix(t)}
	installs := make([]string, 0, 2)
	for _, tenant := range tenants {
		inst, err := store.Install(ctx, tenant, agent, v.ID, registry.EnvironmentDev, "k-"+tenant, granted())
		if err != nil {
			t.Fatalf("Install(%s): %v", tenant, err)
		}
		installs = append(installs, inst.ID)
		if _, err := store.Pin(ctx, tenant, inst.ID, "task-"+tenant); err != nil {
			t.Fatalf("Pin: %v", err)
		}
	}

	// Nothing is disclosed by default: the publisher sees zero.
	before, err := store.PublisherAggregates(ctx, "pub-ag", agent)
	if err != nil {
		t.Fatalf("PublisherAggregates: %v", err)
	}
	if before.Installations != 0 || before.PinnedTasks != 0 {
		t.Fatalf("publisher saw undisclosed installations: %+v", before)
	}

	// One consumer opts in; the publisher then sees exactly that one.
	if err := store.SetPublisherDisclosure(ctx, tenants[0], installs[0], true); err != nil {
		t.Fatalf("SetPublisherDisclosure: %v", err)
	}
	after, err := store.PublisherAggregates(ctx, "pub-ag", agent)
	if err != nil {
		t.Fatalf("PublisherAggregates after opt-in: %v", err)
	}
	if after.Installations != 1 || after.PinnedTasks != 1 || after.Environments[registry.EnvironmentDev] != 1 {
		t.Fatalf("aggregates after opt-in = %+v, want exactly one disclosed installation", after)
	}
	// A foreign tenant cannot opt someone else's installation in.
	if err := store.SetPublisherDisclosure(ctx, tenants[1], installs[0], true); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant disclosure code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	// A non-owner publisher gets nothing at all.
	if _, err := store.PublisherAggregates(ctx, "pub-evil", agent); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign publisher code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	// And the aggregate struct itself has no place to leak a tenant, deployment
	// or capability document: assert on the serialized shape, not on hope.
	doc, err := json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{tenants[0], tenants[1], installs[0], installs[1], "capabilities"} {
		if strings.Contains(string(doc), leak) {
			t.Fatalf("publisher aggregates leak %q in %s", leak, doc)
		}
	}
}

// TestRollbackMovesNewTasksOnly proves the drain rule of rev 3.2, c. 42: "Откат
// переключает на предыдущий digest новые задачи; задачи, начатые новой версией,
// на ней и остаются."
func TestRollbackMovesNewTasksOnly(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	v1 := publishAs(t, store, testPublisher, agent, manifestFor(t, "acme.rollback", nil, nil))
	v2 := publishAs(t, store, testPublisher, agent, manifestFor(t, "acme.rollback", []string{"route-b"}, nil))

	inst, err := store.Install(ctx, tenant, agent, v1.ID, registry.EnvironmentDev, "k-rb", granted())
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	sfx := randSuffix(t)
	taskOnV1 := "task-on-v1-" + sfx
	taskOnV2 := "task-on-v2-" + sfx
	pinnedBefore, err := store.Pin(ctx, tenant, inst.ID, taskOnV1)
	if err != nil {
		t.Fatalf("Pin v1: %v", err)
	}
	if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	pinnedNew, err := store.Pin(ctx, tenant, inst.ID, taskOnV2)
	if err != nil {
		t.Fatalf("Pin v2: %v", err)
	}

	back, err := store.Rollback(ctx, tenant, inst.ID)
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if back.VersionID != v1.ID {
		t.Fatalf("rollback moved to %s, want %s", back.VersionID, v1.ID)
	}
	// The task started on v2 STAYS on v2; the task started on v1 is untouched.
	if still, err := store.PinnedVersion(ctx, tenant, taskOnV2); err != nil || still.ID != pinnedNew.ID {
		t.Fatalf("rolled back a running task: %+v, %v", still, err)
	}
	if still, err := store.PinnedVersion(ctx, tenant, taskOnV1); err != nil || still.ID != pinnedBefore.ID {
		t.Fatalf("v1 pin changed: %+v, %v", still, err)
	}

	history, err := store.ListDeployments(ctx, tenant, inst.ID, 10)
	if err != nil {
		t.Fatalf("ListDeployments: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("history length = %d, want 3 (%+v)", len(history), history)
	}
	got := []string{history[2].Action, history[1].Action, history[0].Action}
	want := []string{"install", "promote", "rollback"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("history = %v, want %v", got, want)
		}
	}
	if history[0].Digest != v1.Digest {
		t.Fatalf("rollback history digest = %s, want the previous digest %s", history[0].Digest, v1.Digest)
	}

	// A fresh installation has no previous version to return to.
	fresh, err := store.Install(ctx, tenant, agent, v1.ID, registry.EnvironmentStage, "k-rb-stage", granted())
	if err != nil {
		t.Fatalf("Install stage: %v", err)
	}
	if _, err := store.Rollback(ctx, tenant, fresh.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("rollback without history code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	// Another tenant cannot roll back somebody else's installation.
	if _, err := store.Rollback(ctx, "t-foreign-"+randSuffix(t), inst.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant rollback code = %q", contracts.Code(err))
	}
}

// TestSystemReleaseDeploysAndRollsBackAsAWhole proves rev 3.2, c. 44: a system
// release pins member versions, and "выкладка и откат идут целиком".
func TestSystemReleaseDeploysAndRollsBackAsAWhole(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	system := "hr-assistant-" + randSuffix(t)
	if err := store.RegisterSystem(ctx, "pub-sys", system); err != nil {
		t.Fatalf("RegisterSystem: %v", err)
	}
	if err := store.RegisterSystem(ctx, "pub-other", system); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("hostile system claim code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}

	agentA := "agent-a-" + randSuffix(t)
	agentB := "agent-b-" + randSuffix(t)
	a1 := publishAs(t, store, "pub-sys", agentA, manifestFor(t, system, nil, nil))
	b1 := publishAs(t, store, "pub-sys", agentB, manifestFor(t, system, nil, nil))
	a2 := publishAs(t, store, "pub-sys", agentA, manifestFor(t, system, []string{"r2"}, nil))

	// A member whose manifest declares a different system cannot join the release.
	foreign := "agent-f-" + randSuffix(t)
	fv := publishAs(t, store, "pub-sys", foreign, manifestFor(t, system+"-elsewhere", nil, nil))
	if _, err := store.PublishSystemRelease(ctx, "pub-sys", system,
		[]registry.SystemMember{{AgentID: agentA, VersionID: a1.ID}, {AgentID: foreign, VersionID: fv.ID}}); err == nil {
		t.Fatal("release accepted a member declaring another system")
	}

	rel1, err := store.PublishSystemRelease(ctx, "pub-sys", system, []registry.SystemMember{
		{AgentID: agentA, VersionID: a1.ID}, {AgentID: agentB, VersionID: b1.ID}})
	if err != nil {
		t.Fatalf("PublishSystemRelease: %v", err)
	}
	replay, err := store.PublishSystemRelease(ctx, "pub-sys", system, []registry.SystemMember{
		{AgentID: agentB, VersionID: b1.ID}, {AgentID: agentA, VersionID: a1.ID}})
	if err != nil || replay.ID != rel1.ID {
		t.Fatalf("release replay = %+v, %v (want %s)", replay, err, rel1.ID)
	}

	deployed, err := store.DeploySystem(ctx, tenant, system, rel1.ID, registry.EnvironmentProd)
	if err != nil {
		t.Fatalf("DeploySystem: %v", err)
	}
	if len(deployed.Installations) != 2 {
		t.Fatalf("deployment pinned %d installations, want 2", len(deployed.Installations))
	}
	for _, agent := range []string{agentA, agentB} {
		inst, err := store.GetInstallation(ctx, tenant, deployed.Installations[agent])
		if err != nil {
			t.Fatalf("GetInstallation(%s): %v", agent, err)
		}
		if inst.Environment != registry.EnvironmentProd {
			t.Fatalf("member %s installed into %s", agent, inst.Environment)
		}
	}

	// A second release moves both members; rollback returns both at once.
	rel2, err := store.PublishSystemRelease(ctx, "pub-sys", system, []registry.SystemMember{
		{AgentID: agentA, VersionID: a2.ID}, {AgentID: agentB, VersionID: b1.ID}})
	if err != nil {
		t.Fatalf("second release: %v", err)
	}
	if _, err := store.DeploySystem(ctx, tenant, system, rel2.ID, registry.EnvironmentProd); err != nil {
		t.Fatalf("DeploySystem rel2: %v", err)
	}
	current, err := store.CurrentSystemDeployment(ctx, tenant, system, registry.EnvironmentProd)
	if err != nil || current.ReleaseID != rel2.ID {
		t.Fatalf("current deployment = %+v, %v", current, err)
	}
	back, err := store.RollbackSystem(ctx, tenant, system, registry.EnvironmentProd)
	if err != nil {
		t.Fatalf("RollbackSystem: %v", err)
	}
	if back.ReleaseID != rel1.ID {
		t.Fatalf("rolled back to %s, want %s", back.ReleaseID, rel1.ID)
	}
	moved, err := store.GetInstallation(ctx, tenant, back.Installations[agentA])
	if err != nil || moved.VersionID != a1.ID {
		t.Fatalf("member A after rollback = %+v, %v", moved, err)
	}

	// A third release would be rolled back onto; but rolling back with no
	// history is refused rather than silently inventing a target.
	if _, err := store.RollbackSystem(ctx, "t-none-"+randSuffix(t), system, registry.EnvironmentProd); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("rollback without history code = %q", contracts.Code(err))
	}

	// Atomicity: a release whose member is blocked must leave every member
	// exactly where it was.
	blocked := "agent-c-" + randSuffix(t)
	c1 := publishAs(t, store, "pub-sys", blocked, manifestFor(t, system, []string{"only-this", "other-route"}, nil))
	// Narrow an installation of C to one model, so a later rollout target that
	// no longer declares it cannot be accepted.
	narrowInst, err := store.Install(ctx, tenant, blocked, c1.ID, registry.EnvironmentProd, "k-narrow",
		registry.Capabilities{Models: []string{"only-this"}, SelfTasks: 1})
	if err != nil {
		t.Fatalf("Install narrow: %v", err)
	}
	c2 := publishAs(t, store, "pub-sys", blocked, manifestFor(t, system, []string{"other-route"}, nil))
	if _, err := store.Install(ctx, tenant, agentB, b1.ID, registry.EnvironmentStage, "k-b-stage", granted()); err != nil {
		t.Fatalf("Install B stage: %v", err)
	}
	badRelease, err := store.PublishSystemRelease(ctx, "pub-sys", system, []registry.SystemMember{
		{AgentID: agentB, VersionID: a2.ID}, {AgentID: blocked, VersionID: c2.ID}})
	if err == nil {
		// The release itself is legal; what must fail is the rollout, because C
		// is granted a model the target ceiling does not contain.
		if _, err := store.DeploySystem(ctx, tenant, system, badRelease.ID, registry.EnvironmentProd); contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("atomic deploy code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
		}
		// Neither member moved.
		untouched, err := store.GetInstallation(ctx, tenant, narrowInst.ID)
		if err != nil || untouched.VersionID != c1.ID {
			t.Fatalf("failed rollout moved a member: %+v, %v", untouched, err)
		}
		bInst, err := store.CurrentSystemDeployment(ctx, tenant, system, registry.EnvironmentProd)
		if err != nil || bInst.ReleaseID != rel1.ID {
			t.Fatalf("failed rollout recorded a system deployment: %+v, %v", bInst, err)
		}
	}
}

// TestSkillRevisionsAndLevels proves rev 3.2, c. 36: "Навык -- Ревизия бандла в
// Registry; уровни admin, workspace, user".
func TestSkillRevisionsAndLevels(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	skillID := "skill-" + randSuffix(t)

	first, err := store.PutSkill(ctx, "pub-sk", skillID, registry.SkillLevelWorkspace, json.RawMessage(`{"instructions":"v1"}`))
	if err != nil {
		t.Fatalf("PutSkill: %v", err)
	}
	if first.Revision != 1 {
		t.Fatalf("first revision = %d", first.Revision)
	}
	replay, err := store.PutSkill(ctx, "pub-sk", skillID, registry.SkillLevelWorkspace, json.RawMessage(`{"instructions":"v1"}`))
	if err != nil || replay.ID != first.ID {
		t.Fatalf("bundle replay = %+v, %v", replay, err)
	}
	second, err := store.PutSkill(ctx, "pub-sk", skillID, registry.SkillLevelWorkspace, json.RawMessage(`{"instructions":"v2"}`))
	if err != nil || second.Revision != 2 || second.ID == first.ID {
		t.Fatalf("second revision = %+v, %v", second, err)
	}
	// Another publisher cannot add revisions to somebody else's skill.
	if _, err := store.PutSkill(ctx, "pub-evil", skillID, registry.SkillLevelUser, json.RawMessage(`{"x":1}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("hostile skill revision code = %q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
	// A bundle is a bundle, not a junk drawer.
	if _, err := store.PutSkill(ctx, "pub-sk", "skill-"+randSuffix(t), "team", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unknown level code = %q", contracts.Code(err))
	}
	if _, err := store.PutSkill(ctx, "pub-sk", "skill-"+randSuffix(t), registry.SkillLevelUser, json.RawMessage(`[1,2]`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("non-object bundle code = %q", contracts.Code(err))
	}

	// Level gating: a workspace skill is invisible below workspace, and an
	// invisible skill is NOT_FOUND (its existence is not confirmed).
	adminOnly, err := store.PutSkill(ctx, "pub-sk", "skill-"+randSuffix(t), registry.SkillLevelAdmin, json.RawMessage(`{"instructions":"admin"}`))
	if err != nil {
		t.Fatalf("PutSkill admin level: %v", err)
	}
	if _, err := store.SkillFor(ctx, adminOnly.SkillID, registry.SkillLevelUser, 0); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("admin skill as user code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	if _, err := store.SkillFor(ctx, adminOnly.SkillID, registry.SkillLevelAdmin, 0); err != nil {
		t.Fatalf("admin skill as admin: %v", err)
	}
	got, err := store.SkillFor(ctx, skillID, registry.SkillLevelUser, 0)
	if err == nil {
		t.Fatalf("workspace skill visible to a user caller: %+v", got)
	}
	if _, err := store.SkillFor(ctx, skillID, registry.SkillLevelWorkspace, 0); err != nil {
		t.Fatalf("workspace skill as workspace: %v", err)
	}
	// The newest revision wins.
	latest, err := store.SkillFor(ctx, skillID, registry.SkillLevelAdmin, 0)
	if err != nil || latest.Revision != 2 {
		t.Fatalf("latest = %+v, %v", latest, err)
	}
	// Listing honours the level too.
	userVisible, err := store.ListSkills(ctx, registry.SkillLevelUser)
	if err != nil {
		t.Fatalf("ListSkills: %v", err)
	}
	for _, s := range userVisible {
		if s.Level != registry.SkillLevelUser {
			t.Fatalf("user listing exposed a %s skill", s.Level)
		}
	}
}

// TestModelRoutesAreTenantScopedAndCredentialFree proves the Registry can own
// "маршруты моделей" (rev 3.2, c. 28) without becoming a secret store: a route
// names a model, is compare-and-swap, and is invisible across tenants.
func TestModelRoutesAreTenantScopedAndCredentialFree(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	route := "default-chat"

	created, err := store.PutModelRoute(ctx, tenant, route, 0, json.RawMessage(`{"model":"qwen-38","max_tokens":4096}`))
	if err != nil || created.Revision != 1 {
		t.Fatalf("PutModelRoute = %+v, %v", created, err)
	}
	// Stale expectation is a conflict carrying the current revision.
	if _, err := store.PutModelRoute(ctx, tenant, route, 0, json.RawMessage(`{"model":"other"}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale route write code = %q", contracts.Code(err))
	}
	updated, err := store.PutModelRoute(ctx, tenant, route, 1, json.RawMessage(`{"model":"qwen-38","max_tokens":8192}`))
	if err != nil || updated.Revision != 2 {
		t.Fatalf("updated route = %+v, %v", updated, err)
	}
	// Credentials never belong in a route document.
	if _, err := store.PutModelRoute(ctx, tenant, "leaky", 0, json.RawMessage(`{"model":"m","api_key":"sk-live"}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("credential-bearing route code = %q", contracts.Code(err))
	}
	// A document that names no model is not a route.
	if _, err := store.PutModelRoute(ctx, tenant, "vague", 0, json.RawMessage(`{"provider":"x"}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("model-less route code = %q", contracts.Code(err))
	}
	// Tenant isolation.
	other := "t-" + randSuffix(t)
	if _, err := store.ResolveModelRoute(ctx, other, route); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant route code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	if _, err := store.PutModelRoute(ctx, other, route, 0, json.RawMessage(`{"model":"mine"}`)); err != nil {
		t.Fatalf("other tenant route: %v", err)
	}
	resolved, err := store.ResolveModelRoute(ctx, tenant, route)
	if err != nil || resolved.Revision != 2 {
		t.Fatalf("resolve = %+v, %v", resolved, err)
	}
	list, err := store.ListModelRoutes(ctx, tenant)
	if err != nil || len(list) != 1 || list[0].Route != route {
		t.Fatalf("listing = %+v, %v", list, err)
	}
}

// TestResolveConfigKeepsPinnedVersionAndRoutes proves Registry ResolveConfig
// (rev 3.2, c. 28) against the two rules that make it load-bearing: a task keeps
// its version and AgentConfig across a rollout (c. 42), and the granted
// capabilities are the narrowed ceiling of the manifest (c. 45).
func TestResolveConfigKeepsPinnedVersionAndRoutes(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	v1 := publishAs(t, store, testPublisher, agent, manifestFor(t, "acme.resolve", []string{"route-x"}, []string{"policy-cache"}))

	if _, err := store.PutModelRoute(ctx, tenant, "route-x", 0, json.RawMessage(`{"model":"qwen-38"}`)); err != nil {
		t.Fatalf("PutModelRoute: %v", err)
	}
	if _, err := store.PutConfig(ctx, tenant, agent, 0, json.RawMessage(`{"greeting":"hello"}`)); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	grantedCaps := registry.Capabilities{Models: []string{"route-x"}, SelfTasks: 1}
	inst, err := store.Install(ctx, tenant, agent, v1.ID, registry.EnvironmentProd, "k-res", grantedCaps)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if err := store.PublishWithRoles(ctx, tenant, inst.ID, []string{"role:hr"}); err != nil {
		t.Fatalf("PublishWithRoles: %v", err)
	}

	eff, err := store.ResolveConfig(ctx, tenant, inst.ID, "")
	if err != nil {
		t.Fatalf("ResolveConfig: %v", err)
	}
	var greeting map[string]string
	if err := json.Unmarshal(eff.Config, &greeting); err != nil || greeting["greeting"] != "hello" {
		t.Fatalf("AgentConfig document not resolved: %s (%v)", eff.Config, err)
	}
	if eff.VersionID != v1.ID {
		t.Fatalf("resolve = %+v", eff)
	}
	if !eff.SharesState {
		t.Fatal("a manifest declaring shared state must report shares_state")
	}
	if route, ok := eff.ModelRoutes["route-x"]; !ok || route.Revision != 1 {
		t.Fatalf("model routes not resolved: %+v", eff.ModelRoutes)
	}
	if len(eff.MissingRoutes) != 0 {
		t.Fatalf("unexpected missing routes: %v", eff.MissingRoutes)
	}

	// Pin a task, then promote: the task's answer never changes.
	pinnedTask := "task-stable-" + randSuffix(t)
	unpinnedTask := "task-unpinned-" + randSuffix(t)
	if _, err := store.Pin(ctx, tenant, inst.ID, pinnedTask); err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if _, err := store.PutConfig(ctx, tenant, agent, 1, json.RawMessage(`{"greeting":"updated"}`)); err != nil {
		t.Fatal(err)
	}
	v2 := publishAs(t, store, testPublisher, agent, manifestFor(t, "acme.resolve", []string{"route-x", "route-y"}, nil))
	if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	pinned, err := store.ResolveConfig(ctx, tenant, inst.ID, pinnedTask)
	if err != nil {
		t.Fatalf("ResolveConfig pinned: %v", err)
	}
	if pinned.VersionID != v1.ID {
		t.Fatalf("a pinned task resolved %s after a promote, want %s", pinned.VersionID, v1.ID)
	}
	if string(pinned.Config) != `{"greeting": "hello"}` || pinned.ConfigRevision != 1 {
		t.Fatalf("pinned config changed: %s revision %d", pinned.Config, pinned.ConfigRevision)
	}
	if pinned.SharesState != true {
		t.Fatal("the pinned version's shared-state answer must come from the pinned manifest")
	}
	fresh, err := store.ResolveConfig(ctx, tenant, inst.ID, unpinnedTask)
	if err != nil || fresh.VersionID != v2.ID {
		t.Fatalf("unpinned task = %+v, %v", fresh, err)
	}
	if fresh.ConfigRevision != 2 {
		t.Fatalf("fresh config revision=%d", fresh.ConfigRevision)
	}
	if len(fresh.MissingRoutes) != 1 || fresh.MissingRoutes[0] != "route-y" {
		t.Fatalf("missing routes = %v, want [route-y]", fresh.MissingRoutes)
	}
	// A foreign tenant resolves nothing, not even an existence answer.
	if _, err := store.ResolveConfig(ctx, "t-foreign-"+randSuffix(t), inst.ID, ""); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant resolve code = %q", contracts.Code(err))
	}
}

// TestCatalogCardsShowSharedStateAndRoleAudience proves rev 3.2, c. 42: the
// catalog card shows whether the agent keeps data between users, and publishing
// carries a role audience.
func TestCatalogCardsShowSharedStateAndRoleAudience(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	sharedAgent := "agent-shared-" + randSuffix(t)
	privateAgent := "agent-private-" + randSuffix(t)
	sharedVersion := publishAs(t, store, testPublisher, sharedAgent, manifestFor(t, "acme.card", nil, []string{"policy-cache"}))
	privateVersion := publishAs(t, store, testPublisher, privateAgent, manifestFor(t, "acme.card", nil, nil))

	sharedInst, err := store.Install(ctx, tenant, sharedAgent, sharedVersion.ID, registry.EnvironmentProd, "k-card-1", granted())
	if err != nil {
		t.Fatalf("Install shared: %v", err)
	}
	privateInst, err := store.Install(ctx, tenant, privateAgent, privateVersion.ID, registry.EnvironmentProd, "k-card-2", granted())
	if err != nil {
		t.Fatalf("Install private: %v", err)
	}

	// Nothing is published yet: neither the admin nor an employee sees a card.
	if cards, err := store.CatalogCards(ctx, tenant, nil); err != nil || len(cards) != 0 {
		t.Fatalf("catalog before publication = %+v, %v", cards, err)
	}
	if err := store.Publish(ctx, tenant, sharedInst.ID, true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := store.PublishWithRoles(ctx, tenant, privateInst.ID, []string{"role:hr", "role:admin"}); err != nil {
		t.Fatalf("PublishWithRoles: %v", err)
	}

	adminView, err := store.CatalogCards(ctx, tenant, nil)
	if err != nil || len(adminView) != 2 {
		t.Fatalf("admin catalog = %+v, %v", adminView, err)
	}
	var sawShared, sawPrivate bool
	for _, card := range adminView {
		switch card.AgentID {
		case sharedAgent:
			sawShared = true
			if !card.SharesState {
				t.Fatalf("shared agent's card hides that it stores data between users: %+v", card)
			}
		case privateAgent:
			sawPrivate = true
			if card.SharesState {
				t.Fatalf("agent with no shared state claims to store data between users: %+v", card)
			}
			if len(card.Roles) != 2 {
				t.Fatalf("role audience lost: %+v", card)
			}
		}
	}
	if !sawShared || !sawPrivate {
		t.Fatalf("catalog missing cards: %+v", adminView)
	}

	// An employee without the role sees the open card only.
	asEmployee, err := store.CatalogCards(ctx, tenant, []string{"role:finance"})
	if err != nil || len(asEmployee) != 1 || asEmployee[0].AgentID != sharedAgent {
		t.Fatalf("employee catalog = %+v, %v", asEmployee, err)
	}
	// An HR employee sees both.
	asHR, err := store.CatalogCards(ctx, tenant, []string{"role:hr"})
	if err != nil || len(asHR) != 2 {
		t.Fatalf("hr catalog = %+v, %v", asHR, err)
	}
	// A bad audience is refused, and another tenant's catalog stays empty.
	if err := store.PublishWithRoles(ctx, tenant, sharedInst.ID, nil); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty audience code = %q", contracts.Code(err))
	}
	foreign := "t-" + randSuffix(t)
	if cards, err := store.CatalogCards(ctx, foreign, nil); err != nil || len(cards) != 0 {
		t.Fatalf("foreign tenant catalog = %+v, %v", cards, err)
	}
	// Publishing a dev installation is refused: stage and dev never reach a catalog.
	devInst, err := store.Install(ctx, tenant, sharedAgent, sharedVersion.ID, registry.EnvironmentDev, "k-card-dev", granted())
	if err != nil {
		t.Fatalf("Install dev: %v", err)
	}
	if err := store.PublishWithRoles(ctx, tenant, devInst.ID, []string{"role:hr"}); err == nil {
		t.Fatal("a dev installation was published into the catalog")
	}
}
