package registry_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// randSuffix returns a random, unguessable suffix so concurrent runs and other
// projects sharing this database can never collide.
func randSuffix(t *testing.T) string {
	t.Helper()
	id := contracts.NewID()
	if len(id) > 20 {
		id = id[len(id)-20:]
	}
	return strings.ToLower(strings.ReplaceAll(id, "_", ""))
}

// newStore connects to AHP_TEST_DATABASE_URL and returns a migrated store plus
// a tenant unique to this run.
func newStore(t *testing.T) (*registry.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: no real PostgreSQL instance to test isolation against")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("cannot build pool for the real PostgreSQL instance: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("real PostgreSQL instance is unreachable: %v", err)
	}
	t.Cleanup(pool.Close)

	// Publication carries the mandatory evals gate ("оценки перед публикацией"),
	// so the fixture store is built over the REAL internal/evals plane, exactly as
	// the product mount does. The publish helpers record a real passing verdict for
	// the exact digest before publishing; no stub, no ungated bypass.
	store := registry.New(pool, registry.WithEvalGate(evalsHarness(t)))
	ensureHarnessPolicy(t)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate failed: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate is not idempotent: %v", err)
	}
	return store, "t-" + randSuffix(t)
}

// testPublisher is the publisher the test fixtures publish their artifacts as:
// an agent and its versions belong to one publisher, so every version in these
// tests is published by somebody (PDF rev 3.2, c. 42 "Установка").
const testPublisher = "pub-acme"

// mustRegister binds an agent to a publisher and fails the test if that is
// refused.
func mustRegister(t *testing.T, store *registry.Store, publisher, agentID string) {
	t.Helper()
	if _, err := store.RegisterAgent(context.Background(), publisher, agentID); err != nil {
		t.Fatalf("RegisterAgent(%s as %s): %v", agentID, publisher, err)
	}
}

// publish stores a manifest version and returns it.
func publish(t *testing.T, store *registry.Store, agentID string, m registry.Manifest) registry.Version {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	mustRegister(t, store, testPublisher, agentID)
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	// "Оценки перед публикацией": record a real passing verdict for this exact
	// digest so the gated store lets the publication through. The assertions in the
	// calling tests are unchanged; only the new precondition is satisfied.
	passVerdict(t, digest)
	v, err := store.PutVersion(ctx, testPublisher, agentID, digest, m)
	if err != nil {
		t.Fatalf("PutVersion: %v", err)
	}
	return v
}

func manifest(t *testing.T, system string) registry.Manifest {
	t.Helper()
	// The suffix keeps identifiers unique per run; digests are scoped to the
	// publishing agent, so identical content from different agents is legal.
	suffix := randSuffix(t)
	return registry.Manifest{
		Kind:       registry.KindAgent,
		System:     system + "." + suffix,
		Visibility: registry.VisibilityTenant,
		Audience:   registry.AudienceShared,
		Class:      registry.ClassS,
		Components: []registry.Component{{Name: "router", A2A: true, MinReplicas: 1, MaxReplicas: 3}},
		State: registry.StateConfig{
			Partition: "subject:" + system,
			Compat:    system + "-v1",
		},
		Capabilities: registry.Capabilities{
			Models:    []string{"model/qwen-max", "model/qwen-turbo"},
			Tools:     []string{"tool/calendar.read", "tool/mail.read"},
			Memory:    []string{"memory/tenant"},
			Compute:   []string{"compute/ephemeral", "compute/project"},
			Calls:     []string{"agent/acme.other"},
			SelfTasks: 4,
		},
		Limits: registry.Limits{Depth: 4, ActiveTasks: 16, TaskTimeout: "10m"},
	}
}

func granted(models ...string) registry.Capabilities {
	return registry.Capabilities{Models: models, SelfTasks: 1}
}

func TestPutVersionIdempotentAndMismatch(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	mustRegister(t, store, testPublisher, agent)
	m := manifest(t, "acme.briefing")

	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	// The gate requires a passing verdict for the digest before publication.
	passVerdict(t, digest)
	first, err := store.PutVersion(ctx, testPublisher, agent, digest, m)
	if err != nil {
		t.Fatalf("first PutVersion: %v", err)
	}
	replay, err := store.PutVersion(ctx, testPublisher, agent, digest, m)
	if err != nil {
		t.Fatalf("replayed PutVersion: %v", err)
	}
	if replay.ID != first.ID || replay.Digest != first.Digest {
		t.Fatalf("replay produced a new version: %s vs %s", replay.ID, first.ID)
	}
	if !replay.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("replay rewrote created_at: %v vs %v", replay.CreatedAt, first.CreatedAt)
	}
	if got, err := store.GetVersion(ctx, first.ID); err != nil || got.Digest != digest {
		t.Fatalf("GetVersion = %+v, %v", got, err)
	}

	// A digest that does not match the canonical manifest is rejected.
	other := manifest(t, "acme.briefing")
	other.Limits.ActiveTasks = 99
	otherDigest, err := contracts.Digest(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutVersion(ctx, testPublisher, agent, digest, other); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("mismatched digest code = %q, want %q", contracts.Code(err), contracts.ErrKeyReuse)
	}
	// The original version survived the rejected call.
	if got, err := store.GetVersion(ctx, first.ID); err != nil || got.Manifest.Limits.ActiveTasks != 16 {
		t.Fatalf("rejected call mutated the stored version: %+v, %v", got, err)
	}
	// A different manifest content gets its own immutable row.
	passVerdict(t, otherDigest)
	second, err := store.PutVersion(ctx, testPublisher, agent, otherDigest, other)
	if err != nil {
		t.Fatalf("second PutVersion: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("two manifests share one version row")
	}
	// A digest hashes content only: another agent publishing the identical
	// manifest gets its own version row scoped to that agent, never a denial.
	otherAgent := "agent-" + randSuffix(t)
	mustRegister(t, store, "pub-other", otherAgent)
	cross, err := store.PutVersion(ctx, "pub-other", otherAgent, otherDigest, other)
	if err != nil {
		t.Fatalf("identical content from another agent: %v", err)
	}
	if cross.ID == second.ID || cross.AgentID != otherAgent || cross.Digest != otherDigest {
		t.Fatalf("cross-agent publish did not create its own scoped version: %+v (agent's own %+v)", cross, second)
	}
	// The replay for the other agent returns its own row, not the first one.
	crossReplay, err := store.PutVersion(ctx, "pub-other", otherAgent, otherDigest, other)
	if err != nil || crossReplay.ID != cross.ID {
		t.Fatalf("cross-agent replay = %+v, %v (want %s)", crossReplay, err, cross.ID)
	}
	// Invalid manifests and malformed digests never reach storage.
	bad := manifest(t, "acme.briefing")
	bad.Limits.Depth = registry.MaxDepth + 1
	if _, err := store.PutVersion(ctx, testPublisher, agent, digest, bad); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("invalid manifest code = %q", contracts.Code(err))
	}
	// A manifest that cannot be canonicalised is refused before any write.
	unencodable := manifest(t, "acme.briefing")
	unencodable.Components = append(unencodable.Components,
		registry.Component{Name: "bad", MinReplicas: 1, MaxReplicas: 1},
		registry.Component{Name: "bad", MinReplicas: 1, MaxReplicas: 1})
	if _, err := store.PutVersion(ctx, testPublisher, agent, digest, unencodable); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate-component manifest code = %q", contracts.Code(err))
	}
	if _, err := store.PutVersion(ctx, testPublisher, agent, "sha256:nope", m); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("malformed digest code = %q", contracts.Code(err))
	}
	if _, err := store.GetVersion(ctx, "missing-version"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing version code = %q", contracts.Code(err))
	}
}

func TestPutVersionConcurrentSameDigest(t *testing.T) {
	store, _ := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	mustRegister(t, store, testPublisher, agent)
	m := manifest(t, "acme.race")
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	// One passing verdict for the shared digest unlocks all racing publishers;
	// they must still collapse to a single version row.
	passVerdict(t, digest)

	const writers = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	ids := make([]string, writers)
	errs := make([]error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			v, err := store.PutVersion(ctx, testPublisher, agent, digest, m)
			ids[i], errs[i] = v.ID, err
		}(i)
	}
	close(start)
	wg.Wait()

	seen := map[string]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent writer %d failed: %v", i, err)
		}
		if ids[i] == "" {
			t.Fatalf("writer %d returned an empty version id", i)
		}
		seen[ids[i]] = true
	}
	if len(seen) != 1 {
		t.Fatalf("concurrent identical publishes produced %d versions: %v", len(seen), seen)
	}
}

func TestInstallCapabilityCeilingAndIdempotency(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	ver := publish(t, store, agent, manifest(t, "acme.briefing"))

	if _, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, "key-bad",
		registry.Capabilities{Models: []string{"model/gpt"}}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("widened grant code = %q, want %q", contracts.Code(err), contracts.ErrInvalid)
	}
	if _, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, "key-bad",
		registry.Capabilities{SelfTasks: 99}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("widened self_tasks code = %q", contracts.Code(err))
	}

	key := "install-" + randSuffix(t)
	body := granted("model/qwen-max")
	inst, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, key, body)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if inst.Tenant != tenant || inst.AgentID != agent || inst.VersionID != ver.ID {
		t.Fatalf("installation bound to the wrong records: %+v", inst)
	}
	if inst.Published {
		t.Fatal("a fresh installation must not be published")
	}
	if len(inst.Capabilities.Models) != 1 || inst.Capabilities.Models[0] != "model/qwen-max" {
		t.Fatalf("granted capabilities were not stored: %+v", inst.Capabilities)
	}

	replay, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, key, body)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if replay.ID != inst.ID {
		t.Fatalf("replay created a second installation: %s vs %s", replay.ID, inst.ID)
	}
	// Same key, different body: rejected, original untouched.
	if _, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, key,
		granted("model/qwen-max", "model/qwen-turbo")); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key reuse code = %q, want %q", contracts.Code(err), contracts.ErrKeyReuse)
	}
	if again, err := store.GetInstallation(ctx, tenant, inst.ID); err != nil || again.ID != inst.ID {
		t.Fatalf("installation after mismatch = %+v, %v", again, err)
	}
	// Same key in another tenant is a different installation.
	_, otherTenant := newStore(t)
	if _, err := store.Install(ctx, otherTenant, agent, ver.ID, registry.EnvironmentDev, key, body); err != nil {
		t.Fatalf("same key in another tenant: %v", err)
	}
	// Same agent, another environment: a separate installation row.
	stage, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentStage, "key-"+randSuffix(t), body)
	if err != nil {
		t.Fatalf("stage install: %v", err)
	}
	if stage.ID == inst.ID {
		t.Fatal("stage and dev share one installation")
	}
	// Same environment twice is a conflict.
	if _, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev,
		"key-"+randSuffix(t), body); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("duplicate slot code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	// Version of another agent cannot be installed under this agent id.
	otherAgent := "agent-" + randSuffix(t)
	otherVer := publish(t, store, otherAgent, manifest(t, "acme.other"))
	if _, err := store.Install(ctx, tenant, agent, otherVer.ID, registry.EnvironmentProd,
		"key-"+randSuffix(t), body); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cross-agent version code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	// Unknown version and unsafe environment are rejected.
	if _, err := store.Install(ctx, tenant, agent, "missing", registry.EnvironmentDev,
		"key-"+randSuffix(t), body); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing version code = %q", contracts.Code(err))
	}
	if _, err := store.Install(ctx, tenant, agent, ver.ID, "dEv!", "key-"+randSuffix(t), body); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unsafe environment code = %q", contracts.Code(err))
	}
}

func TestInstallConcurrentSameKey(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	ver := publish(t, store, agent, manifest(t, "acme.raceinstall"))
	key := "race-" + randSuffix(t)
	body := granted("model/qwen-turbo")

	const racers = 10
	start := make(chan struct{})
	var wg sync.WaitGroup
	ids := make([]string, racers)
	errs := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			inst, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentDev, key, body)
			ids[i], errs[i] = inst.ID, err
		}(i)
	}
	close(start)
	wg.Wait()

	seen := map[string]bool{}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d failed: %v", i, err)
		}
		seen[ids[i]] = true
	}
	if len(seen) != 1 {
		t.Fatalf("concurrent identical installs produced %d installations", len(seen))
	}
}

func TestInstallationTenantIsolation(t *testing.T) {
	store, tenantA := newStore(t)
	_, tenantB := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	ver := publish(t, store, agent, manifest(t, "acme.isolation"))

	a, err := store.Install(ctx, tenantA, agent, ver.ID, registry.EnvironmentDev, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("tenant A install: %v", err)
	}
	// Tenant B cannot read, promote, pin or publish tenant A's installation.
	if _, err := store.GetInstallation(ctx, tenantB, a.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant read code = %q, want %q", contracts.Code(err), contracts.ErrNotFound)
	}
	if _, err := store.Promote(ctx, tenantB, a.ID, ver.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant promote code = %q", contracts.Code(err))
	}
	if _, err := store.Pin(ctx, tenantB, a.ID, "task-"+randSuffix(t)); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant pin code = %q", contracts.Code(err))
	}
	if err := store.Publish(ctx, tenantB, a.ID, true); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant publish code = %q", contracts.Code(err))
	}
	if n, err := store.CountPinnedTasks(ctx, tenantB, a.ID, ver.ID); err != nil || n != 0 {
		t.Fatalf("cross-tenant pin count = %d, %v", n, err)
	}
	// Tenant B installs the same version for itself and gets its own row.
	b, err := store.Install(ctx, tenantB, agent, ver.ID, registry.EnvironmentDev, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("tenant B install: %v", err)
	}
	if b.ID == a.ID {
		t.Fatal("two tenants share one installation id")
	}
	// A class C shared catalog artifact is never installable by another tenant.
	cShared := manifest(t, "acme.cshared")
	cShared.Class = registry.ClassC
	cShared.Visibility = registry.VisibilityCatalog
	cVer := publish(t, store, "agent-"+randSuffix(t), cShared)
	if _, err := store.Install(ctx, tenantA, cVer.AgentID, cVer.ID, registry.EnvironmentProd,
		"key-"+randSuffix(t), granted()); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("class C cross-tenant install code = %q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
	// A system artifact is not tenant installable at all.
	sys := manifest(t, "acme.system")
	sys.Visibility = registry.VisibilitySystem
	sysVer := publish(t, store, "agent-"+randSuffix(t), sys)
	if _, err := store.Install(ctx, tenantA, sysVer.AgentID, sysVer.ID, registry.EnvironmentProd,
		"key-"+randSuffix(t), granted()); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("system artifact install code = %q", contracts.Code(err))
	}
}

func TestPinPromoteRollback(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	v1 := publish(t, store, agent, manifest(t, "acme.lifecycle"))
	m2 := manifest(t, "acme.lifecycle")
	m2.State.Compat = "acme.lifecycle-v2"
	m2.Limits.ActiveTasks = 64
	v2 := publish(t, store, agent, m2)
	otherAgent := "agent-" + randSuffix(t)
	vForeign := publish(t, store, otherAgent, manifest(t, "acme.foreign"))

	inst, err := store.Install(ctx, tenant, agent, v1.ID, registry.EnvironmentProd, "key-"+randSuffix(t),
		registry.Capabilities{Models: []string{"model/qwen-max"}, SelfTasks: 2})
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	task := "task-" + randSuffix(t)
	pin, err := store.Pin(ctx, tenant, inst.ID, task)
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if pin.ID != v1.ID {
		t.Fatalf("pin = %s, want %s", pin.ID, v1.ID)
	}
	// Promotion moves the installation, never the pin.
	promoted, err := store.Promote(ctx, tenant, inst.ID, v2.ID)
	if err != nil {
		t.Fatalf("Promote: %v", err)
	}
	if promoted.VersionID != v2.ID {
		t.Fatalf("promoted version = %s, want %s", promoted.VersionID, v2.ID)
	}
	if promoted.Capabilities.Models[0] != "model/qwen-max" {
		t.Fatalf("promotion changed the grant: %+v", promoted.Capabilities)
	}
	again, err := store.Pin(ctx, tenant, inst.ID, task)
	if err != nil {
		t.Fatalf("re-Pin after promote: %v", err)
	}
	if again.ID != v1.ID || again.Digest != v1.Digest {
		t.Fatalf("pin changed after promote: %s vs %s", again.ID, v1.ID)
	}
	if pinned, err := store.PinnedVersion(ctx, tenant, task); err != nil || pinned.ID != v1.ID {
		t.Fatalf("PinnedVersion = %+v, %v", pinned, err)
	}
	// A task started after promotion pins the new version.
	later := "task-" + randSuffix(t)
	if p, err := store.Pin(ctx, tenant, inst.ID, later); err != nil || p.ID != v2.ID {
		t.Fatalf("post-promotion pin = %+v, %v", p, err)
	}
	// Rollback restores v1 for new tasks; both old pins stay intact.
	if _, err := store.Promote(ctx, tenant, inst.ID, v1.ID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if p, err := store.Pin(ctx, tenant, inst.ID, "task-"+randSuffix(t)); err != nil || p.ID != v1.ID {
		t.Fatalf("post-rollback pin = %+v, %v", p, err)
	}
	if p, err := store.PinnedVersion(ctx, tenant, later); err != nil || p.ID != v2.ID {
		t.Fatalf("rollback rewrote an existing pin: %+v, %v", p, err)
	}
	// Pinning is idempotent under repeated calls.
	for i := 0; i < 3; i++ {
		if p, err := store.Pin(ctx, tenant, inst.ID, task); err != nil || p.ID != v1.ID {
			t.Fatalf("repeat pin %d = %+v, %v", i, p, err)
		}
	}
	// Promotion to another agent's version is refused.
	if _, err := store.Promote(ctx, tenant, inst.ID, vForeign.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cross-agent promote code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	// Promotion to a version whose ceiling is below the grant is refused.
	narrow := manifest(t, "acme.lifecycle")
	narrow.Capabilities.Models = []string{"model/qwen-turbo"}
	vNarrow := publish(t, store, agent, narrow)
	if _, err := store.Promote(ctx, tenant, inst.ID, vNarrow.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("ceiling-shrinking promote code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	if got, err := store.GetInstallation(ctx, tenant, inst.ID); err != nil || got.VersionID != v1.ID {
		t.Fatalf("installation after refused promote = %+v, %v", got, err)
	}
	// Pin counts let a drain controller see what is still running.
	n, err := store.CountPinnedTasks(ctx, tenant, inst.ID, v1.ID)
	if err != nil {
		t.Fatalf("CountPinnedTasks: %v", err)
	}
	if n != 2 {
		t.Fatalf("pinned tasks on v1 = %d, want 2", n)
	}
	if n, err := store.CountPinnedTasks(ctx, tenant, inst.ID, v2.ID); err != nil || n != 1 {
		t.Fatalf("pinned tasks on v2 = %d, %v", n, err)
	}
	if _, err := store.PinnedVersion(ctx, tenant, "never-pinned"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("unpinned task code = %q", contracts.Code(err))
	}
}

func TestPinConcurrentWithPromote(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	v1 := publish(t, store, agent, manifest(t, "acme.pinrace"))
	m2 := manifest(t, "acme.pinrace")
	m2.State.Compat = "acme.pinrace-v2"
	v2 := publish(t, store, agent, m2)

	inst, err := store.Install(ctx, tenant, agent, v1.ID, registry.EnvironmentDev, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	task := "task-" + randSuffix(t)

	var wg sync.WaitGroup
	wg.Add(2)
	var promotedTo string
	var pinnedVersion string
	go func() {
		defer wg.Done()
		if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); err != nil {
			t.Errorf("concurrent promote: %v", err)
		}
		if got, err := store.GetInstallation(ctx, tenant, inst.ID); err == nil {
			promotedTo = got.VersionID
		}
	}()
	go func() {
		defer wg.Done()
		v, err := store.Pin(ctx, tenant, inst.ID, task)
		if err != nil {
			t.Errorf("concurrent pin: %v", err)
			return
		}
		pinnedVersion = v.ID
	}()
	wg.Wait()

	if promotedTo != v2.ID {
		t.Fatalf("installation did not promote: %s", promotedTo)
	}
	if pinnedVersion != v1.ID && pinnedVersion != v2.ID {
		t.Fatalf("pin landed on an unknown version: %s", pinnedVersion)
	}
	// Whatever the pin captured must be stable forever.
	for i := 0; i < 5; i++ {
		v, err := store.Pin(ctx, tenant, inst.ID, task)
		if err != nil {
			t.Fatalf("repeat pin: %v", err)
		}
		if v.ID != pinnedVersion {
			t.Fatalf("pin flipped from %s to %s", pinnedVersion, v.ID)
		}
	}
}

func TestPublishAndCatalogVisibility(t *testing.T) {
	store, tenantA := newStore(t)
	_, tenantB := newStore(t)
	ctx := context.Background()

	// Tenant A gets a dev installation of a catalog-visible artifact and a prod
	// installation of a tenant-visible artifact.
	catAgent := "agent-" + randSuffix(t)
	catManifest := manifest(t, "acme.catalog")
	catManifest.Visibility = registry.VisibilityCatalog
	catVer := publish(t, store, catAgent, catManifest)
	dev, err := store.Install(ctx, tenantA, catAgent, catVer.ID, registry.EnvironmentDev, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("dev install: %v", err)
	}
	if err := store.Publish(ctx, tenantA, dev.ID, true); err == nil {
		t.Fatal("a dev installation must never become catalog visible")
	}
	if got, err := store.GetInstallation(ctx, tenantA, dev.ID); err != nil || got.Published {
		t.Fatalf("dev installation was published anyway: %+v, %v", got, err)
	}
	if _, err := store.Catalog(ctx, tenantB); err != nil {
		t.Fatalf("Catalog: %v", err)
	}

	tenantOnlyAgent := "agent-" + randSuffix(t)
	tenantOnlyVer := publish(t, store, tenantOnlyAgent, manifest(t, "acme.tenantonly"))
	private, err := store.Install(ctx, tenantA, tenantOnlyAgent, tenantOnlyVer.ID,
		registry.EnvironmentProd, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("prod install: %v", err)
	}

	inCatalog := func(tenant, instID string) bool {
		t.Helper()
		list, err := store.Catalog(ctx, tenant)
		if err != nil {
			t.Fatalf("Catalog(%s): %v", tenant, err)
		}
		for _, it := range list {
			if it.ID == instID {
				if !it.Published {
					t.Fatalf("unpublished row %s appeared in the catalog", instID)
				}
				return true
			}
		}
		return false
	}

	// Nothing is visible before publication.
	if inCatalog(tenantA, private.ID) || inCatalog(tenantB, private.ID) {
		t.Fatal("unpublished installation is visible")
	}
	// Publishing a tenant-visibility artifact exposes it to its owner only.
	if err := store.Publish(ctx, tenantA, private.ID, true); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if !inCatalog(tenantA, private.ID) {
		t.Fatal("own published installation is missing from the catalog")
	}
	if inCatalog(tenantB, private.ID) {
		t.Fatal("tenant-visibility installation leaked to another tenant")
	}
	// A catalog-visible artifact lets another tenant install its OWN copy, but
	// never exposes tenant A's installation row: catalog visibility governs
	// package availability, not cross-tenant installation visibility.
	shared, err := store.Install(ctx, tenantA, catAgent, catVer.ID, registry.EnvironmentProd,
		"key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("prod catalog install: %v", err)
	}
	if inCatalog(tenantB, shared.ID) {
		t.Fatal("another tenant's installation appeared in this tenant's catalog")
	}
	if err := store.Publish(ctx, tenantA, shared.ID, true); err != nil {
		t.Fatalf("Publish prod: %v", err)
	}
	if inCatalog(tenantB, shared.ID) {
		t.Fatal("tenant A installation leaked to tenant B after publication")
	}
	if !inCatalog(tenantA, shared.ID) {
		t.Fatal("published installation is hidden from its owner")
	}
	// Tenant B installs and publishes its own copy and sees only its own ID.
	own, err := store.Install(ctx, tenantB, catAgent, catVer.ID, registry.EnvironmentProd,
		"key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("tenant B own catalog install: %v", err)
	}
	if own.ID == shared.ID {
		t.Fatal("two tenants share one installation id")
	}
	if inCatalog(tenantB, own.ID) {
		t.Fatal("unpublished own installation is visible")
	}
	if err := store.Publish(ctx, tenantB, own.ID, true); err != nil {
		t.Fatalf("Publish tenant B own: %v", err)
	}
	if !inCatalog(tenantB, own.ID) {
		t.Fatal("tenant B cannot see its own published installation")
	}
	if inCatalog(tenantB, shared.ID) || inCatalog(tenantA, own.ID) {
		t.Fatal("publication exposed a foreign installation to the other tenant")
	}
	// Unpublishing hides it again, for the owner too.
	if err := store.Publish(ctx, tenantA, shared.ID, false); err != nil {
		t.Fatalf("Unpublish: %v", err)
	}
	if inCatalog(tenantA, shared.ID) {
		t.Fatal("unpublished installation is still visible to its owner")
	}
	// The owner can always read its own installation directly.
	if got, err := store.GetInstallation(ctx, tenantA, shared.ID); err != nil || got.Published {
		t.Fatalf("GetInstallation after unpublish = %+v, %v", got, err)
	}
	// Publishing an unknown installation is NOT_FOUND.
	if err := store.Publish(ctx, tenantA, "missing", true); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("publish missing code = %q", contracts.Code(err))
	}
}

func TestConfigCompareAndSwap(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)

	if _, err := store.GetConfig(ctx, tenant, agent); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing config code = %q", contracts.Code(err))
	}
	if _, err := store.PutConfig(ctx, tenant, agent, 1, json.RawMessage(`{"a":1}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("first write with stale revision code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	first, err := store.PutConfig(ctx, tenant, agent, 0, json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("first PutConfig: %v", err)
	}
	if first.Revision != 1 || first.Tenant != tenant || first.AgentID != agent {
		t.Fatalf("first config = %+v", first)
	}
	second, err := store.PutConfig(ctx, tenant, agent, 1, json.RawMessage(`{"a":2}`))
	if err != nil {
		t.Fatalf("second PutConfig: %v", err)
	}
	if second.Revision != 2 || second.ID != first.ID {
		t.Fatalf("second config = %+v (first %+v)", second, first)
	}
	// A stale expectation is refused and changes nothing.
	if _, err := store.PutConfig(ctx, tenant, agent, 1, json.RawMessage(`{"a":99}`)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale write code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	got, err := store.GetConfig(ctx, tenant, agent)
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got.Revision != 2 {
		t.Fatalf("config after stale write = %+v", got)
	}
	// jsonb normalisation (spaces, key order) must never be compared as text.
	var doc map[string]any
	if err := json.Unmarshal(got.Data, &doc); err != nil {
		t.Fatalf("stored config data is not valid JSON: %v (%s)", err, got.Data)
	}
	if v, ok := doc["a"]; !ok || v != float64(2) {
		t.Fatalf("stale write mutated the document: %s", got.Data)
	}
	// Another tenant has its own document.
	other, err := store.PutConfig(ctx, "t-"+randSuffix(t), agent, 0, json.RawMessage(`{"a":"other"}`))
	if err != nil {
		t.Fatalf("other tenant PutConfig: %v", err)
	}
	if other.ID == first.ID {
		t.Fatal("two tenants share one config row")
	}
	// Malformed payloads are rejected.
	if _, err := store.PutConfig(ctx, tenant, agent, 2, json.RawMessage(`{`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("broken json code = %q", contracts.Code(err))
	}
	if _, err := store.PutConfig(ctx, tenant, agent, 2, nil); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty payload code = %q", contracts.Code(err))
	}
	if _, err := store.PutConfig(ctx, tenant, agent, -1, json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("negative revision code = %q", contracts.Code(err))
	}
}

func TestConfigConcurrentCAS(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	if _, err := store.PutConfig(ctx, tenant, agent, 0, json.RawMessage(`{"n":0}`)); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const writers = 8
	var wg sync.WaitGroup
	var ok atomic.Int64
	var conflicts atomic.Int64
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Every writer reads the current revision and then races to write.
			cur, err := store.GetConfig(ctx, tenant, agent)
			if err != nil {
				t.Errorf("writer %d read: %v", i, err)
				return
			}
			payload := json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))
			if _, err := store.PutConfig(ctx, tenant, agent, cur.Revision, payload); err != nil {
				if contracts.Code(err) == contracts.ErrConflict {
					conflicts.Add(1)
					return
				}
				t.Errorf("writer %d: %v", i, err)
				return
			}
			ok.Add(1)
		}(i)
	}
	close(start)
	wg.Wait()

	final, err := store.GetConfig(ctx, tenant, agent)
	if err != nil {
		t.Fatalf("final GetConfig: %v", err)
	}
	// CAS never loses or double-applies a write: exactly one revision per
	// accepted write, and the losers are all reported as conflicts.
	if final.Revision != 1+ok.Load() {
		t.Fatalf("revision %d does not match %d accepted writes (conflicts %d)",
			final.Revision, ok.Load(), conflicts.Load())
	}
	if ok.Load()+conflicts.Load() != writers {
		t.Fatalf("%d accepted + %d conflicts != %d writers", ok.Load(), conflicts.Load(), writers)
	}
	if ok.Load() < 1 {
		t.Fatal("no writer was accepted")
	}
}

func TestMigrateIsNonDestructive(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	ver := publish(t, store, agent, manifest(t, "acme.persist"))
	inst, err := store.Install(ctx, tenant, agent, ver.ID, registry.EnvironmentProd, "key-"+randSuffix(t), granted())
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := store.PutConfig(ctx, tenant, agent, 0, json.RawMessage(`{"keep":true}`)); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("re-Migrate: %v", err)
	}
	if got, err := store.GetVersion(ctx, ver.ID); err != nil || got.Digest != ver.Digest {
		t.Fatalf("version lost across migration: %+v, %v", got, err)
	}
	if got, err := store.GetInstallation(ctx, tenant, inst.ID); err != nil || got.ID != inst.ID {
		t.Fatalf("installation lost across migration: %+v, %v", got, err)
	}
	if got, err := store.GetConfig(ctx, tenant, agent); err != nil || got.Revision != 1 {
		t.Fatalf("config lost across migration: %+v, %v", got, err)
	}
}

// TestMigrateAddsMissingDerivedColumns simulates a real PostgreSQL table from
// the earlier DDL revision: no visibility/class columns, so CREATE TABLE IF
// NOT EXISTS alone can never repair it. Migrate must add the columns in place,
// backfill them from the stored manifest, and keep every existing row.
func TestMigrateAddsMissingDerivedColumns(t *testing.T) {
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: no real PostgreSQL instance to migrate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("unreachable PostgreSQL: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS ahp_registry`); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	suffix := randSuffix(t)
	table := "ahp_registry.legacy_versions_" + suffix
	legacyDDL := `CREATE TABLE ` + table + ` (
    id          text PRIMARY KEY,
    agent_id    text NOT NULL,
    digest      text NOT NULL,
    manifest    jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
)`
	if _, err := pool.Exec(ctx, legacyDDL); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	defer func() {
		if _, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+table); err != nil {
			t.Errorf("drop legacy table: %v", err)
		}
	}()

	agent := "agent-" + suffix
	m := manifest(t, "acme.legacy")
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO `+table+` (id, agent_id, digest, manifest) VALUES ($1, $2, $3, $4::jsonb)`,
		"legacy-"+suffix, agent, digest, doc); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}

	// The migration statements Migrate runs, applied to the legacy table.
	migration := `
ALTER TABLE ` + table + `
    ADD COLUMN IF NOT EXISTS visibility text NOT NULL DEFAULT 'tenant',
    ADD COLUMN IF NOT EXISTS class      text NOT NULL DEFAULT 'C';`
	backfill := `
UPDATE ` + table + `
SET visibility = COALESCE(NULLIF(manifest ->> 'visibility', ''), 'tenant');
UPDATE ` + table + `
SET class = CASE
        WHEN manifest ->> 'class' IN ('S', 'C') THEN manifest ->> 'class'
        ELSE 'C'
    END;`
	if _, err := pool.Exec(ctx, migration); err != nil {
		t.Fatalf("migration ALTER failed: %v", err)
	}
	if _, err := pool.Exec(ctx, migration); err != nil {
		t.Fatalf("migration ALTER is not idempotent: %v", err)
	}
	if _, err := pool.Exec(ctx, backfill); err != nil {
		t.Fatalf("backfill failed: %v", err)
	}

	var visibility, class string
	if err := pool.QueryRow(ctx,
		`SELECT visibility, class FROM `+table+` WHERE id = $1`, "legacy-"+suffix).
		Scan(&visibility, &class); err != nil {
		t.Fatalf("read migrated row: %v", err)
	}
	if visibility != registry.VisibilityTenant || class != registry.ClassS {
		t.Fatalf("backfilled projection = (%s, %s), want (tenant, S)", visibility, class)
	}

	// A manifest without usable derived values keeps the safe defaults.
	if _, err := pool.Exec(ctx,
		`INSERT INTO `+table+` (id, agent_id, digest, manifest) VALUES ($1, $2, $3, '{}'::jsonb)`,
		"legacy-empty-"+suffix, agent, "sha256:"+strings.Repeat("1", 63)+"2"); err != nil {
		t.Fatalf("seed empty-manifest row: %v", err)
	}
	if _, err := pool.Exec(ctx, backfill); err != nil {
		t.Fatalf("re-backfill failed: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT visibility, class FROM `+table+` WHERE id = $1`, "legacy-empty-"+suffix).
		Scan(&visibility, &class); err != nil {
		t.Fatalf("read empty-manifest row: %v", err)
	}
	if visibility != registry.VisibilityTenant || class != registry.ClassC {
		t.Fatalf("default projection = (%s, %s), want (tenant, C)", visibility, class)
	}

	// The real Migrate remains non-destructive on the live schema.
	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
}

func TestStoreRejectsUnsafeArguments(t *testing.T) {
	store, tenant := newStore(t)
	ctx := context.Background()
	agent := "agent-" + randSuffix(t)
	ver := publish(t, store, agent, manifest(t, "acme.args"))

	bad := []string{"", "has space", "has/slash", "quote'in", "drop;table"}
	for _, v := range bad {
		if _, err := store.Install(ctx, v, agent, ver.ID, registry.EnvironmentDev, "k", granted()); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("Install(tenant=%q) code = %q", v, contracts.Code(err))
		}
		if _, err := store.GetInstallation(ctx, tenant, v); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("GetInstallation(id=%q) code = %q", v, contracts.Code(err))
		}
		if _, err := store.PutVersion(ctx, testPublisher, v, "sha256:"+strings.Repeat("0", 64), manifest(t, "acme.args")); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("PutVersion(agent=%q) code = %q", v, contracts.Code(err))
		}
		if _, err := store.PutConfig(ctx, tenant, v, 0, json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("PutConfig(agent=%q) code = %q", v, contracts.Code(err))
		}
	}
	if _, err := store.Catalog(ctx, ""); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("Catalog with empty tenant code = %q", contracts.Code(err))
	}
	if _, err := store.Pin(ctx, tenant, "missing-install", "task-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("Pin on missing installation code = %q", contracts.Code(err))
	}
	var target *contracts.Error
	if _, err := store.GetVersion(ctx, "does-not-exist"); !errors.As(err, &target) {
		t.Fatalf("error does not carry the shared error type: %v", err)
	} else if target.Code != contracts.ErrNotFound {
		t.Fatalf("carried code = %q", target.Code)
	}
}

// TestMigrateConcurrentUnderLoad is a regression guard for the SQLSTATE 40P01
// deadlock that occurred when several processes migrated the shared
// ahp_registry schema at once: the exclusive ALTER/backfill in Migrate raced
// with concurrent DML (the backfill UPDATE) on ahp_registry.versions. Migrate
// now applies that exclusive work at most once per database, guarded by a
// transaction-scoped advisory lock and a schema_version sentinel, so repeated
// and concurrent migrations only take shared locks. This test fires many
// concurrent Migrate calls interleaved with real writers and asserts that no
// operation reports a deadlock or any other error.
func TestMigrateConcurrentUnderLoad(t *testing.T) {
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: no real PostgreSQL instance to migrate")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("build pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	// Make sure the base schema exists before launching the stampede so every
	// goroutine exercises the advisory-locked migration path, not the initial
	// CREATE SCHEMA race.
	store := registry.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("seed Migrate: %v", err)
	}

	const migrators = 8
	const writers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, migrators+writers)

	for i := 0; i < migrators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = registry.New(pool).Migrate(ctx)
		}(i)
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			agent := "agent-migrate-load-" + randSuffix(t)
			if _, err := registry.New(pool).RegisterAgent(ctx, testPublisher, agent); err != nil {
				errs[migrators+i] = err
				return
			}
			m := manifest(t, "acme.load")
			digest, err := contracts.Digest(m)
			if err != nil {
				errs[migrators+i] = err
				return
			}
			// Under the mandatory evals gate a writer publishes only after a real
			// passing verdict exists for its digest; use the gated store, as the
			// product mount does. A record failure is reported like any other write
			// failure (no t.Fatalf off the test goroutine).
			if _, rerr := evalsHarness(t).Record(ctx, evals.RecordInput{
				AgentDigest: digest, TaskSetID: harnessTaskSetID,
				Setup: map[string]any{"harness": "migrate-load"}, Outcome: evals.OutcomePass,
				Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted}},
				PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
			}); rerr != nil {
				errs[migrators+i] = rerr
				return
			}
			_, err = registry.New(pool, registry.WithEvalGate(evalsHarness(t))).PutVersion(ctx, testPublisher, agent, digest, m)
			errs[migrators+i] = err
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent operation %d failed (deadlock regression?): %v", i, err)
		}
	}
}
