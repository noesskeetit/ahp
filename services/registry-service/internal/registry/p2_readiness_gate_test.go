package registry_test

// П2 (root-audit 20261109, Registry/Deploy): the pipeline probes a runtime's
// readiness BEFORE traffic is switched onto its version, and switches back by
// itself when that runtime stops serving ("проб готовности ДО переключения
// трафика + автооткат при неуспехе"). These legs run on a store whose promote
// readiness gate is wired; the prober is scripted per immutable digest, so
// "ready" and "not ready" are driven explicitly rather than by wall-clock luck.
// It is a SYNTHETIC store (no operator PKI): the readiness gate and drain are
// orthogonal to supply-chain verification, and the synthetic election is asserted
// observable so this fixture can never be read as real-PKI evidence.

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// digestProber reports readiness per immutable version digest.
type digestProber struct {
	mu    sync.Mutex
	ready map[string]bool
}

func newDigestProber() *digestProber { return &digestProber{ready: map[string]bool{}} }

func (p *digestProber) set(digest string, ready bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ready[digest] = ready
}

func (p *digestProber) ProbeRuntimeReady(_ context.Context, b registry.RuntimeBinding) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready[b.Digest] {
		return nil
	}
	return contracts.NewError(contracts.ErrUnavailable, "scripted: runtime not ready")
}

// newGatedStore is a synthetic, eval-gated store with the promote readiness gate
// active for the given environments.
func newGatedStore(t *testing.T, prober registry.ReadinessProber, envs ...string) (*registry.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := registry.New(poolFor(t),
		registry.WithEvalGate(evalsHarness(t)),
		registry.WithSyntheticSupplyChain("p2 readiness gate fixture, no operator PKI"),
		registry.WithPromoteReadinessGate(prober, envs...))
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("gated store migrate: %v", err)
	}
	return store, "t-" + randSuffix(t)
}

// runtimeFor builds a well-formed binding for a version on this loopback stand.
func runtimeFor(t *testing.T, tenant, env, agent string, v registry.Version) registry.RuntimeBinding {
	t.Helper()
	srv := httptest.NewServer(nil)
	t.Cleanup(srv.Close)
	return registry.RuntimeBinding{
		Tenant: tenant, Deployment: "rt-" + randSuffix(t), Environment: env, AgentID: agent,
		Digest: v.Digest, Image: "registry.local/ahp/agent:p2@sha256:" + strings.Repeat("a", 64),
		Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz",
		Attestation: `{"payloadType":"application/vnd.in-toto+json","payload":"e30","signatures":[]}`,
		SBOM:        []byte(`{"bomFormat":"CycloneDX","components":[]}`),
	}
}

func TestPromoteProbesReadinessBeforeSwitchAndAutoRollsBack(t *testing.T) {
	ctx := context.Background()
	prober := newDigestProber()
	store, tenant := newGatedStore(t, prober, "stage")

	agent := "agent-" + randSuffix(t)
	a := publish(t, store, agent, manifest(t, "p2gate.a"))
	b := publish(t, store, agent, manifest(t, "p2gate.b"))

	prober.set(a.Digest, true)
	if err := store.RegisterRuntime(ctx, runtimeFor(t, tenant, "stage", agent, a)); err != nil {
		t.Fatalf("register runtime A: %v", err)
	}
	inst, err := store.Install(ctx, tenant, agent, a.ID, "stage", "inst-"+randSuffix(t), granted("model/qwen-max"))
	if err != nil {
		t.Fatalf("install A into stage: %v", err)
	}

	// (1) No runtime registered for the target at all: the gate refuses BEFORE
	// switching, because there is nothing it can confirm is serving.
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("promote to an unprobed target = %v, want UNAVAILABLE", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != a.ID {
		t.Fatalf("refused promote still moved traffic to %s, want %s", got.VersionID, a.ID)
	}

	// (2) The target's runtime exists but is not ready: still refused, traffic held.
	if err := store.RegisterRuntime(ctx, runtimeFor(t, tenant, "stage", agent, b)); err != nil {
		t.Fatalf("register runtime B: %v", err)
	}
	prober.set(b.Digest, false)
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("promote to a not-ready target = %v, want UNAVAILABLE", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != a.ID {
		t.Fatalf("not-ready promote moved traffic to %s, want %s", got.VersionID, a.ID)
	}

	// (3) Once the target passes the probe, the switch proceeds.
	prober.set(b.Digest, true)
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); err != nil {
		t.Fatalf("promote to a ready target: %v", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != b.ID {
		t.Fatalf("ready promote did not switch traffic: %s, want %s", got.VersionID, b.ID)
	}

	// A task pinned while on B must survive the automatic rollback on that
	// version -- only NEW tasks follow traffic back.
	waiting := "task-" + randSuffix(t)
	if pinned, err := store.Pin(ctx, tenant, inst.ID, waiting); err != nil || pinned.ID != b.ID {
		t.Fatalf("pin on B = %s (%v), want %s", pinned.ID, err, b.ID)
	}

	// (4) The live runtime (B) stops serving: the supervisor switches traffic back.
	prober.set(b.Digest, false)
	rolled, didRollback, err := store.AutoRollbackIfUnready(ctx, tenant, inst.ID)
	if err != nil {
		t.Fatalf("auto-rollback: %v", err)
	}
	if !didRollback {
		t.Fatal("an unready live runtime did not trigger an auto-rollback")
	}
	if rolled.VersionID != a.ID {
		t.Fatalf("auto-rollback target = %s, want %s", rolled.VersionID, a.ID)
	}
	if pinned, err := store.PinnedVersion(ctx, tenant, waiting); err != nil || pinned.ID != b.ID {
		t.Fatalf("auto-rollback moved a task pinned on B: %s (%v), want %s", pinned.ID, err, b.ID)
	}

	// (5) With the live runtime (now A) ready, the supervisor is a no-op -- it does
	// not ping-pong traffic back and forth.
	if _, didRollback, err := store.AutoRollbackIfUnready(ctx, tenant, inst.ID); err != nil || didRollback {
		t.Fatalf("supervisor rolled back a healthy live runtime: did=%v err=%v", didRollback, err)
	}
}

// The gate is defined for stage/prod; a dev rollout is not gated, so it must not
// require a probed runtime -- this is what keeps the dev inner loop usable while
// every stage/prod switch is still probed.
func TestDevPromoteIsNotReadinessGated(t *testing.T) {
	ctx := context.Background()
	prober := newDigestProber()
	store, tenant := newGatedStore(t, prober, "stage", "prod")

	agent := "agent-" + randSuffix(t)
	a := publish(t, store, agent, manifest(t, "p2dev.a"))
	b := publish(t, store, agent, manifest(t, "p2dev.b"))
	inst, err := store.Install(ctx, tenant, agent, a.ID, "dev", "inst-"+randSuffix(t), granted("model/qwen-max"))
	if err != nil {
		t.Fatalf("install A into dev: %v", err)
	}
	// No runtime for B is registered and the prober reports nothing ready, yet
	// the dev promote proceeds because dev is outside the gate.
	if _, err := store.Promote(ctx, tenant, inst.ID, b.ID); err != nil {
		t.Fatalf("dev promote must not be readiness-gated: %v", err)
	}
	if got, _ := store.GetInstallation(ctx, tenant, inst.ID); got.VersionID != b.ID {
		t.Fatalf("dev promote did not switch: %s, want %s", got.VersionID, b.ID)
	}
	// And the supervisor is inert for an ungated environment.
	if _, did, err := store.AutoRollbackIfUnready(ctx, tenant, inst.ID); err != nil || did {
		t.Fatalf("supervisor acted on an ungated dev installation: did=%v err=%v", did, err)
	}
}
