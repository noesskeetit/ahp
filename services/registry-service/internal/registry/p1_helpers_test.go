package registry_test

// Test-only helpers for the P1 supply-chain/drain acceptance face. They build
// the state a real pipeline leaves behind: a content-addressed image, an SBOM
// bound by a publisher signature, and the stage verdict planes (evaluation,
// restart-chaos, security) recorded by their own authorities.

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/jackc/pgx/v5/pgxpool"
)

// poolFor opens a pool against the same real PostgreSQL instance the tests use.
func poolFor(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newSyntheticStore builds the same real-PostgreSQL fixture as newStore, but on
// an explicitly elected SYNTHETIC stand, and returns its tenant. Wire-level
// tests that exercise transport plumbing (routing by an immutable digest,
// binding immutability) need runtime registrations without an operator PKI; the
// election makes that visible instead of relying on an unchecked attestation
// string. Supply-chain behaviour itself is tested on stores that DO carry roots.
func newSyntheticStore(t *testing.T) (*registry.Store, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := registry.New(poolFor(t),
		registry.WithEvalGate(evalsHarness(t)),
		registry.WithSyntheticSupplyChain("wire regression, no operator PKI"))
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("synthetic store migrate: %v", err)
	}
	return store, "t-" + randSuffix(t)
}

// recordChaos records the restart-chaos/"availability" clearance of an image.
// In the product this write belongs to the platform verification authority,
// never to the publisher or a task.
func (e *supplyEnv) recordChaos(t *testing.T, im imageEnv, outcome string, checked, expires time.Time) string {
	t.Helper()
	cv, err := e.store.RecordChaosVerdict(context.Background(), registry.ChaosVerdict{
		OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: outcome,
		Source: "stage-restart-chaos", CheckedAt: checked, ExpiresAt: expires,
	})
	if err != nil {
		t.Fatalf("record chaos verdict: %v", err)
	}
	return cv.ID
}

// signAndAdmit signs the attestation that binds im's verdict references and
// admits the image, failing the test on refusal.
func (e *supplyEnv) signAndAdmit(t *testing.T, im imageEnv) registry.Admission {
	t.Helper()
	return e.admit(t, im)
}

// admitPair brings one published version of one agent to the state a stage
// rollout requires: security and restart-chaos clearances recorded for this
// exact image, and an admission whose signature binds them.
func (e *supplyEnv) admitPair(t *testing.T, agent string, v registry.Version, im imageEnv) {
	t.Helper()
	im.agent = agent
	im.version = v
	if im.secID == "" {
		im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	}
	e.recordChaos(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.signAndAdmit(t, im)
}

// fullReadyImage publishes an image and clears every stage plane for it.
func (e *supplyEnv) fullReadyImage(t *testing.T, tag string) imageEnv {
	t.Helper()
	im := e.publishImage(t, tag)
	im.secID = e.recordSecurity(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.recordChaos(t, im, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.signAndAdmit(t, im)
	return im
}

// stageInstall installs a version into a governed environment, taking care that
// the signature binds the clearances that environment demands.
func (e *supplyEnv) stageInstall(t *testing.T, agent string, v registry.Version, im imageEnv, env string) registry.Installation {
	t.Helper()
	e.admitPair(t, agent, v, im)
	inst, err := e.store.Install(context.Background(), e.tenant, agent, v.ID, env, "inst-"+randSuffix(t), v.Manifest.Capabilities)
	if err != nil {
		t.Fatalf("install %s of %s: %v", env, v.ID, err)
	}
	return inst
}

// awaitDrainElapsed waits until the recorded drain window has really elapsed.
// No clock is faked: the fixtures declare a one-second window and this waits it
// out, so the drain deadline is exercised by the same clock the product reads.
func (e *supplyEnv) awaitDrainElapsed(t *testing.T, installationID string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for {
		drain, err := e.store.DrainState(ctx, e.tenant, installationID)
		if err != nil {
			t.Fatalf("drain state: %v", err)
		}
		if !drain.DrainUntil.IsZero() && !time.Now().Before(drain.DrainUntil) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("drain window never elapsed: %+v", drain)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
