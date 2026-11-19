package registry_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// "Оценки перед публикацией" (PDF rev 3.2, с. 42/48) governs every move that puts
// an agent version in front of users, not only the first publication. Promotion
// of an installation to a target version whose evaluation verdict is missing or
// has since been superseded by a FAIL must be refused with CONFLICT, even for a
// manifest-only version (no OCI image) and with the supply-chain admission off:
// the eval gate is the product rule; supply-chain admission is an extra plane.
func TestPromoteRequiresPassingEvalVerdict(t *testing.T) {
	store, tenant := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	agent := "agent-h26-" + randSuffix(t)
	mustRegister(t, store, testPublisher, agent)
	v1 := publish(t, store, agent, manifest(t, "acme.promote-gate.a"))
	v2 := publish(t, store, agent, manifest(t, "acme.promote-gate.b"))

	inst, err := store.Install(ctx, tenant, agent, v1.ID, "dev", "promote-gate-"+randSuffix(t), v1.Manifest.Capabilities)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}

	// A late FAIL verdict for v2 supersedes its passing verdict. Promotion onto a
	// version whose newest verdict fails must be refused.
	if _, err := evalsHarness(t).Record(ctx, evals.RecordInput{
		AgentDigest: v2.Digest, TaskSetID: "reference-set-h26",
		Setup: map[string]any{"harness": "promote-gate"}, Outcome: evals.OutcomeFail,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: false, Cost: 0.01}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record late FAIL for v2: %v", err)
	}
	if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("Promote onto v2 with a superseded failing verdict = %v, want CONFLICT", err)
	}

	// Its own fresh PASS re-enables the promotion (the gate is a freshness rule,
	// not a permanent ban).
	passVerdict(t, v2.Digest)
	if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); err != nil {
		t.Fatalf("Promote after a fresh PASS verdict: %v", err)
	}
}

// A whole-system deploy is a promotion of every member at once and carries the
// same precondition: a member version whose newest verdict fails cannot be
// deployed. Rollback (moving back to what already ran) stays possible, matching
// the accepted rollback escape-hatch semantics.
func TestDeploySystemRequiresPassingEvalVerdict(t *testing.T) {
	store, tenant := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	system := "sys-h26-" + randSuffix(t)
	if err := store.RegisterSystem(ctx, testPublisher, system); err != nil {
		t.Fatalf("RegisterSystem: %v", err)
	}
	agent := "agent-h26-" + randSuffix(t)
	mustRegister(t, store, testPublisher, agent)
	// The manifests must declare the system for release membership.
	v1 := publish(t, store, agent, manifestFor(t, system, nil, nil))

	rel1, err := store.PublishSystemRelease(ctx, testPublisher, system, []registry.SystemMember{{AgentID: agent, VersionID: v1.ID}})
	if err != nil {
		t.Fatalf("PublishSystemRelease rel1: %v", err)
	}
	if _, err := store.DeploySystem(ctx, tenant, system, rel1.ID, "dev"); err != nil {
		t.Fatalf("DeploySystem rel1 (fresh verdict): %v", err)
	}

	v2 := publish(t, store, agent, manifestFor(t, system, nil, nil))
	passVerdict(t, v2.Digest)
	rel2, err := store.PublishSystemRelease(ctx, testPublisher, system, []registry.SystemMember{{AgentID: agent, VersionID: v2.ID}})
	if err != nil {
		t.Fatalf("PublishSystemRelease rel2: %v", err)
	}
	if _, err := store.DeploySystem(ctx, tenant, system, rel2.ID, "dev"); err != nil {
		t.Fatalf("DeploySystem rel2 with a fresh verdict: %v", err)
	}

	// The deployed release goes bad after the fact: its newest verdict fails.
	if _, err := evalsHarness(t).Record(ctx, evals.RecordInput{
		AgentDigest: v2.Digest, TaskSetID: "reference-set-h26",
		Setup: map[string]any{"harness": "release-gate"}, Outcome: evals.OutcomeFail,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: false, Cost: 0.01, TerminalState: evals.TerminalCompleted}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record late FAIL for v2: %v", err)
	}

	// Escape hatch: retreating from a bad release stays possible even though
	// its verdict now fails -- the gate stops going forward, never running away.
	back, err := store.RollbackSystem(ctx, tenant, system, "dev")
	if err != nil {
		t.Fatalf("RollbackSystem must stay possible: %v", err)
	}
	if back.ReleaseID != rel1.ID {
		t.Fatalf("RollbackSystem landed on %s, want %s", back.ReleaseID, rel1.ID)
	}

	// Re-deploying the now-failing release forward is refused again.
	if _, err := store.DeploySystem(ctx, tenant, system, rel2.ID, "dev"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("DeploySystem rel2 onto a failing verdict = %v, want CONFLICT", err)
	}
}

// Freshness is measured against the CURRENT policy, which is a stored operator
// fact: renaming the promotion target or replaying the request cannot conjure a
// verdict under the new policy. A verdict earned under a superseded policy
// denies promotion until a real re-run re-earns it.
func TestPromoteRejectsVerdictUnderSupersededPolicy(t *testing.T) {
	store, tenant := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	agent := "agent-h26-" + randSuffix(t)
	mustRegister(t, store, testPublisher, agent)
	v1 := publish(t, store, agent, manifest(t, "acme.stale-policy.a"))
	v2 := publish(t, store, agent, manifest(t, "acme.stale-policy.b"))
	inst, err := store.Install(ctx, tenant, agent, v1.ID, "dev", "stale-policy-"+randSuffix(t), v1.Manifest.Capabilities)
	if err != nil {
		t.Fatalf("Install: %v", err)
	}
	if _, err := store.Promote(ctx, tenant, inst.ID, v2.ID); err != nil {
		t.Fatalf("Promote under the current policy: %v", err)
	}

	// The operator declares a new policy (same reference set, new required
	// coverage): v2's verdict is now stale by POLICY IDENTITY even though it
	// passed and is not yet aged.
	polID := "stale-check-" + randSuffix(t)
	if _, err := evalsHarness(t).SetPolicy(ctx, evals.Policy{
		ID: polID, TaskSetID: harnessTaskSetID,
		RequiredTaskIDs: []string{"reference-task", "extra-task"}, MaxAgeSeconds: 3600,
	}); err != nil {
		t.Fatalf("SetPolicy: %v", err)
	}
	back, err := store.Promote(ctx, tenant, inst.ID, v1.ID)
	if contracts.Code(err) != contracts.ErrConflict || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("Promote under a superseded verdict = %v (inst %+v), want CONFLICT mentioning superseded", err, back)
	}

	// A fresh honest run under the new policy re-opens promotion -- the gate
	// yields only to evidence, never to request mutation.
	ensureHarnessPolicy(t)
	passVerdict(t, v1.Digest)
	if _, err := store.Promote(ctx, tenant, inst.ID, v1.ID); err != nil {
		t.Fatalf("Promote after re-earning the verdict under the current policy: %v", err)
	}
}
