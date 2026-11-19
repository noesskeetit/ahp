package registry_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The publication gate ("оценки перед публикацией", rev 3.2 с. 42/48) is a hard
// product rule. Against real PostgreSQL: a store built without a gate refuses
// every publication; a gated store refuses until a PASSING verdict exists for the
// exact digest; a passing verdict publishes the version and re-publishing it is
// idempotent; and a verdict never unlocks a different digest.
func TestPublishRequiresPassingEvalVerdict(t *testing.T) {
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()

	evalStore := evals.NewStore(pool)
	if err := evalStore.Migrate(ctx); err != nil {
		t.Fatalf("evals migrate: %v", err)
	}
	gated := registry.New(pool, registry.WithEvalGate(evalStore))
	if err := gated.Migrate(ctx); err != nil {
		t.Fatalf("registry migrate: %v", err)
	}
	ungated := registry.New(pool)

	agent := "agent-h26-" + randSuffix(t)
	mustRegister(t, gated, testPublisher, agent)
	m := manifest(t, "acme.evalsgate")
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}

	// (0) A store with no evals gate wired fails closed: it publishes nothing.
	if _, err := ungated.PutVersion(ctx, testPublisher, agent, digest, m); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("ungated PutVersion = %v, want CONFLICT (fail closed)", err)
	}
	// (1) A gated store with no verdict at all refuses.
	if _, err := gated.PutVersion(ctx, testPublisher, agent, digest, m); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gated PutVersion with no verdict = %v, want CONFLICT", err)
	}
	// (2) A FAILING verdict does not unblock publication.
	ensurePolicy(t, evalStore)
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: harnessTaskSetID, Setup: map[string]any{"v": 1}, Outcome: evals.OutcomeFail,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: false, Cost: 0.01}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record fail verdict: %v", err)
	}
	if _, err := gated.PutVersion(ctx, testPublisher, agent, digest, m); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("gated PutVersion after FAIL verdict = %v, want CONFLICT", err)
	}
	// (3) A PASSING verdict for this exact digest publishes.
	ensurePolicy(t, evalStore)
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest, TaskSetID: harnessTaskSetID, Setup: map[string]any{"v": 2}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.01}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record pass verdict: %v", err)
	}
	first, err := gated.PutVersion(ctx, testPublisher, agent, digest, m)
	if err != nil {
		t.Fatalf("gated PutVersion after PASS verdict: %v", err)
	}
	// (4) Re-publishing the same digest is idempotent (same version row).
	replay, err := gated.PutVersion(ctx, testPublisher, agent, digest, m)
	if err != nil {
		t.Fatalf("re-publish same digest: %v", err)
	}
	if replay.ID != first.ID || replay.Digest != first.Digest {
		t.Fatalf("re-publish not idempotent: %s vs %s", replay.ID, first.ID)
	}
	// A late failure also closes the employee-catalog publication boundary.
	inst, err := gated.Install(ctx, "tenant-"+randSuffix(t), agent, first.ID, "prod", "late-fail-"+randSuffix(t), m.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := evalStore.Record(ctx, evals.RecordInput{AgentDigest: digest, TaskSetID: "late-failure", Outcome: evals.OutcomeFail, Tasks: []evals.TaskResult{{TaskID: "reference", Passed: false}}, PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture}); err != nil {
		t.Fatal(err)
	}
	if err := gated.Publish(ctx, inst.Tenant, inst.ID, true); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("catalog publish admitted late failure: %v", err)
	}
	if err := gated.PublishWithRoles(ctx, inst.Tenant, inst.ID, []string{"employee"}); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("role catalog publish admitted late failure: %v", err)
	}
	if err := gated.Publish(ctx, inst.Tenant, inst.ID, false); err != nil {
		t.Fatalf("unpublish must remain possible: %v", err)
	}
	// (5) The verdict for `digest` does not unlock a different digest.
	agent2 := "agent-h26-" + randSuffix(t)
	mustRegister(t, gated, testPublisher, agent2)
	m2 := manifest(t, "acme.evalsgate2")
	digest2, err := contracts.Digest(m2)
	if err != nil {
		t.Fatalf("digest2: %v", err)
	}
	if _, err := gated.PutVersion(ctx, testPublisher, agent2, digest2, m2); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("publish of a second digest = %v, want CONFLICT (verdict not reusable)", err)
	}
	// ...until its own passing verdict is recorded.
	ensurePolicy(t, evalStore)
	if _, err := evalStore.Record(ctx, evals.RecordInput{
		AgentDigest: digest2, TaskSetID: harnessTaskSetID, Setup: map[string]any{"v": 1}, Outcome: evals.OutcomePass,
		Tasks:    []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0.02}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("Record pass verdict (digest2): %v", err)
	}
	if _, err := gated.PutVersion(ctx, testPublisher, agent2, digest2, m2); err != nil {
		t.Fatalf("publish after its own PASS verdict: %v", err)
	}
}
