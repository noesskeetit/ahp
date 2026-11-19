package registry_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/evals"
)

// evalsHarness is the REAL internal/evals plane the accepted registry tests
// publish against. "Оценки перед публикацией" (rev 3.2) makes a passing verdict a
// precondition of publication, so the registry test store is gated on this store
// and the publish helpers record a genuine passing verdict for the exact version
// digest before every publication. It is not a stub and not a WithEvalGate(nil)
// bypass: verdicts are written to the real ahp_evals schema on real PostgreSQL and
// read back by the same gate the product uses -- only the verdict CONTENT (a
// throwaway reference task) is fixture data, never a way to disable the check.
var (
	evalsOnce  sync.Once
	evalsPlane *evals.Store
	evalsErr   error
)

// The harness policy is the registry tests' stand-in for the operator's
// "актуальный policy" (row 24): verdicts are recorded under it and the gate
// measures them against it. Re-declaring identical content is idempotent and
// keeps it CURRENT, so every publishing test re-establishes its own baseline
// regardless of which policy a previous test left in force.
const (
	harnessPolicyID  = "h26-harness-policy"
	harnessTaskSetID = "reference-set-h26"
)

func harnessPolicy() evals.Policy {
	return evals.Policy{
		ID: harnessPolicyID, TaskSetID: harnessTaskSetID,
		RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600,
	}
}

func ensurePolicy(t *testing.T, s *evals.Store) {
	t.Helper()
	if _, err := s.SetPolicy(context.Background(), harnessPolicy()); err != nil {
		t.Fatalf("declare harness eval policy: %v", err)
	}
}

func ensureHarnessPolicy(t *testing.T) {
	t.Helper()
	ensurePolicy(t, evalsHarness(t))
}

func evalsHarness(t *testing.T) *evals.Store {
	t.Helper()
	evalsOnce.Do(func() {
		url := os.Getenv("AHP_TEST_DATABASE_URL")
		if url == "" {
			evalsErr = errors.New("AHP_TEST_DATABASE_URL is not set")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		p, err := pgxpool.New(ctx, url)
		if err != nil {
			evalsErr = err
			return
		}
		s := evals.NewStore(p)
		if err := s.Migrate(ctx); err != nil {
			evalsErr = err
			return
		}
		evalsPlane = s
	})
	if evalsErr != nil {
		t.Fatalf("evals harness: %v", evalsErr)
	}
	return evalsPlane
}

// passVerdict records a real passing evaluation verdict for exactly this version
// digest, satisfying the publication precondition for the harness fixtures. A
// caller that asserts publication SUCCESS must ensure a verdict for that digest
// exists. A caller that asserts a DIFFERENT failure (unclaimed agent, foreign
// publisher, invalid manifest, malformed digest) must NOT: those paths fail before
// the gate runs, so a verdict is neither needed nor would it change the outcome.
func passVerdict(t *testing.T, digest string) {
	t.Helper()
	ensureHarnessPolicy(t)
	if _, err := evalsHarness(t).Record(context.Background(), evals.RecordInput{
		AgentDigest: digest,
		TaskSetID:   harnessTaskSetID,
		Setup:       map[string]any{"harness": "registry"},
		Outcome:     evals.OutcomePass,
		Tasks:       []evals.TaskResult{{TaskID: "reference-task", Passed: true, TerminalState: evals.TerminalCompleted, Cost: 0}},
		PolicyID:    harnessPolicyID, TargetKind: evals.TargetFixture,
	}); err != nil {
		t.Fatalf("record passing verdict for %s: %v", digest, err)
	}
}
