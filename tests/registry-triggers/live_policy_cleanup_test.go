package registrytriggers_test

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/internal/evals"
)

// This probes fixture lifecycle only; the HTTPS URL is never called and this
// test does not claim an LLM run. Real suites must leave the operator policy
// they found, rather than silently invalidate unrelated deployed artifacts.
func TestLiveGateRestoresPriorEvaluationPolicy(t *testing.T) {
	ctx := context.Background()
	pool := newPool(t, requireEnv(t, "AHP_TEST_DATABASE_URL"))
	store := evals.NewStore(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	original, originalErr := store.CurrentPolicy(ctx)
	if originalErr == nil {
		t.Cleanup(func() {
			if _, err := store.SetPolicy(context.Background(), original); err != nil {
				t.Errorf("restore fixture policy: %v", err)
			}
		})
	}
	before, err := store.SetPolicy(ctx, evals.Policy{ID: "live-cleanup-before-" + uniqueSuffix(), TaskSetID: "existing-reference", RequiredTaskIDs: []string{"existing-case"}, MaxAgeSeconds: 86400, RequireRealTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("deployed-fixture", func(t *testing.T) {
		t.Setenv("AHP_LIVE_QWEN", "1")
		t.Setenv("AHP_DEPLOYED_URL", "https://127.0.0.1:1")
		t.Setenv("AHP_DEPLOYED_DATABASE_URL", "")
		liveGate(t)
		if _, err := store.SetPolicy(ctx, evals.Policy{ID: "h15-live-policy", TaskSetID: "reference-set-h15", RequiredTaskIDs: []string{"reference-task"}, MaxAgeSeconds: 3600}); err != nil {
			t.Fatal(err)
		}
	})
	after, err := store.CurrentPolicy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.ID != before.ID || after.Digest != before.Digest {
		t.Fatalf("live fixture replaced operator policy: got %s, want %s", after.ID, before.ID)
	}
}
