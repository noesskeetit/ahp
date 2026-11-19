package dote2e_test

// TestVerdictSemanticsCannotBePassWithIncompleteLeg is a pure (no Docker, no PG)
// guard on the report generator itself (DEF-VERDICT-1). It proves mechanically
// that the overall scenario verdict can never be PASS while any leg is not a
// completed terminal. The historical anti-fixture
// evidence/dot-e2e-runtime/evidence/dot-e2e-scenario-personal-dot-journey-20261109T165158Z.json
// (recorded verdict=PASS while its J10 carried terminal_status=failed) is fed
// through the same journeyVerdict the generator uses; the file is left untouched.

import (
	"encoding/json"
	"os"
	"testing"
)

func passLeg(terminal string) map[string]any {
	leg := map[string]any{"status": "PASS"}
	if terminal != "" {
		leg["terminal_status"] = terminal
	}
	return leg
}

func journeyChecks(j10 map[string]any) map[string]any {
	c := map[string]any{}
	for _, name := range []string{
		"j1_task_grant_token", "j2_config_profile_pin", "j3_model_real_qwen",
		"j4_tool_hitl_result", "j5_memory", "j6_file_sha", "j7_history_reload",
		"j8_hitl_cancel", "j9_two_user_isolation", "j11_human_artifact_read",
	} {
		// DeclaredFacts, not a bare PASS: a declared frame-free leg counts as
		// completed only while the durable fact its declaration names is in the
		// artifact (DEF-DOTSDK-TERMINAL-1). A fixture that omitted them would test a
		// PASS shape the harness no longer accepts.
		c[name] = declaredFacts(passLeg(""), scenarioPersonalDotJourney, name)
	}
	c["j10_browser_turn_def1_branch"] = j10
	return c
}

func TestVerdictSemanticsCannotBePassWithIncompleteLeg(t *testing.T) {
	t.Run("all completed -> PASS", func(t *testing.T) {
		// J10 carries a completed terminal ("done"); every other leg has no
		// terminal_status and passes. This is the ONLY shape allowed to be PASS.
		verdict, completed, blocked, failed := journeyVerdict(journeyChecks(passLeg("done")))
		if verdict != "PASS" {
			t.Fatalf("want PASS when every leg is completed, got %s (blocked=%v failed=%v)", verdict, blocked, failed)
		}
		if len(completed) != 11 || len(blocked) != 0 || len(failed) != 0 {
			t.Fatalf("want 11 completed / 0 blocked / 0 failed, got %d/%d/%d", len(completed), len(blocked), len(failed))
		}
	})

	t.Run("J10 status PASS but terminal_status failed -> NOT PASS", func(t *testing.T) {
		j10 := map[string]any{"status": "PASS", "terminal_status": "failed", "defect": "DEF-DOTE2E-9"}
		verdict, _, blocked, failed := journeyVerdict(journeyChecks(j10))
		if verdict == "PASS" {
			t.Fatalf("regression: a leg with terminal_status=failed produced overall PASS")
		}
		if verdict != "BLOCKED" {
			t.Fatalf("want BLOCKED for a recorded non-completed J10, got %s (blocked=%v failed=%v)", verdict, blocked, failed)
		}
	})

	t.Run("J10 failed status -> FAIL", func(t *testing.T) {
		j10 := map[string]any{"status": "FAIL", "terminal_status": "failed"}
		verdict, _, _, failed := journeyVerdict(journeyChecks(j10))
		if verdict == "PASS" {
			t.Fatalf("regression: a FAIL leg produced overall PASS")
		}
		if verdict != "FAIL" || len(failed) == 0 {
			t.Fatalf("want FAIL with a non-empty failed list, got %s failed=%v", verdict, failed)
		}
	})

	t.Run("no legs -> FAIL", func(t *testing.T) {
		verdict, completed, _, _ := journeyVerdict(map[string]any{})
		if verdict != "FAIL" {
			t.Fatalf("want FAIL when nothing was proven, got %s", verdict)
		}
		if len(completed) != 0 {
			t.Fatalf("want no completed legs, got %v", completed)
		}
	})

	t.Run("historical anti-fixture -> NOT PASS", func(t *testing.T) {
		path := os.Getenv("AHP_DOT_E2E_FIXTURE")
		candidates := []string{
			path,
			"../../../evidence/dot-e2e-runtime/evidence/dot-e2e-scenario-personal-dot-journey-20261109T165158Z.json",
		}
		var raw []byte
		var used string
		for _, c := range candidates {
			if c == "" {
				continue
			}
			if b, err := os.ReadFile(c); err == nil {
				raw, used = b, c
				break
			}
		}
		if raw == nil {
			// Token grammar owned by scripts/skip-verdict.mjs: the anti-fixture is a file
			// the lane layout ships; unreachable means this checkout cannot measure it.
			t.Skip("SKIP_ENV[tag=dot_e2e_layout] reason=anti_fixture_unreachable missing=AHP_DOT_E2E_EVIDENCE -- a skip here means not-covered, not passed: anti-fixture file not reachable from this layout")
		}
		var fixture struct {
			Verdict string         `json:"verdict"`
			Checks  map[string]any `json:"checks"`
		}
		if err := json.Unmarshal(raw, &fixture); err != nil {
			t.Fatalf("parse fixture %s: %v", used, err)
		}
		if fixture.Verdict != "PASS" {
			t.Fatalf("expected the anti-fixture to record PASS (that is the bug), got %s", fixture.Verdict)
		}
		if verdict, _, _, _ := journeyVerdict(fixture.Checks); verdict == "PASS" {
			t.Fatalf("generator still returns PASS for the historical anti-fixture %s", used)
		}
		t.Logf("anti-fixture %s (old verdict=PASS) now computes %s", used, func() string {
			verdict, _, _, _ := journeyVerdict(fixture.Checks)
			return verdict
		}())
	})
}
