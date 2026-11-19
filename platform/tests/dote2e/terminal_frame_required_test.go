package dote2e_test

// Terminal-frame requirement (DEF-VERDICT-1 completion). Independent review of the
// Dot 11/11 artifact (reports/mac-director/HEISENBERG-DOT-VERIFY-f14585d2.md) found
// that terminalIsCompleted returned TRUE for a leg with no terminal_status at all,
// so "no terminal frame observed" was indistinguishable from "terminal completed".
// That is the same false-PASS class root rejected twice. These tests are pure (no
// Docker, no PG): they exercise the report generator, and they are RED against the
// two-state classifier and GREEN against the three-state one.

import (
	"strings"
	"testing"
)

// strictJourney classifies under the acceptance-scenario rule and returns the
// whole classification, so tests can assert reasons and terminal states too.
func strictJourney(t *testing.T, checks map[string]any) legClassification {
	t.Helper()
	t.Setenv("AHP_DOT_E2E_STRICT_TERMINAL", "")
	return classifyLegs("personal-dot-journey", checks)
}

// undeclaredLeg renames a declared frame-free leg, simulating a journey leg that
// was added without declaring its terminal evidence - the case that must go
// BLOCKED, never PASS.
func undeclaredLeg(checks map[string]any, from string) string {
	to := "j9_two_user_isolation_unregistered"
	if v, ok := checks[from]; ok {
		checks[to] = v
		delete(checks, from)
	}
	return to
}

func TestTerminalFrameAbsentCannotBeCompleted(t *testing.T) {
	t.Run("absent terminal_status on an undeclared leg is unknown, not completed", func(t *testing.T) {
		checks := journeyChecks(passLeg("done"))
		leg := undeclaredLeg(checks, "j9_two_user_isolation")
		c := strictJourney(t, checks)
		if got := c.terminalStates[leg]; got != terminalAbsent {
			t.Fatalf("want terminal state %q for an undeclared leg with no terminal frame, got %q", terminalAbsent, got)
		}
		for _, name := range c.completed {
			if name == leg {
				t.Fatalf("regression: leg without a terminal frame was counted as completed")
			}
		}
		found := false
		for _, name := range c.blocked {
			if name == leg {
				found = true
			}
		}
		if !found {
			t.Fatalf("unknown leg must be blocked, blocked=%v", c.blocked)
		}
		if reason := c.blockedReasons[leg]; !strings.Contains(reason, "missing_terminal_frame") {
			t.Fatalf("blocked reason must name missing_terminal_frame, got %q", reason)
		}
		// The artifact carries the unknown legs explicitly, so an empty blocked or
		// failed list can be read as "no such leg" rather than "reason lost".
		if len(c.unknown) != 1 || c.unknown[0] != leg {
			t.Fatalf("unknown_terminal_frame_legs must list exactly %s, got %v", leg, c.unknown)
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
			t.Fatalf("regression: overall verdict PASS while a leg has unknown terminal evidence")
		} else if v != "BLOCKED" {
			t.Fatalf("want BLOCKED for unknown terminal evidence, got %s", v)
		}
	})

	t.Run("empty or nil terminal_status is unknown too", func(t *testing.T) {
		for _, v := range []any{"", "   ", nil} {
			if got := terminalFrameState(v); got != terminalAbsent {
				t.Fatalf("terminal_status %#v must classify as %q, got %q", v, terminalAbsent, got)
			}
		}
		checks := journeyChecks(map[string]any{"status": "PASS", "terminal_status": ""})
		c := strictJourney(t, checks)
		if c.terminalStates["j10_browser_turn_def1_branch"] != terminalAbsent {
			t.Fatalf("empty terminal_status must be unknown, got %q", c.terminalStates["j10_browser_turn_def1_branch"])
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v != "BLOCKED" {
			t.Fatalf("want BLOCKED when the turn leg shows an empty terminal frame, got %s", v)
		}
	})

	t.Run("failed and canceled terminals are non_completed never completed", func(t *testing.T) {
		for _, ts := range []string{"failed", "canceled", "cancelled", "error", "pending", "blocked"} {
			if got := terminalFrameState(ts); got != terminalContradicted {
				t.Fatalf("terminal_status %q must classify as %q, got %q", ts, terminalContradicted, got)
			}
		}
		// A leg that writes PASS while its terminal frame says failed must be
		// downgraded, not trusted (the historical ...165158Z shape).
		checks := journeyChecks(map[string]any{"status": "PASS", "terminal_status": "failed"})
		c := strictJourney(t, checks)
		if c.terminalStates["j10_browser_turn_def1_branch"] != terminalContradicted {
			t.Fatalf("failed terminal must classify as %q", terminalContradicted)
		}
		for _, name := range c.completed {
			if name == "j10_browser_turn_def1_branch" {
				t.Fatalf("regression: PASS with terminal_status=failed counted completed")
			}
		}
		if v := overallVerdict(c.completed, c.blocked, c.failed); v == "PASS" {
			t.Fatalf("regression: overall PASS with terminal_status=failed present")
		}
	})
}

func TestDeclaredFrameFreeLegsCompleteTheJourney(t *testing.T) {
	// The declared set is the only way a journey leg passes without a terminal
	// frame. It must cover exactly the ten observation legs and must NOT cover the
	// turn leg - otherwise "unknown" could be laundered by adding j10 to the list.
	if len(journeyFrameFreeLegs) != 10 {
		t.Fatalf("want exactly 10 declared frame-free journey legs, got %d", len(journeyFrameFreeLegs))
	}
	if journeyFrameFreeLegs["j10_browser_turn_def1_branch"] {
		t.Fatal("the browser turn leg must never be declared frame-free: it has a durable terminal frame")
	}
	c := strictJourney(t, journeyChecks(passLeg("done")))
	if v := overallVerdict(c.completed, c.blocked, c.failed); v != "PASS" {
		t.Fatalf("a journey with a completed turn frame and ten declared legs must PASS, got %s blocked=%v failed=%v",
			v, c.blocked, c.failed)
	}
	if len(c.completed) != 11 {
		t.Fatalf("want 11 completed, got %d", len(c.completed))
	}
	for name, state := range c.terminalStates {
		if state == terminalAbsent || state == terminalContradicted {
			t.Fatalf("passing journey must show no unknown/contradicted leg, %s is %q", name, state)
		}
	}
	if c.terminalStates["j10_browser_turn_def1_branch"] != terminalSeenCompleted {
		t.Fatalf("turn leg must show an OBSERVED completed frame, got %q", c.terminalStates["j10_browser_turn_def1_branch"])
	}
	if len(c.unknown) != 0 {
		t.Fatalf("a passing journey must list no unknown-terminal legs, got %v", c.unknown)
	}
}

func TestReasonsAreNeverLostOnIncompleteLegs(t *testing.T) {
	// An empty reason list while a leg list is non-empty would mean "the reason was
	// lost", indistinguishable from "no such leg". Every listed leg owes a reason.
	checks := journeyChecks(map[string]any{"status": "FAIL", "terminal_status": "failed"})
	delete(checks["j5_memory"].(map[string]any), "status")
	checks["j5_memory"].(map[string]any)["status"] = "BLOCKED"
	c := strictJourney(t, checks)
	if len(c.failed) == 0 && len(c.blocked) == 0 {
		t.Fatal("expected incomplete legs in this shape")
	}
	for _, name := range c.failed {
		if strings.TrimSpace(c.failedReasons[name]) == "" {
			t.Fatalf("failed leg %s has no reason recorded", name)
		}
	}
	for _, name := range c.blocked {
		if strings.TrimSpace(c.blockedReasons[name]) == "" {
			t.Fatalf("blocked leg %s has no reason recorded", name)
		}
	}
}

func TestTerminalEnforcementModeIsExplicit(t *testing.T) {
	// The journey is always strict; other scenarios are strict only when the lane
	// opts in, and in that mode the residual gap must disappear rather than be
	// reported as completed.
	t.Setenv("AHP_DOT_E2E_STRICT_TERMINAL", "")
	if !strictTerminalEnforcement("personal-dot-journey") {
		t.Fatal("the personal Dot journey must always enforce the terminal-frame rule")
	}
	checks := map[string]any{"schema_probe": map[string]any{"status": "PASS"}}
	if c := classifyLegs("model-schema-probe", checks); c.strictEnforcement {
		t.Fatal("non-journey scenarios are lenient unless AHP_DOT_E2E_STRICT_TERMINAL=1")
	} else if len(c.completed) != 1 {
		t.Fatalf("lenient mode keeps the legacy classification, got completed=%v", c.completed)
	}
	t.Setenv("AHP_DOT_E2E_STRICT_TERMINAL", "1")
	c := classifyLegs("model-schema-probe", checks)
	if !c.strictEnforcement {
		t.Fatal("AHP_DOT_E2E_STRICT_TERMINAL=1 must enable strict classification for every scenario")
	}
	if len(c.completed) != 0 || len(c.blocked) != 1 {
		t.Fatalf("strict mode must block an undeclared leg, got completed=%v blocked=%v", c.completed, c.blocked)
	}
	if !strings.Contains(c.blockedReasons["schema_probe"], "missing_terminal_frame") {
		t.Fatalf("strict mode must name missing_terminal_frame, got %q", c.blockedReasons["schema_probe"])
	}
}
