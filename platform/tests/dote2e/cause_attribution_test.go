package dote2e_test

// TestCauseAttributionRequiredOnTerminalFailure is a pure guard (no Docker, no
// PG) on the J10 cause guarantee (director mandate 2026-11-10): every
// non-completed terminal leg must ship a named cause - a failure_reason, a
// durable failure_cause, or a named defect. A bare FAIL with none of them is
// reported as an explicit attribution gap, so the artifact can never present
// an unexplained terminal failure.

import (
	"strings"
	"testing"
)

func TestCauseAttributionRequiredOnTerminalFailure(t *testing.T) {
	t.Run("PASS leg needs no cause", func(t *testing.T) {
		if got := causeAttribution(map[string]any{"status": "PASS", "terminal_status": "done"}); got != "" {
			t.Fatalf("PASS leg must not demand a cause, got %q", got)
		}
	})
	t.Run("FAIL with failure_reason is attributed", func(t *testing.T) {
		leg := map[string]any{"status": "FAIL", "terminal_status": "failed", "failure_reason": "model stream refused: HTTP 400"}
		if got := causeAttribution(leg); got != "" {
			t.Fatalf("named failure_reason must satisfy the guarantee, got %q", got)
		}
	})
	t.Run("FAIL with durable failure_cause is attributed", func(t *testing.T) {
		leg := map[string]any{"status": "FAIL", "terminal_status": "failed", "failure_reason": "",
			"durable_forensics": map[string]any{"failure_cause": "preflight refused: budget exceeded"}}
		if got := causeAttribution(leg); got != "" {
			t.Fatalf("durable failure_cause must satisfy the guarantee, got %q", got)
		}
	})
	t.Run("BLOCKED with named defect is attributed", func(t *testing.T) {
		leg := map[string]any{"status": "BLOCKED", "terminal_status": "failed", "defect": "DEF-DOTE2E-14"}
		if got := causeAttribution(leg); got != "" {
			t.Fatalf("named defect must satisfy the guarantee, got %q", got)
		}
	})
	t.Run("bare FAIL without any cause becomes an explicit gap", func(t *testing.T) {
		leg := map[string]any{"status": "FAIL", "terminal_status": "failed", "failure_reason": ""}
		got := causeAttribution(leg)
		if !strings.Contains(got, "cause_attribution_gap") {
			t.Fatalf("bare FAIL must surface an explicit attribution gap, got %q", got)
		}
	})
}
