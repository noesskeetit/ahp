package harness

import (
	"strings"
	"testing"
)

// Acceptance has twice found a published number bound to the WRONG binary,
// because the lane's bin/ is mutable: "bin/ahp 7d77e893... vs 5ccf40b8...".
// The rule that closes the class: a run measures an immutable copy of its own
// run directory, and the guard refuses anything else as a final profile.
func TestQualityGuardRefusesMutableMeasuredBinary(t *testing.T) {
	rep := goodFinalReport()
	rep.Version.BinaryPath = "/work/lanes/prof-harden/bin/ahp-profiling"
	var hit bool
	for _, v := range rep.QualityViolations(DefaultFinalRequirements()) {
		if strings.Contains(v, "pin") || strings.Contains(v, "mutable") {
			hit = true
		}
	}
	if !hit {
		t.Fatalf("guard accepted a final profile measured through the mutable lane bin/: %v", rep.QualityViolations(DefaultFinalRequirements()))
	}
}

func TestQualityGuardAcceptsRunPinnedBinary(t *testing.T) {
	rep := goodFinalReport()
	rep.Version.BinaryPath = "/work/evidence/runs/20261109T221500Z-abc123/bin/ahp-profiling"
	for _, v := range rep.QualityViolations(DefaultFinalRequirements()) {
		if strings.Contains(v, "pin") || strings.Contains(v, "mutable") {
			t.Fatalf("a per-run pinned copy must be accepted: %s", v)
		}
	}
}
