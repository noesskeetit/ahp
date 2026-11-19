package main

import (
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/profiling/harness"
)

// A step with no sampler stream must say so in the live log, not print -1.
func TestMissingSamplerStreamIsNotNumberedInTheLiveLog(t *testing.T) {
	if got := samplesLabelOf(harness.StepResult{Requests: 1}); got != "not_measured" {
		t.Fatalf("no sampler stream rendered as %q: an absent observation is not a number", got)
	}
	if got := samplesLabelOf(harness.StepResult{Samplers: &harness.SamplerSummary{SampleCount: 42}}); got != "42" {
		t.Fatalf("observed density rendered as %q", got)
	}
}

// The invariant at the source level: no live rung line may format sampler
// density as an integer again, because "-1" was exactly how an unmeasured
// stream reached the reader's eye (precedent for a source-level guard:
// internal/model/routing/config_copies_test.go).
func TestLiveRungLinesNeverFormatSamplesAsInt(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if strings.Contains(string(src), "samples=%d") {
		t.Fatal("a live rung line still formats sampler density with an integer verb: an unmeasured stream can only be printed as a number")
	}
}
