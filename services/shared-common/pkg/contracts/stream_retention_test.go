package contracts

import "testing"

func TestStreamRetentionForParsesOnlyResultsOnly(t *testing.T) {
	for raw, want := range map[string]StreamRetention{
		"":               StreamRetentionJournal,
		"   ":            StreamRetentionJournal,
		"journal":        StreamRetentionJournal,
		"results-only":   StreamRetentionResultsOnly,
		"  Results-Only": StreamRetentionResultsOnly,
		"chunk":          StreamRetentionJournal, // typo / wrong value -> safe default
		"results_only":   StreamRetentionJournal, // underscore is NOT the keyword
	} {
		if got := StreamRetentionFor(raw); got != want {
			t.Fatalf("StreamRetentionFor(%q)=%q want %q", raw, got, want)
		}
	}
}

func TestResultsOnlyRequiresVolatileTailBoundary(t *testing.T) {
	t.Setenv(EnvStreamRetention, "results-only")
	// Default chunk boundary has no volatile tail -> the retention selector alone
	// must never change default behaviour.
	t.Setenv(EnvCheckpointBoundary, "")
	if ResultsOnlyRetention() {
		t.Fatal("results-only must be inert when the boundary is chunk")
	}
	for _, b := range []string{"step", "turn"} {
		t.Setenv(EnvCheckpointBoundary, b)
		if !ResultsOnlyRetention() {
			t.Fatalf("results-only must activate with boundary %q", b)
		}
	}
	// A journal retention with a step boundary stays journal (controls preserved).
	t.Setenv(EnvStreamRetention, "")
	t.Setenv(EnvCheckpointBoundary, "step")
	if ResultsOnlyRetention() {
		t.Fatal("unset retention must stay journal with a step boundary")
	}
}
