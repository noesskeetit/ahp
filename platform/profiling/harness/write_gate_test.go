package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The rule "no sampler stream => the report is not a result" must be an
// invariant of WRITING an artifact, not a courtesy the caller can skip: the
// rejected overhead run proved a report can reach disk with steps that had no
// stream at all. WriteOutputs itself now enforces it for every kind that is
// designed to sample.
func TestWriteOutputsEnforcesSamplerStreamRule(t *testing.T) {
	for _, kind := range []string{"overhead", "recovery", "soak", "llm", "depfail"} {
		dir := t.TempDir()
		r := &Report{Kind: kind, Label: "x", RunID: "r", Verdict: "PASS", Notes: []string{},
			Version: VersionInfo{ProductRevision: "sampler-rule-fixture"},
			Steps:   []StepResult{{Workload: "w", Concurrency: 1, Requests: 100}}}
		if _, err := r.WriteOutputs(dir, "art"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "art.json"))
		if err != nil {
			t.Fatal(err)
		}
		var back Report
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		if back.Verdict != "FAIL" {
			t.Fatalf("kind %s: a step without a sampler stream must never be written as %s", kind, back.Verdict)
		}
		if !strings.Contains(strings.Join(back.VerdictReasons, "|"), "no sampler stream") {
			t.Fatalf("kind %s: written reasons must say why: %v", kind, back.VerdictReasons)
		}
	}
	// A reporting kind that runs WITH a stream keeps its verdict untouched.
	dir := t.TempDir()
	r := &Report{Kind: "overhead", Label: "x", RunID: "r", Verdict: "PASS", Notes: []string{},
		Version: VersionInfo{ProductRevision: "sampler-rule-fixture"},
		Steps:   []StepResult{{Workload: "w", Concurrency: 1, Requests: 100, Samplers: denseSummary(50)}}}
	if _, err := r.WriteOutputs(dir, "art"); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "art.json"))
	var back Report
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Verdict != "PASS" {
		t.Fatalf("a streamed report must keep its verdict: %s %v", back.Verdict, back.VerdictReasons)
	}
}
