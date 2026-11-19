package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The artifact contract must be provable on disk, not only in memory: this
// writes the synthetic self-check and reads the JSON back the way acceptance
// does.
func TestSelfCheckArtifactCarriesTheNullContract(t *testing.T) {
	dir := t.TempDir()
	if out := os.Getenv("AHP_HARNESS_EVIDENCE_DIR"); out != "" {
		dir = out
	}
	paths, err := SelfCheckArtifact(dir, "selfcheck-"+t.Name())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "profiling-contract-selfcheck.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("artifact is not valid JSON: %v", err)
	}
	sum, ok := doc["summary"].(map[string]any)
	if !ok {
		t.Fatalf("no summary block")
	}
	for _, k := range []string{"rss_start_bytes", "pg_blks_read_delta", "redis_used_memory_max", "minio_health_ok_all", "goroutines_start"} {
		if sum[k] != nil {
			t.Fatalf("summary.%s = %v; a synthetic stream with no collector must serialize null", k, sum[k])
		}
	}
	if sum["sample_count"] != float64(1) {
		t.Fatalf("sample_count must stay truthful: %v", sum["sample_count"])
	}
	step := doc["steps"].([]any)[0].(map[string]any)
	for _, k := range []string{"p95_ms", "p99_ms", "ttfb_p95_ms", "ttfb_p99_ms"} {
		if step[k] != nil {
			t.Fatalf("step.%s = %v on n=1: the tail rule must hold in the written artifact", k, step[k])
		}
	}
	if step["p50_ms"] == nil || step["percentile_note"] == "" {
		t.Fatalf("p50 and its explanation must survive to disk: %v / %q", step["p50_ms"], step["percentile_note"])
	}
	machine := doc["machine"].(map[string]any)
	limits, ok := machine["resource_limits"].([]any)
	if !ok || len(limits) == 0 {
		t.Fatalf("machine.resource_limits missing: the artifact may not be silent about quotas")
	}
	var sawUnsupported bool
	for _, l := range limits {
		lm := l.(map[string]any)
		state, _ := lm["state"].(string)
		if state != "measured" && state != "unsupported" && state != "unavailable" {
			t.Fatalf("resource limit state %q outside the contract", state)
		}
		if state == "unsupported" {
			sawUnsupported = true
			if r, _ := lm["reason"].(string); r == "" {
				t.Fatalf("unsupported without a reason: %v", lm)
			}
		}
	}
	if !sawUnsupported {
		t.Skipf("no unsupported quota on this OS (linux reads real cgroups): states checked on %d entries", len(limits))
	}
	proc := doc["instance"].(map[string]any)["measured_process"].(map[string]any)
	if proc["pid"].(float64) <= 0 || proc["child_pids_state"] != "measured" {
		t.Fatalf("measured_process incomplete: %v", proc)
	}
	samples := strings.Split(strings.TrimSpace(string(readFileForTest(t, filepath.Join(dir, "profiling-contract-selfcheck-samples.csv")))), "\n")
	if !strings.Contains(samples[len(samples)-1], "not_measured") || !strings.Contains(samples[len(samples)-1], "ps:not_configured") {
		t.Fatalf("samples.csv must mark unmeasured cells and name the silent collectors, got %q", samples[len(samples)-1])
	}
	t.Logf("self-check artifact: %v", paths)
}

func readFileForTest(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
