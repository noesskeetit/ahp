package harness

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The producer-side rule (Tails) is only half of the contract: acceptance found
// a tail asserted in one field and correctly refused in two others, which means
// some code path wrote a percentile without going through the rule. This gate
// closes it at the boundary every artifact must cross on the way to disk, so a
// future phase or a hand-built step cannot publish a tail on one observation.

func TestWriteOutputsRefusesATailWithoutObservations(t *testing.T) {
	dir := t.TempDir()
	one := 1
	bad := StepResult{Workload: "submit_c1", Concurrency: 1, Requests: 1, Errors: 1,
		TTFBObservations: &one,
		P50:              fptr(761.174), P99: fptr(761.174),
		TTFBP50: fptr(761.174), TTFBP99: fptr(761.174),
		ErrP50: fptr(15000), ErrP99: fptr(15000)}
	rep := &Report{Kind: "smoke", Label: "tail-gate", RunID: "tail-gate",
		Version: VersionInfo{SourceRevision: "x", BinarySHA256: "x"},
		Steps:   []StepResult{bad}}
	paths, err := rep.WriteOutputs(dir, "gate")
	if err != nil {
		t.Fatalf("WriteOutputs: %v", err)
	}
	if rep.Verdict != "FAIL" {
		t.Fatalf("an artifact publishing p99 on 1 observation must not be written as a result (verdict=%q, reasons=%v)", rep.Verdict, rep.VerdictReasons)
	}
	joined := strings.Join(rep.VerdictReasons, " | ")
	for _, want := range []string{"p99_ms", "ttfb_p99_ms", "err_p99_ms"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("reasons must name the offending field %s, got: %s", want, joined)
		}
	}
	if !strings.Contains(joined, "n=1") {
		t.Fatalf("a refusal must state the observation count it refused on, got: %s", joined)
	}
	// The demotion has to be on disk, not only in memory: whoever reads the file
	// must not be able to miss it.
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if !strings.Contains(string(raw), `"verdict": "FAIL"`) {
		t.Fatal("the written artifact does not carry the FAIL verdict")
	}
}

// The control: the same step with enough observations keeps its tails. Without
// this half the gate would only be a ban, and a ban teaches nothing about n.
func TestWriteOutputsKeepsTailsWithEnoughObservations(t *testing.T) {
	dir := t.TempDir()
	hundred := 100
	good := StepResult{Workload: "kv_write", Concurrency: 10, Requests: 400, Errors: 120,
		TTFBObservations: &hundred,
		P50:              fptr(1), P95: fptr(2), P99: fptr(3),
		TTFBP50: fptr(1), TTFBP95: fptr(2), TTFBP99: fptr(3),
		ErrP50: fptr(4), ErrP95: fptr(5), ErrP99: fptr(6)}
	rep := &Report{Kind: "smoke", Label: "tail-gate-ok", RunID: "tail-gate-ok",
		Version: VersionInfo{SourceRevision: "x", BinarySHA256: "x"},
		Steps:   []StepResult{good}}
	if _, err := rep.WriteOutputs(dir, "ok"); err != nil {
		t.Fatalf("WriteOutputs: %v", err)
	}
	for _, x := range rep.VerdictReasons {
		if strings.HasPrefix(x, "tail-gate:") {
			t.Fatalf("legitimate tails refused: %s", x)
		}
	}
}

// End-to-end: drive the REAL recorder at n=1, n=5 and n=20, write the artifact,
// then read the bytes back and assert no field anywhere -- top level, nested, or
// a model_usage-shaped block -- carries a tail the sample size cannot support.
// This is the regression test for "asserted in one field, refused in two".
func TestArtifactCarriesNoTailAnywhereAtSmallN(t *testing.T) {
	dir := t.TempDir()
	var steps []StepResult
	for _, n := range []int{1, 5, 20} {
		rec := NewLatencyRecorder()
		for i := 0; i < n; i++ {
			rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: float64(700 + i), TTFBms: float64(600 + i)})
		}
		steps = append(steps, rec.Summarize("submit_c1", 1, time.Second))
	}
	// The model_usage block is assembled independently of StepResult in the LLM
	// phase, so the artifact is tested with one present, shaped as on disk.
	phaseMap := map[string]any{"phase": "submit_c1", "requests": 1, "p50_ms": steps[0].P50,
		"p95_ms": steps[0].P95, "p99_ms": steps[0].P99, "ttfb_p50_ms": steps[0].TTFBP50,
		"ttfb_p95_ms": steps[0].TTFBP95, "ttfb_p99_ms": steps[0].TTFBP99}
	rep := &Report{Kind: "smoke", Label: "small-n", RunID: "small-n",
		Version: VersionInfo{SourceRevision: "x", BinarySHA256: "x"},
		Steps:   steps,
		Model:   map[string]any{"phases": []any{phaseMap}}}
	paths, err := rep.WriteOutputs(dir, "smalln")
	if err != nil {
		t.Fatalf("WriteOutputs: %v", err)
	}
	raw, err := os.ReadFile(filepath.Clean(paths[0]))
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var offenders []string
	var walk func(node any, path string, owner map[string]any)
	walk = func(node any, path string, owner map[string]any) {
		switch v := node.(type) {
		case map[string]any:
			for k, val := range v {
				if val != nil && (strings.HasSuffix(k, "p95_ms") || strings.HasSuffix(k, "p99_ms")) {
					min := MinSamplesP95
					if strings.HasSuffix(k, "p99_ms") {
						min = MinSamplesP99
					}
					if n := tailCountFor(k, owner); n != nil && *n < min {
						offenders = append(offenders, path+"."+k+" (n="+strconv.Itoa(*n)+", need "+strconv.Itoa(min)+")")
					}
				}
				walk(val, path+"."+k, v)
			}
		case []any:
			for i, el := range v {
				walk(el, path+"["+strconv.Itoa(i)+"]", owner)
			}
		}
	}
	walk(doc, "", nil)
	if len(offenders) > 0 {
		t.Fatalf("artifact publishes a tail its sample size cannot support: %v", offenders)
	}
	// p95 must exist at n=20 -- otherwise the test would pass merely because
	// nothing is ever measured.
	if steps[2].P95 == nil {
		t.Fatalf("n=20 must assert p95 (threshold is 20): %s", steps[2].PercentileNote)
	}
	if steps[2].P99 != nil {
		t.Fatal("n=20 must NOT assert p99")
	}
}

// tailCountFor maps a field to the observation count its family may claim:
// TTFB has its own counter, the rest count requests, err_* count failed calls.
func tailCountFor(field string, obj map[string]any) *int {
	if obj == nil {
		return nil
	}
	if strings.Contains(field, "ttfb") {
		if v, ok := obj["ttfb_observations"].(float64); ok {
			n := int(v)
			return &n
		}
	}
	if strings.HasPrefix(field, "err_") {
		if v, ok := obj["errors"].(float64); ok {
			n := int(v)
			return &n
		}
	}
	if v, ok := obj["requests"].(float64); ok {
		n := int(v)
		return &n
	}
	return nil
}
