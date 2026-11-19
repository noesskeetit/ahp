package harness

import (
	"fmt"
	"os"
	"time"
)

// SelfCheckArtifact writes an artifact whose ONLY purpose is to prove the
// artifact contract on disk (metrics_contract.go, DEF-PROF-3; tail rule in
// Tails; provenance blocks). It is SYNTHETIC: the figures come from a stream
// where no collector answered, precisely so that every metric must serialize as
// null. A reviewer who wants a measurement must read a measurement run.
//
// Why this exists: acceptance reads raw files and has twice been handed a
// summary whose zeros looked like measurements. A writer-level fixture that
// says "an unmeasured metric is null here, and the quota block says
// unsupported" is the cheapest thing that makes the contract checkable without
// burning a measurement window.
func SelfCheckArtifact(dir, runID string) ([]string, error) {
	if dir == "" {
		return nil, fmt.Errorf("harness: no evidence directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	emptySample := TakeSample(SamplerConfig{}, nil)
	emptySample.AtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	emptySample.Errors = nil
	sum := SummarizeSamples([]Sample{emptySample})
	oneCall := StepResult{Workload: "selfcheck_single_call", Concurrency: 1, Requests: 1, DurationSec: 0.761174}
	tails := Tails([]float64{761.174}, &oneCall.PercentileNote)
	tm := 761.174
	oneCall.P50, oneCall.P95, oneCall.P99, oneCall.Mean, oneCall.Max = tails.P50, tails.P95, tails.P99, &tm, &tm
	tt := Tails([]float64{761.174}, &oneCall.TTFBPercentileNote)
	oneCall.TTFBP50, oneCall.TTFBP95, oneCall.TTFBP99 = tt.P50, tt.P95, tt.P99
	rep := &Report{
		Kind:           "selfcheck",
		Label:          runID,
		Methodology:    "CONTRACT SELF-CHECK (synthetic): nothing was measured; every metric must be null and every unavailable source must say unsupported/unavailable",
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
		RunID:          runID,
		RunIDSource:    RunIDGenerated,
		Machine:        CollectMachine(),
		Version:        VersionInfo{SourceRevision: "selfcheck", BinarySHA256: "selfcheck", ProductRevision: "selfcheck"},
		Instance:       InstanceInfo{MeasuredProcess: DescribeProcess(os.Getpid(), PlatformRole)},
		Steps:          []StepResult{oneCall},
		Summary:        &sum,
		Samples:        []Sample{emptySample},
		Notes: []string{
			"synthetic fixture: p50 of one observation is the observation; p95/p99 are null by the declared tail rule (n=1 < 20, < 100)",
			"synthetic fixture: no collector ran, so every sampler metric is null and unobserved_sources names ps/debug_stats/pg/redis/docker_stats/minio_health",
		},
	}
	paths, err := rep.WriteOutputs(dir, "profiling-contract-selfcheck")
	if err != nil {
		return nil, err
	}
	for _, name := range []string{"-steps.csv", "-samples.csv"} {
		p := dir + "/profiling-contract-selfcheck" + name
		if _, err := os.Stat(p); err != nil {
			return paths, fmt.Errorf("harness: self-check artifact missing %s: %w", name, err)
		}
	}
	return paths, nil
}
