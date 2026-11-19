package harness

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// DEF-PROF-3 (root): a metric that was NOT observed must never serialize as 0.
//
// The density smoke proved the failure mode: 17 samples whose whole
// memory-accounting block read "0 MiB", and a cgroup block reading
// "unavailable" -- a reader cannot tell "the process uses no memory" from
// "the sampler never got an answer", and the first reading is a fabricated
// measurement of the exact kind acceptance already rejected twice
// (interim profiles: "CPU delta=0", "usage_tokens=0").
//
// Rules asserted here, in the order acceptance reads them:
//   a collector that failed leaves JSON null and CSV "not_measured";
//   a summary is computed from OBSERVED endpoints only, and is null otherwise;
//   the quality guard refuses a step whose process metrics were never observed;
//   a source that cannot exist on this OS says "unsupported", not 0.

// unobservedSample is what TakeSample returns when no collector answered:
// no platform, no database pool, no redis address, no container names.
func unobservedSample(t *testing.T) Sample {
	t.Helper()
	smp := TakeSample(SamplerConfig{}, nil)
	if len(smp.Errors) > 0 {
		t.Fatalf("expected an idle sample with no sources, got collector errors %v", smp.Errors)
	}
	return smp
}

func jsonMap(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// TestUnobservedSampleSerializesAsNull is the core of DEF-PROF-3: every metric
// of a sample nobody collected must be JSON null. A 0 here is a claim that the
// value was measured and was zero.
func TestUnobservedSampleSerializesAsNull(t *testing.T) {
	m := jsonMap(t, unobservedSample(t))
	keys := []string{
		"proc_cpu_sec", "proc_rss_bytes", "proc_pid",
		"goroutines", "heap_alloc_bytes", "sys_bytes", "stats_rss_bytes",
		"pg_blks_read", "pg_blks_hit", "pg_xact_commit", "pg_db_size_bytes",
		"redis_used_memory", "redis_total_commands_processed",
		"pg_container_block_read_bytes", "minio_block_read_bytes",
		"minio_health_ok", "docker_snapshot_age_ms",
	}
	for _, k := range keys {
		v, ok := m[k]
		if !ok {
			t.Fatalf("sample lost field %q entirely: acceptance cannot see that it was not measured", k)
		}
		if v != nil {
			t.Fatalf("metric %q = %v (%T); an uncollected metric must be null, not %v", k, v, v, v)
		}
	}
	// obs_ms stays a number: the observation itself happened and cost time.
	if m["obs_ms"] == nil {
		t.Fatalf("obs_ms must stay measured -- it is the evidence of the tick cost")
	}
}

// TestUnobservedSampleNamesTheMissingSources: null is honest only when the
// reader can see WHICH collectors produced nothing.
func TestUnobservedSampleNamesTheMissingSources(t *testing.T) {
	m := jsonMap(t, unobservedSample(t))
	raw, ok := m["unobserved"]
	if !ok {
		t.Fatalf("sample has no 'unobserved' block: nulls without a source list are indistinguishable from zeros with a bad collector")
	}
	list, ok := raw.([]any)
	if !ok || len(list) == 0 {
		t.Fatalf("unobserved = %v, want the list of collectors that answered nothing", raw)
	}
	joined := ""
	for _, x := range list {
		joined += toString(x) + " "
	}
	for _, want := range []string{"ps", "debug_stats", "pg", "redis", "docker_stats"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("unobserved list %q does not name collector %q", joined, want)
		}
	}
}

// TestSampleRowMarksUnobservedAsNotMeasured: the CSV is the raw proof acceptance
// reads; a bare 0 in a metric column is read as a measurement.
func TestSampleRowMarksUnobservedAsNotMeasured(t *testing.T) {
	row := sampleRow("overhead", unobservedSample(t))
	metricCols := map[string]int{
		"proc_cpu_sec": 2, "proc_rss_bytes": 3, "goroutines": 4, "heap_alloc_bytes": 5,
		"pg_blks_read": 7, "redis_used_memory": 14, "pg_container_blk_read": 20,
		"minio_blk_read": 25, "minio_health_ok": 26, "proc_pid": 29,
	}
	for name, idx := range metricCols {
		if idx >= len(row) {
			t.Fatalf("sample row has %d columns, no %q", len(row), name)
		}
		if row[idx] != "not_measured" {
			t.Fatalf("column %s = %q, want not_measured (a 0 there reads as a measurement)", name, row[idx])
		}
	}
}

// TestSummaryNeverInventsMetricsFromUnobservedCollectors: same rule one level up.
func TestSummaryNeverInventsMetricsFromUnobservedCollectors(t *testing.T) {
	var samples []Sample
	for i := 0; i < 5; i++ {
		samples = append(samples, Sample{AtUTC: utcAt(i)})
	}
	m := jsonMap(t, SummarizeSamples(samples))
	for _, k := range []string{
		"proc_cpu_sec_delta", "rss_start_bytes", "rss_end_bytes", "rss_max_bytes",
		"heap_start_bytes", "goroutines_start", "goroutines_max",
		"pg_blks_read_delta", "pg_db_size_delta_bytes", "pg_active_backends_max",
		"redis_commands_delta", "redis_used_memory_max",
		"pg_container_block_read_delta_bytes", "minio_block_read_delta_bytes",
		"minio_health_ok_all", "rss_slope_bytes_per_min",
	} {
		v, ok := m[k]
		if !ok {
			t.Fatalf("summary lost field %q", k)
		}
		if v != nil {
			t.Fatalf("summary metric %q = %v; nothing was observed, so it must be null", k, v)
		}
	}
	if m["sample_count"] != float64(5) {
		t.Fatalf("sample_count must stay a real count, got %v", m["sample_count"])
	}
}

// TestQualityGuardRefusesStepWithoutProcessObservations: a step whose CPU/RSS
// columns came from nowhere cannot be part of an accepted profile, no matter
// how many sample points it has.
func TestQualityGuardRefusesStepWithoutProcessObservations(t *testing.T) {
	var samples []Sample
	for i := 0; i < 25; i++ {
		samples = append(samples, Sample{AtUTC: utcAt(i * 2)})
	}
	sum := SummarizeSamples(samples)
	rep := &Report{Kind: "overhead", Steps: []StepResult{{
		Workload: "edge_health", Concurrency: 1, Requests: 1000, Samplers: &sum,
	}}}
	v := rep.QualityViolations(QualityRequirements{MinSamplesPerStep: 20})
	var hit bool
	for _, x := range v {
		if strings.Contains(x, "no process") || strings.Contains(x, "process metrics") || strings.Contains(x, "ps:") {
			hit = true
		}
	}
	if !hit {
		t.Fatalf("guard accepted a step with 25 samples and zero process observations; violations=%v", v)
	}
}

// utcAt builds a stable observation timestamp; seconds offset from a fixed base.
func utcAt(sec int) string {
	return time.Unix(1762704000, 0).UTC().Add(time.Duration(sec) * time.Second).Format(time.RFC3339Nano)
}

// pointer constructors used by every harness test that builds a Sample by hand.
func u64(v uint64) *uint64 { return &v }
func i64(v int64) *int64   { return &v }
func ip(v int) *int        { return &v }

func toString(v any) string {
	s, _ := v.(string)
	return s
}

// TestSummaryUsesOnlyObservedEndpoints: when the process collector answers only
// from the third sample on, RSS start/end/max and the slope must come from the
// observed points. Anchoring on the leading unobserved samples invents a
// 200 MiB ramp that never happened.
func TestSummaryUsesOnlyObservedEndpoints(t *testing.T) {
	mk := func(min int, rss uint64) Sample {
		s := Sample{AtUTC: utcAt(min * 60)}
		if rss > 0 {
			s.ProcRSS = u64(rss)
		}
		return s
	}
	sum := SummarizeSamples([]Sample{mk(0, 0), mk(1, 0), mk(2, 100<<20), mk(3, 300<<20)})
	m := jsonMap(t, sum)
	if got := m["rss_start_bytes"]; got != float64(100<<20) {
		t.Fatalf("rss_start_bytes = %v, want 104857600: a delta must start at the first OBSERVED sample, not at an unobserved 0", got)
	}
	if got := m["rss_end_bytes"]; got != float64(300<<20) {
		t.Fatalf("rss_end_bytes = %v, want 314572800", got)
	}
	if got := m["rss_max_bytes"]; got != float64(300<<20) {
		t.Fatalf("rss_max_bytes = %v, want 314572800", got)
	}
	if sum.SourceObservations[SourceProcess] != 2 || sum.SourceObservations[SourceDebugStats] != 0 {
		t.Fatalf("observation counts must say ps ran twice and debug_stats never: %v", sum.SourceObservations)
	}
	// Two observed points cannot carry a slope, even though the stream has 4.
	if sum.RSSSlopeBytesPerMin != nil {
		t.Fatalf("slope invented from 2 observations: %v", *sum.RSSSlopeBytesPerMin)
	}
	if sum.ProcCPUSecDelta != nil {
		t.Fatalf("cpu delta invented with no cpu observations: %v", *sum.ProcCPUSecDelta)
	}
}

// TestObservedCountsComeFromValues: a source counts as observed only where a
// value exists -- markers alone may not make zeros look measured.
func TestObservedCountsComeFromValues(t *testing.T) {
	samples := []Sample{
		{AtUTC: utcAt(0)},
		{AtUTC: utcAt(2), RedisUsedMemory: i64(1 << 20)},
		{AtUTC: utcAt(4), RedisUsedMemory: i64(2 << 20)},
	}
	got := SourceObservationCounts(samples)
	if got[SourceRedis] != 2 {
		t.Fatalf("redis observations = %d, want 2", got[SourceRedis])
	}
	if got[SourceProcess] != 0 || got[SourceMinio] != 0 {
		t.Fatalf("sources with no values must count zero: %v", got)
	}
}

// TestGuardRefusesAggregatesWithoutSamples is the single-shot artifact's second
// defect: summary.sample_count=0 printed next to RSS/heap/PG/Redis aggregates.
func TestGuardRefusesAggregatesWithoutSamples(t *testing.T) {
	invented := &SamplerSummary{SampleCount: 0, RSSStartBytes: u64(1 << 20), PGBlksReadDelta: i64(7)}
	rep := &Report{Kind: "overhead", Steps: []StepResult{{Workload: "edge_health", Concurrency: 1, Requests: 30, Samplers: invented}}}
	var hit bool
	for _, x := range rep.QualityViolations(QualityRequirements{MinSamplesPerStep: 20}) {
		if strings.Contains(x, "sample_count=0 but") {
			hit = true
		}
	}
	if !hit {
		t.Fatalf("guard accepted aggregates on a stream with no samples")
	}
	// An honest empty summary is not a violation of this rule.
	if n := SummarizeSamples(nil).inventedAggregateCount(); n != 0 {
		t.Fatalf("an empty summary must carry no metrics, carries %d", n)
	}
}

// TestArtifactDeclaresQuotaAndMeasuredProcess: the two provenance blocks
// acceptance asked for must be in the artifact, with the state vocabulary.
func TestArtifactDeclaresQuotaAndMeasuredProcess(t *testing.T) {
	rep := &Report{Kind: "overhead", Machine: CollectMachine(),
		Instance: InstanceInfo{MeasuredProcess: DescribeProcess(os.Getpid(), PlatformRole)}}
	m := jsonMap(t, rep.Machine)
	limits, ok := m["resource_limits"].([]any)
	if !ok || len(limits) == 0 {
		t.Fatalf("machine block carries no resource_limits: %v", m["resource_limits"])
	}
	pm := jsonMap(t, rep.Instance.MeasuredProcess)
	if pm["pid"] == nil || pm["pid"].(float64) <= 0 {
		t.Fatalf("measured_process must name the pid: %v", pm)
	}
	if pm["role"] != PlatformRole {
		t.Fatalf("measured_process.role = %v, want %q (one pid covering gateway+runner+data is a claim the artifact must state)", pm["role"], PlatformRole)
	}
	if pm["child_pids_state"] != "measured" {
		t.Fatalf("child_pids_state must be an observation, got %v", pm["child_pids_state"])
	}
}
