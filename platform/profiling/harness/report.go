package harness

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Report is the top-level artifact of one harness phase.
type Report struct {
	Kind           string `json:"kind"` // overhead|recovery|soak|depfail|llm-e2e|smoke
	Label          string `json:"label"`
	Methodology    string `json:"methodology"`
	GeneratedAtUTC string `json:"generated_at_utc"`
	RunID          string `json:"run_id"`
	// RunIDSource says whether this id came from the run wrapper (so the run
	// log carries the same id) or was invented here (so it does not, and the
	// artifact is only half a provenance chain).
	RunIDSource string          `json:"run_id_source"`
	Machine     MachineInfo     `json:"machine"`
	Version     VersionInfo     `json:"version"`
	LoadProfile any             `json:"load_profile"`
	Instance    InstanceInfo    `json:"instance"`
	Steps       []StepResult    `json:"steps"`
	Summary     *SamplerSummary `json:"summary"`
	Phases      []SubPhase      `json:"phases,omitempty"`
	Model       any             `json:"model_usage,omitempty"`
	// Authorization records how each plane and each bootstrapped session was
	// authorized: a live product exchange, never a harness self-sign. It is a
	// ledger (per plane + per session, with live rotation counts), not a single
	// field a later session could overwrite. Absent only for the no-load idle
	// reference, which makes no product call.
	Authorization *AuthorizationLedger `json:"authorization_composition,omitempty"`
	// Verdict is the run-level machine verdict of the phase accounting
	// (DEF-PROF-1): never PASS while any counted phase carries terminal
	// failures/blocked or logical errors. Empty when the kind performs no
	// generation accounting.
	Verdict        string   `json:"verdict,omitempty"`
	VerdictReasons []string `json:"verdict_reasons,omitempty"`
	Notes          []string `json:"notes"`
	Samples        []Sample `json:"-"`
}

// InstanceInfo identifies the isolated measurement instance.
type InstanceInfo struct {
	GatewayPort int    `json:"gateway_port"`
	Tenant      string `json:"tenant"`
	Deployment  string `json:"deployment"`
	Database    string `json:"database"`
	TLS         string `json:"tls"`
	LiveStand   string `json:"live_stand_untouched"`
	// MeasuredProcess names the pid whose CPU/RSS the numbers describe, the
	// role it was launched with, and whether it has children whose memory is
	// NOT in those numbers (DEF-PROF-3: platform vs measured runner identity).
	MeasuredProcess ProcessDescription `json:"measured_process"`
}

// SubPhase is a named block of steps with its own sampler stream.
type SubPhase struct {
	Name    string       `json:"name"`
	Steps   []StepResult `json:"steps"`
	Samples []Sample     `json:"-"`
}

// EnforceSamplerStreamRule is the write-time invariant: for every kind that
// is designed to run a sampler stream, a step with no stream makes the whole
// artifact invalid -- it is demoted to FAIL on the way to disk, even if the
// caller never invoked the quality guard. A measurement of platform behavior
// without platform samples is not a measurement.
// EnforceProductRevision is a root acceptance gate: every profiling artifact must
// declare a non-empty product_revision so its numbers cannot be read as belonging
// to the wrong candidate. An empty one demotes the artifact to FAIL and says how to
// fix it -- acceptance refuses to reconcile an anonymous artifact by hand.
func (r *Report) EnforceProductRevision() {
	if strings.TrimSpace(r.Version.ProductRevision) == "" {
		r.VerdictReasons = append(r.VerdictReasons, "product_revision is empty: every profiling artifact must bind its numbers to a candidate revision (pass -product-rev, or measure a binary stamped with vcs.revision); an empty product_revision forces acceptance to reconcile the artifact manually, which is disallowed")
		if r.Verdict == "" || r.Verdict == "PASS" {
			r.Verdict = "FAIL"
		}
	}
}

func (r *Report) EnforceSamplerStreamRule() {
	switch r.Kind {
	case "overhead", "recovery", "soak", "llm", "llm-e2e", "depfail":
	default:
		return
	}
	steps := append([]StepResult{}, r.Steps...)
	for _, p := range r.Phases {
		steps = append(steps, p.Steps...)
	}
	if len(steps) == 0 {
		return
	}
	missing := 0
	for _, s := range steps {
		if s.Samplers == nil {
			missing++
		}
	}
	if missing == 0 {
		return
	}
	r.VerdictReasons = append(r.VerdictReasons, fmt.Sprintf("sampler-gate: %d of %d steps have no sampler stream: the report is not a result", missing, len(steps)))
	r.Verdict = "FAIL"
}

// EnforceTailSampleRule is the second write-time invariant: a tail quantile may
// not exist in the artifact unless the family behind it has enough observations
// (thresholds: metrics_contract.go). Tails applies the rule for every producer
// today; this gate is what keeps a future phase, or a hand-built step, from
// publishing "ttfb_p99_ms=761.174" on a step that made ONE request while its
// siblings correctly reported null. The artifact is demoted to FAIL and says
// which field, which n and which threshold -- a producer bug must not become a
// published number.
func (r *Report) EnforceTailSampleRule() {
	steps := append([]StepResult{}, r.Steps...)
	for _, p := range r.Phases {
		steps = append(steps, p.Steps...)
	}
	for _, s := range steps {
		tag := fmt.Sprintf("%s c=%d", s.Workload, s.Concurrency)
		ttfbN := s.Requests
		if s.TTFBObservations != nil {
			ttfbN = *s.TTFBObservations
		}
		for _, q := range []struct {
			name string
			val  *float64
			n    int
			min  int
		}{
			{"p95_ms", s.P95, s.Requests, MinSamplesP95},
			{"p99_ms", s.P99, s.Requests, MinSamplesP99},
			{"ttfb_p95_ms", s.TTFBP95, ttfbN, MinSamplesP95},
			{"ttfb_p99_ms", s.TTFBP99, ttfbN, MinSamplesP99},
			{"err_p95_ms", s.ErrP95, s.Errors, MinSamplesP95},
			{"err_p99_ms", s.ErrP99, s.Errors, MinSamplesP99},
			{"all_p95_ms", s.AllP95, s.Requests, MinSamplesP95},
			{"all_p99_ms", s.AllP99, s.Requests, MinSamplesP99},
		} {
			if q.val != nil && q.n < q.min {
				r.VerdictReasons = append(r.VerdictReasons, fmt.Sprintf("tail-gate: step %s publishes %s=%.3f on n=%d observations (<%d): at this n the figure is the maximum, not a tail -- it must be null", tag, q.name, *q.val, q.n, q.min))
				r.Verdict = "FAIL"
			}
		}
	}
}

// WriteOutputs writes <stem>.json, <stem>-steps.csv, <stem>-samples.csv.
func (r *Report) WriteOutputs(outDir, stem string) ([]string, error) {
	r.EnforceSamplerStreamRule()
	r.EnforceTailSampleRule()
	if r.Authorization != nil {
		// Fill each session's live rotation counts now, so a long phase reports the
		// rotations it actually performed, not the zero at session-creation time.
		r.Authorization.materialize()
	}
	r.EnforceProductRevision()
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, err
	}
	var written []string
	jsonPath := filepath.Join(outDir, stem+".json")
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(jsonPath, append(data, '\n'), 0o644); err != nil {
		return nil, err
	}
	written = append(written, jsonPath)

	stepsPath := filepath.Join(outDir, stem+"-steps.csv")
	f, err := os.Create(stepsPath)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(f)
	header := []string{"phase", "workload", "concurrency", "duration_s", "requests", "errors", "error_rate_pct", "throughput_rps",
		"p50_ms", "p95_ms", "p99_ms", "mean_ms", "max_ms", "ttfb_p50_ms", "ttfb_p95_ms", "ttfb_p99_ms", "resp_bytes", "error_classes",
		"err_p50_ms", "err_p95_ms", "err_p99_ms", "err_max_ms", "repeat", "sessions_mode", "load_pattern", "cache_state", "pg_hit_ratio", "offered_requests", "dropped_at_capacity", "max_queue_depth", "proc_pid", "proc_pid_changes", "process_identity_changed", "percentile_note",
		"all_p50_ms", "all_p95_ms", "all_p99_ms", "all_observations", "all_percentile_note"}
	if err := w.Write(header); err != nil {
		f.Close()
		return nil, err
	}
	emit := func(pname string, s StepResult) error {
		row := []string{pname, s.Workload, strconv.Itoa(s.Concurrency), f2(s.DurationSec), strconv.Itoa(s.Requests), strconv.Itoa(s.Errors), f2(s.ErrorRatePct), f2(s.ThroughputRPS),
			f2ptr(s.P50), f2ptr(s.P95), f2ptr(s.P99), f2ptr(s.Mean), f2ptr(s.Max), f2ptr(s.TTFBP50), f2ptr(s.TTFBP95), f2ptr(s.TTFBP99), strconv.FormatInt(s.TotalBytes, 10), fmt.Sprint(s.ErrorClasses),
			f2ptr(s.ErrP50), f2ptr(s.ErrP95), f2ptr(s.ErrP99), f2ptr(s.ErrMax), strconv.Itoa(s.Repeat), s.Sessions, s.LoadPattern, cacheLabel(s), cacheRatio(s), intptr(s.Offered), intptr(s.Dropped), intptr(s.MaxQueue), pidOf(s), intptr(pidChangesOf(s)), strconv.FormatBool(identityChanged(s)), s.PercentileNote,
			f2ptr(s.AllP50), f2ptr(s.AllP95), f2ptr(s.AllP99), intptr(s.AllObservations), s.AllPercentileNote}
		return w.Write(row)
	}
	for _, s := range r.Steps {
		if err := emit(r.Kind, s); err != nil {
			f.Close()
			return nil, err
		}
	}
	for _, p := range r.Phases {
		for _, s := range p.Steps {
			if err := emit(p.Name, s); err != nil {
				f.Close()
				return nil, err
			}
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()
	written = append(written, stepsPath)

	var samples []Sample
	samples = append(samples, r.Samples...)
	for _, p := range r.Phases {
		samples = append(samples, p.Samples...)
	}
	if len(samples) > 0 {
		sPath := filepath.Join(outDir, stem+"-samples.csv")
		sf, err := os.Create(sPath)
		if err != nil {
			return written, err
		}
		sw := csv.NewWriter(sf)
		_ = sw.Write([]string{"at_utc", "phase", "proc_cpu_sec", "proc_rss_bytes", "goroutines", "heap_alloc_bytes", "sys_bytes",
			"pg_blks_read", "pg_blks_hit", "pg_xact_commit", "pg_temp_bytes", "pg_deadlocks", "pg_active_backends", "pg_db_size_bytes",
			"redis_used_memory", "redis_connected_clients", "redis_total_commands", "redis_ops", "redis_net_in", "redis_net_out",
			"pg_container_blk_read", "pg_container_blk_write", "pg_container_net_rx", "pg_container_net_tx",
			"minio_blk_read", "minio_blk_write", "minio_health_ok", "obs_ms", "docker_snapshot_age_ms", "proc_pid", "proc_identity_mismatch", "collector_errors", "unobserved_sources"})
		for _, s := range r.Samples {
			_ = sw.Write(sampleRow("", s))
		}
		for _, p := range r.Phases {
			for _, s := range p.Samples {
				_ = sw.Write(sampleRow(p.Name, s))
			}
		}
		sw.Flush()
		err = sw.Error()
		sf.Close()
		if err != nil {
			return written, err
		}
		written = append(written, sPath)
	}
	return written, nil
}

// pidOf / pidChangesOf / identityChanged expose the sampled process identity
// of a step through the flat CSV, where acceptance checks attribution.
func pidOf(s StepResult) string {
	if s.Samplers == nil || s.Samplers.ProcPID == nil {
		return "not_recorded"
	}
	return strconv.Itoa(*s.Samplers.ProcPID)
}

func pidChangesOf(s StepResult) *int {
	if s.Samplers == nil {
		return nil
	}
	n := s.Samplers.ProcPIDChanges
	return &n
}

func identityChanged(s StepResult) bool {
	return s.Samplers != nil && s.Samplers.ProcessIdentityChanged
}

func sampleRow(phase string, s Sample) []string {
	// Every metric cell goes through a cell* renderer: an unobserved metric
	// prints "not_measured", never 0 (metrics_contract.go / DEF-PROF-3).
	return []string{s.AtUTC, phase, f2ptr(s.ProcCPUSec), cellU64(s.ProcRSS), cellInt(s.Goroutines),
		cellU64(s.HeapAlloc), cellU64(s.SysBytes),
		cellI64(s.PGBlksRead), cellI64(s.PGBlksHit), cellI64(s.PGxactCommit),
		cellI64(s.PGTempBytes), cellI64(s.PGDeadlocks), cellI64(s.PGActiveBackends), cellI64(s.PGDBSizeBytes),
		cellI64(s.RedisUsedMemory), cellI64(s.RedisConnectedClients), cellI64(s.RedisTotalCommands), cellI64(s.RedisInstantaneousOPS),
		cellI64(s.RedisNetInputBytes), cellI64(s.RedisNetOutputBytes),
		cellI64(s.PGContainerBlockRead), cellI64(s.PGContainerBlockWrite), cellI64(s.PGContainerNetRx), cellI64(s.PGContainerNetTx),
		cellI64(s.MinioBlockRead), cellI64(s.MinioBlockWrite), cellBool(s.MinioHealthOK),
		strconv.FormatInt(s.ObsMS, 10), cellI64(s.DockerSnapshotAgeMS), cellInt(s.ProcPID),
		strconv.FormatBool(s.ProcIdentityMismatch), strings.Join(s.Errors, ";"), strings.Join(s.Unobserved, ";")}
}

// SamplerSummary condenses a sample stream: gauges as start/end/max, counters
// as first-to-last deltas, plus linear slopes for leak analysis.
//
// All metrics are pointers (metrics_contract.go, DEF-PROF-3): null means the
// underlying collector never answered during this step, and a 0 is a real
// observation of zero. Deltas/slopes are computed over the OBSERVED points of
// their own source, so a stream whose ps collector only worked from the third
// sample onwards cannot report a memory ramp that starts at an unobserved 0.
type SamplerSummary struct {
	SampleCount int    `json:"sample_count"`
	FirstUTC    string `json:"first_utc"`
	LastUTC     string `json:"last_utc"`
	// TickIntervalMS is the configured sampler tick for the stream these
	// samples came from; with sample_count it proves the density rule
	// (count >= floor(T/I)-1) instead of asking acceptance to trust it.
	TickIntervalMS *int64 `json:"tick_interval_ms"`
	// ObsP95MS is the 95th percentile duration of a single observation.
	// Density is only sustainable when observation cost << tick.
	ObsP95MS *float64 `json:"obs_p95_ms"`
	// SourceObservations counts, per collector, how many of SampleCount
	// samples that collector contributed. A metric whose collector shows 0 is
	// not measured for this step, and the quality guard says so instead of
	// letting zeros stand in for platform behaviour.
	SourceObservations map[string]int `json:"source_observations"`
	// ProcCPUSecDelta is null when fewer than two OBSERVED samples exist: a
	// delta from one endpoint is a fabricated measurement (the rejected
	// interim profile reported "CPU delta=0" built on sample_count=1).
	ProcCPUSecDelta *float64 `json:"proc_cpu_sec_delta"`
	// ProcPID is the single process these samples describe; null when no
	// process was observed at all. ProcPIDChanges counts observations taken
	// from any other pid, and ProcCPURegressions counts samples where
	// cumulative cpu-time went DOWN (impossible for one process -- proof of an
	// identity switch). With a change, rss/cpu deltas describe no single
	// process and the guard refuses the step.
	ProcPID                *int    `json:"proc_pid"`
	ProcPIDChanges         int     `json:"proc_pid_changes"`
	ProcCPURegressions     int     `json:"proc_cpu_regressions"`
	ProcessIdentityChanged bool    `json:"process_identity_changed"`
	RSSStartBytes          *uint64 `json:"rss_start_bytes"`
	RSSEndBytes            *uint64 `json:"rss_end_bytes"`
	RSSMaxBytes            *uint64 `json:"rss_max_bytes"`
	HeapStartBytes         *uint64 `json:"heap_start_bytes"`
	HeapEndBytes           *uint64 `json:"heap_end_bytes"`
	GoroutinesStart        *int    `json:"goroutines_start"`
	GoroutinesEnd          *int    `json:"goroutines_end"`
	GoroutinesMax          *int    `json:"goroutines_max"`
	PGBlksReadDelta        *int64  `json:"pg_blks_read_delta"`
	PGBlksHitDelta         *int64  `json:"pg_blks_hit_delta"`
	PGxactCommitDelta      *int64  `json:"pg_xact_commit_delta"`
	PGTempBytesDelta       *int64  `json:"pg_temp_bytes_delta"`
	PGDeadlocksDelta       *int64  `json:"pg_deadlocks_delta"`
	PGDBSizeDeltaBytes     *int64  `json:"pg_db_size_delta_bytes"`
	PGActiveBackendsMax    *int64  `json:"pg_active_backends_max"`
	RedisCommandsDelta     *int64  `json:"redis_commands_delta"`
	RedisNetInDelta        *int64  `json:"redis_net_in_delta_bytes"`
	RedisNetOutDelta       *int64  `json:"redis_net_out_delta_bytes"`
	RedisMaxMemory         *int64  `json:"redis_used_memory_max"`
	PGBlockReadDelta       *int64  `json:"pg_container_block_read_delta_bytes"`
	PGBlockWriteDelta      *int64  `json:"pg_container_block_write_delta_bytes"`
	PGNetRxDelta           *int64  `json:"pg_container_net_rx_delta_bytes"`
	PGNetTxDelta           *int64  `json:"pg_container_net_tx_delta_bytes"`
	MinioBlockReadDelta    *int64  `json:"minio_block_read_delta_bytes"`
	MinioBlockWriteDelta   *int64  `json:"minio_block_write_delta_bytes"`
	// MinioHealthAllOK is null when MinIO was never probed in this window --
	// "all probes healthy" is a claim, not a default.
	MinioHealthAllOK *bool `json:"minio_health_ok_all"`
	// Slopes are null when there are too few OBSERVED points to fit one: the
	// old -1 read like "memory is reclaimed at -1 bytes/min", a lie with units.
	RSSSlopeBytesPerMin   *float64 `json:"rss_slope_bytes_per_min"`
	GoroutinesSlopePerMin *float64 `json:"goroutines_slope_per_min"`
}

// SummarizeSamples computes the summary for a stream.
func SummarizeSamples(samples []Sample) SamplerSummary {
	s := SamplerSummary{SampleCount: len(samples)}
	if len(samples) == 0 {
		return s
	}
	s.SourceObservations = SourceObservationCounts(samples)
	obsCosts := make([]float64, 0, len(samples))
	for _, x := range samples {
		obsCosts = append(obsCosts, float64(x.ObsMS))
	}
	sort.Float64s(obsCosts)
	s.ObsP95MS = fptr(pct(obsCosts, 0.95))
	s.FirstUTC, s.LastUTC = samples[0].AtUTC, samples[len(samples)-1].AtUTC
	// Identity: pin the first observed pid, then count every observation that
	// disagrees with it -- a different pid, or cumulative cpu going backwards.
	var prevCPU *float64
	for _, x := range samples {
		if x.ProcPID != nil && *x.ProcPID > 0 {
			if s.ProcPID == nil {
				pid := *x.ProcPID
				s.ProcPID = &pid
			} else if *x.ProcPID != *s.ProcPID {
				s.ProcPIDChanges++
			}
		}
		if prevCPU != nil && x.ProcCPUSec != nil && *x.ProcCPUSec < *prevCPU {
			s.ProcCPURegressions++
		}
		if x.ProcCPUSec != nil {
			prevCPU = x.ProcCPUSec
		}
	}
	s.ProcessIdentityChanged = s.ProcPIDChanges > 0 || s.ProcCPURegressions > 0
	// Metrics: each source is summarized over its own observed points.
	s.ProcCPUSecDelta = deltaObserved(samples, func(x Sample) *float64 { return x.ProcCPUSec })
	rss := func(x Sample) *uint64 { return x.ProcRSS }
	heap := func(x Sample) *uint64 { return x.HeapAlloc }
	gr := func(x Sample) *int { return x.Goroutines }
	s.RSSStartBytes, s.RSSEndBytes, s.RSSMaxBytes = firstObserved(samples, rss), lastObserved(samples, rss), maxObserved(samples, rss)
	s.HeapStartBytes, s.HeapEndBytes = firstObserved(samples, heap), lastObserved(samples, heap)
	s.GoroutinesStart, s.GoroutinesEnd, s.GoroutinesMax = firstObserved(samples, gr), lastObserved(samples, gr), maxObserved(samples, gr)
	s.RSSSlopeBytesPerMin = slopeObserved(samples, rss)
	s.GoroutinesSlopePerMin = slopeObserved(samples, gr)
	s.PGBlksReadDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGBlksRead })
	s.PGBlksHitDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGBlksHit })
	s.PGxactCommitDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGxactCommit })
	s.PGTempBytesDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGTempBytes })
	s.PGDeadlocksDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGDeadlocks })
	s.PGDBSizeDeltaBytes = deltaObserved(samples, func(x Sample) *int64 { return x.PGDBSizeBytes })
	s.PGActiveBackendsMax = maxObserved(samples, func(x Sample) *int64 { return x.PGActiveBackends })
	s.RedisCommandsDelta = deltaObserved(samples, func(x Sample) *int64 { return x.RedisTotalCommands })
	s.RedisNetInDelta = deltaObserved(samples, func(x Sample) *int64 { return x.RedisNetInputBytes })
	s.RedisNetOutDelta = deltaObserved(samples, func(x Sample) *int64 { return x.RedisNetOutputBytes })
	s.RedisMaxMemory = maxObserved(samples, func(x Sample) *int64 { return x.RedisUsedMemory })
	s.PGBlockReadDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGContainerBlockRead })
	s.PGBlockWriteDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGContainerBlockWrite })
	s.PGNetRxDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGContainerNetRx })
	s.PGNetTxDelta = deltaObserved(samples, func(x Sample) *int64 { return x.PGContainerNetTx })
	s.MinioBlockReadDelta = deltaObserved(samples, func(x Sample) *int64 { return x.MinioBlockRead })
	s.MinioBlockWriteDelta = deltaObserved(samples, func(x Sample) *int64 { return x.MinioBlockWrite })
	s.MinioHealthAllOK = minioHealthVerdict(samples)
	return s
}

// minioHealthVerdict reports "every probe was healthy" -- or null when no probe
// happened. A stream that never reached MinIO used to default to "all ok".
func minioHealthVerdict(samples []Sample) *bool {
	probed := false
	ok := true
	for _, x := range samples {
		if x.MinioHealthOK != nil {
			probed = true
			if !*x.MinioHealthOK {
				ok = false
			}
		}
	}
	if !probed {
		return nil
	}
	return &ok
}

// Slopes of null mean "insufficient samples" (fewer than three observations).

// slopePerMinute is an ordinary least-squares slope of y over x (x in minutes).
func slopePerMinute[T any](xs []T, x func(T) float64, y func(T) float64) float64 {
	n := float64(len(xs))
	if n < 3 {
		return math.NaN()
	}
	var sx, sy, sxy, sxx float64
	for _, v := range xs {
		xv, yv := x(v), y(v)
		sx += xv
		sy += yv
		sxy += xv * yv
		sxx += xv * xv
	}
	denom := n*sxx - sx*sx
	if denom == 0 {
		return math.NaN()
	}
	return (n*sxy - sx*sy) / denom
}

func parseUTC(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return t
	}
	t, err = time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

func f2(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return ""
	}
	return strconv.FormatFloat(math.Round(v*100)/100, 'f', 2, 64)
}

// f2ptr renders a possibly-unmeasured millisecond figure for CSV. A nil here
// is "never measured" (DEF-PROF-1) and must not print as a plausible-looking
// "0.00" -- a reader who sees 0.00 ms TTFB believes an instant first byte was
// measured, which is exactly the placeholder this fix removes.
// cacheLabel/cacheRatio/intptr render step provenance for CSV with the same
// "not_measured" marker: an absent measurement must not read as a zero.
func cacheLabel(s StepResult) string {
	if s.CacheState == nil {
		return "not_measured"
	}
	return s.CacheState.Label
}

func cacheRatio(s StepResult) string {
	if s.CacheState == nil {
		return "not_measured"
	}
	return f2ptr(s.CacheState.PGHitRatio)
}

func intptr(v *int) string {
	if v == nil {
		return "not_measured"
	}
	return strconv.Itoa(*v)
}

func f2ptr(v *float64) string {
	if v == nil {
		return "not_measured"
	}
	return f2(*v)
}

// ManifestDiscipline checks the link between this report and the acceptance
// manifest it declares. Violations must fail the run; notes are facts a reader
// cannot infer (why the manifest's subject binary differs from the measured
// one). A profile with no manifest reference is refused rather than waved
// through: acceptance reads profiles per accepted revision, and an unbound
// number can be attributed to the wrong candidate.
func (r *Report) ManifestDiscipline() (violations, notes []string) {
	m := r.Version.AcceptanceManifest
	if m == nil {
		return []string{"acceptance manifest: not referenced -- the profile does not say which attested source/binary/image of the accepted candidate it belongs to"}, nil
	}
	if r.Version.ProductRevision != "" && m.SourceRevision != "" && m.SourceRevision != r.Version.ProductRevision {
		violations = append(violations, "acceptance manifest: attests source "+m.SourceRevision+" but the profile declares product_revision "+r.Version.ProductRevision+" -- these numbers would be attributed to the wrong candidate")
	}
	if r.Version.BinarySHA256 != "" && m.SubjectBinarySHA256 != "" {
		same := m.SubjectBinarySHA256 == r.Version.BinarySHA256
		m.SubjectBinaryEqualsRun = &same
		if !same {
			notes = append(notes, "acceptance manifest: subject binary "+short(m.SubjectBinarySHA256)+" is not this run's measured binary "+short(r.Version.BinarySHA256)+" (an image build and a source build are different bytes); equality is reported, never assumed")
		}
	}
	return violations, notes
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}
