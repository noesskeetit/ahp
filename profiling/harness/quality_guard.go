package harness

import (
	"fmt"
	"reflect"
	"strings"
)

// QualityRequirements encodes the acceptance criteria for a FINAL profile.
// root rejected the interim profile (profiling-llm-...170442Z) precisely
// because nothing refused it: sample_count=1, a "CPU delta" of 0 computed
// from one sample, and slope=-1 placeholders made the numbers meaningless.
// A final report that violates any gate below can never read as accepted --
// the guard demotes the verdict the same way the Dot journey's consistency
// guard does: evidence first, claims second.
type QualityRequirements struct {
	// MinSamplesPerStep is the floor for both sampler samples and completed
	// requests on every counted step: p50/p95/p99 and slopes need enough
	// points to mean anything. sample_count=1 is an automatic denial.
	MinSamplesPerStep int
	// RequiredLadder are the concurrency levels a final ladder must contain.
	RequiredLadder []int
	// RequireBoundary demands evidence that a capacity/degradation boundary was
	// reached (an extension step beyond the ladder, or the recorded note).
	RequireBoundary bool
	// Machine binding: a final profile belongs to one named machine class.
	ExpectedHostname    string
	ExpectedCPUModelSub string
	MinPhysicalCores    int
	MinMemoryBytes      uint64
	// RequireCleanSource: measurements must bind to an exact, unmodified
	// revision; RequireBinaryHash / RequireImagePins: no anonymous artifacts.
	RequireCleanSource bool
	RequireImagePins   bool
	// RequirePinnedBinary refuses a final profile whose measured binary is not
	// an immutable per-run copy (acceptance has seen numbers bound to a binary
	// that a later build overwrote).
	RequirePinnedBinary bool
}

// DefaultFinalRequirements is the shipped acceptance bar for final profiles:
// ladder 1/5/10/20/30 plus boundary, >=20 samples AND requests per step,
// clean pinned source, pinned image digests.
func DefaultFinalRequirements() QualityRequirements {
	return QualityRequirements{
		MinSamplesPerStep:   20,
		RequiredLadder:      []int{1, 5, 10, 20, 30},
		RequireBoundary:     true,
		RequireCleanSource:  true,
		RequireImagePins:    true,
		RequirePinnedBinary: true,
	}
}

// QualityViolations lists every acceptance criterion this report breaks.
func (r *Report) QualityViolations(req QualityRequirements) []string {
	var v []string
	steps := append([]StepResult{}, r.Steps...)
	for _, ph := range r.Phases {
		steps = append(steps, ph.Steps...)
	}
	// (1) multi-point samples: no percentile, slope or delta may rest on a
	// single observation -- neither as a number nor as a fake zero.
	levels := map[int]bool{}
	maxConc := 0
	for _, s := range steps {
		levels[s.Concurrency] = true
		if s.Concurrency > maxConc {
			maxConc = s.Concurrency
		}
		tag := fmt.Sprintf("step %s c=%d:", s.Workload, s.Concurrency)
		if s.Samplers == nil {
			v = append(v, tag+" no sampler stream: platform behaviour during this step is unmeasured")
		} else {
			v = append(v, samplerViolations(tag, *s.Samplers, req.MinSamplesPerStep)...)
		}
		if s.Requests < req.MinSamplesPerStep {
			v = append(v, fmt.Sprintf("step %s c=%d: requests=%d < %d: percentiles are not statistically meaningful", s.Workload, s.Concurrency, s.Requests, req.MinSamplesPerStep))
		}
		// Failures must be measured, not only counted. p50/p95/p99 come from
		// successful calls; a step that had failures without their own
		// latencies reports a tail that excludes exactly the calls that were
		// slow, so the verdict would describe only the successes.
		if s.Errors > 0 && s.ErrP50 == nil {
			v = append(v, fmt.Sprintf("step %s c=%d: errors=%d but no failure latencies recorded: percentiles describe only the %d successful calls, not the failure", s.Workload, s.Concurrency, s.Errors, s.Requests-s.Errors))
		}
		if s.Errors > 0 && len(s.ErrorClasses) == 0 {
			v = append(v, fmt.Sprintf("step %s c=%d: errors=%d are unclassified: an unclassified failure cannot be attributed to a subsystem", s.Workload, s.Concurrency, s.Errors))
		}
		// Attribution gate: the measured process must be one process.
		if s.Samplers != nil && s.Samplers.ProcessIdentityChanged {
			v = append(v, fmt.Sprintf("identity: step %s c=%d changed measured process (pid=%v, pid_changes=%d, cpu_regressions=%d): cpu/rss of this step belongs to no single process", s.Workload, s.Concurrency, s.Samplers.ProcPID, s.Samplers.ProcPIDChanges, s.Samplers.ProcCPURegressions))
		}
	}
	if r.Summary != nil {
		v = append(v, samplerViolations("phase summary:", *r.Summary, req.MinSamplesPerStep)...)
	}
	// (5) provenance of the subject (DEF-PROF-3). A profile that cannot say
	// which quota it ran under, or which pid its CPU/RSS belongs to, cannot be
	// compared with any other profile -- and "0 MiB" style placeholders are how
	// those two questions used to be answered by accident.
	if len(r.Machine.ResourceLimits) == 0 {
		v = append(v, "machine: resource limits not declared -- measured, unsupported and unavailable are all valid answers, silence is not")
	}
	if (r.Summary != nil || len(steps) > 0) && r.Instance.MeasuredProcess.PID <= 0 {
		v = append(v, "machine: measured process not declared -- the cpu/rss figures do not name the pid they belong to, so platform and runner identity are indistinguishable")
	}
	// (2) the ladder and its capacity boundary.
	for _, want := range req.RequiredLadder {
		if !levels[want] {
			v = append(v, fmt.Sprintf("ladder: concurrency %d is missing", want))
		}
	}
	if req.RequireBoundary {
		bounded := false
		maxLadder := 0
		for _, l := range req.RequiredLadder {
			if l > maxLadder {
				maxLadder = l
			}
		}
		if maxConc > maxLadder {
			bounded = true
		}
		for _, n := range r.Notes {
			if strings.Contains(n, "boundary at concurrency") {
				bounded = true
			}
		}
		if !bounded {
			v = append(v, "boundary: no capacity/degradation boundary evidence beyond the required ladder")
		}
	}
	// (6) machine and pin binding: the numbers must belong to THIS machine at
	// THIS pinned source/image set.
	if req.ExpectedHostname != "" || r.Machine.Hostname == "" {
		if r.Machine.Hostname == "" {
			v = append(v, "machine: hostname unbound")
		} else if req.ExpectedHostname != "" && r.Machine.Hostname != req.ExpectedHostname {
			v = append(v, fmt.Sprintf("machine: hostname %q is not the expected %q", r.Machine.Hostname, req.ExpectedHostname))
		}
	}
	if req.ExpectedCPUModelSub != "" && !strings.Contains(r.Machine.CPUModel, req.ExpectedCPUModelSub) {
		v = append(v, fmt.Sprintf("machine: cpu %q does not contain %q", r.Machine.CPUModel, req.ExpectedCPUModelSub))
	}
	if req.MinPhysicalCores > 0 && r.Machine.PhysicalCores < req.MinPhysicalCores {
		v = append(v, fmt.Sprintf("machine: %d physical cores < %d", r.Machine.PhysicalCores, req.MinPhysicalCores))
	}
	if req.MinMemoryBytes > 0 && r.Machine.MemoryBytes < req.MinMemoryBytes {
		v = append(v, fmt.Sprintf("machine: memory %d < %d", r.Machine.MemoryBytes, req.MinMemoryBytes))
	}
	if r.Version.SourceRevision == "" || r.Version.SourceRevision == "unknown" {
		v = append(v, "version: source revision unbound")
	}
	if req.RequireCleanSource && r.Version.SourceDirty {
		v = append(v, "version: source tree dirty -- a final profile pins an exact revision")
	}
	if r.Version.BinarySHA256 == "" {
		v = append(v, "version: measured binary hash missing")
	}
	if req.RequirePinnedBinary {
		if ok, why := BinaryPathIsPerRun(r.Version.BinaryPath); !ok {
			v = append(v, "binary pin: "+why)
		}
	}
	if r.Version.ProductRevision == "" {
		v = append(v, "version: product_revision not declared -- the profile does not say which product code produced these numbers (the vcs stamp describes the working tree, not the product revision)")
	} else if r.Version.ProductDeltaOutsideProfiling != 0 {
		v = append(v, fmt.Sprintf("version: product_revision %s is not the measured tree: %d changed files outside profiling/", r.Version.ProductRevision, r.Version.ProductDeltaOutsideProfiling))
	}
	if req.RequireImagePins && len(r.Version.ImageDigests) == 0 {
		v = append(v, "version: no pinned image digests recorded")
	}
	// (4) overhead and LLM stay separate reports; a mixed artifact cannot be
	// attributed to either axis.
	if r.Kind == "overhead" && r.Model != nil {
		v = append(v, "separation: overhead report carries model usage -- overhead and LLM must be measured and reported apart")
	}
	if r.Kind == "llm-e2e" && r.Model == nil {
		v = append(v, "separation: llm report without model usage -- usage was not collected, the LLM axis is unmeasured")
	}
	return v
}

func samplerViolations(tag string, s SamplerSummary, minSamples int) []string {
	var v []string
	if s.SampleCount < minSamples {
		v = append(v, fmt.Sprintf("%s sample_count=%d < %d: statistically meaningless", tag, s.SampleCount, minSamples))
	}
	// Attribution of absence (DEF-PROF-3): if no sample carried a process
	// observation, this step has NO cpu/rss measurements at all -- the columns
	// are null, and the step cannot describe platform behaviour however dense
	// the stream was. This is the rule that would have refused the density
	// smoke whose memory accounting read "0 MiB" over 17 samples.
	procObs, debugObs := s.SampleCount, s.SampleCount
	if s.SourceObservations != nil {
		// A summary without this block predates the observation accounting;
		// absence may not be inferred from a missing field.
		procObs = s.SourceObservations[SourceProcess]
		debugObs = s.SourceObservations[SourceDebugStats]
	}
	if s.SampleCount > 0 && procObs == 0 {
		v = append(v, fmt.Sprintf("%s no process (ps) observations in %d samples: cpu/rss here were never measured and cannot be read as platform behaviour", tag, s.SampleCount))
	}
	if s.SampleCount > 0 && debugObs == 0 {
		v = append(v, fmt.Sprintf("%s no platform runtime (debug/stats) observations in %d samples: heap/goroutine figures were never measured", tag, s.SampleCount))
	}
	// (3) null discipline: a delta needs two OBSERVED endpoints; a slope needs
	// a fit over observed points. Numbers on sparser data are fabricated
	// measurements, not measurements.
	if s.SampleCount < 2 && s.ProcCPUSecDelta != nil {
		v = append(v, fmt.Sprintf("%s proc cpu delta reported on %d sample(s): must be null", tag, s.SampleCount))
	}
	if procObs >= 2 && s.ProcCPUSecDelta == nil {
		v = append(v, tag+" proc cpu delta is null although its collector had enough observations")
	}
	if procObs < 3 && s.RSSSlopeBytesPerMin != nil {
		v = append(v, fmt.Sprintf("%s rss slope reported on %d process observation(s): must be null", tag, procObs))
	}
	if debugObs < 3 && s.GoroutinesSlopePerMin != nil {
		v = append(v, fmt.Sprintf("%s goroutine slope reported on %d runtime observation(s): must be null", tag, debugObs))
	}
	if procObs >= 3 && s.RSSSlopeBytesPerMin == nil {
		v = append(v, tag+" rss slope is null although its collector had enough observations")
	}
	if s.SampleCount > 0 && s.SourceObservations[SourceMinio] == 0 && s.MinioHealthAllOK != nil {
		v = append(v, tag+" minio health reported although no probe was taken: must be null")
	}
	// Zero samples with non-null aggregates is the exact shape acceptance
	// caught in the single-shot artifact (sample_count=0 next to RSS/heap/PG/
	// Redis figures): nothing was sampled, so every metric there is invented.
	if s.SampleCount == 0 {
		if n := s.inventedAggregateCount(); n > 0 {
			v = append(v, fmt.Sprintf("%s sample_count=0 but %d aggregate metric(s) are non-null: with no samples every metric must be null", tag, n))
		}
	}
	return v
}

// ApplyQualityGuard merges the data-quality verdict into the report verdict:
// no PASS (silent or explicit) survives a violation.
func (r *Report) ApplyQualityGuard(req QualityRequirements) {
	v := r.QualityViolations(req)
	if len(v) == 0 {
		return
	}
	for _, x := range v {
		r.VerdictReasons = append(r.VerdictReasons, "data-quality: "+x)
	}
	if r.Verdict != "FAIL" {
		r.Verdict = "FAIL"
	}
}

// AcceptFinalProfile is the cross-report acceptance of a final profile set
// (ladder + soak + recovery + LLM): every member passes its own gates AND
// they all belong to the same machine at the same pinned revision/binary.
func AcceptFinalProfile(req QualityRequirements, reports ...*Report) []string {
	var v []string
	if len(reports) == 0 {
		return []string{"acceptance: no reports"}
	}
	for _, r := range reports {
		for _, x := range r.QualityViolations(req) {
			v = append(v, r.Kind+": "+x)
		}
	}
	first := reports[0]
	for _, r := range reports[1:] {
		if r.Machine.Hostname != first.Machine.Hostname {
			v = append(v, fmt.Sprintf("acceptance: %s measured on machine %q, another report on %q", r.Kind, r.Machine.Hostname, first.Machine.Hostname))
		}
		if r.Version.SourceRevision != first.Version.SourceRevision {
			v = append(v, fmt.Sprintf("acceptance: %s source revision %q differs from %q -- soak/recovery must run on the same revision", r.Kind, r.Version.SourceRevision, first.Version.SourceRevision))
		}
		if r.Version.BinarySHA256 != first.Version.BinarySHA256 {
			v = append(v, fmt.Sprintf("acceptance: %s binary hash differs from the ladder binary", r.Kind))
		}
	}
	return v
}

// inventedAggregateCount counts metric fields the summary carries. It is used
// to prove that a summary with no samples carries nothing: TickIntervalMS is
// configuration (what the tick WAS set to), not a measurement, so it is the
// one pointer field allowed to survive an empty stream.
func (s SamplerSummary) inventedAggregateCount() int {
	val := reflect.ValueOf(s)
	n := 0
	for i := 0; i < val.NumField(); i++ {
		name := val.Type().Field(i).Name
		if name == "TickIntervalMS" {
			continue
		}
		fv := val.Field(i)
		if fv.Kind() == reflect.Ptr && !fv.IsNil() {
			n++
		}
	}
	return n
}
