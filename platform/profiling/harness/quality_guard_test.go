package harness

import (
	"os"
	"strings"
	"testing"
)

// The interim profile profiling-llm-...170442Z was rejected by root as the
// final profile: sample_count=1, CPU delta=0 and slope=-1 made the numbers
// meaningless, yet nothing in the harness refused to present it as accepted.
// These tests pin the acceptance criteria as data gates: sparse samples, a
// missing ladder rung, a missing capacity boundary, an unbound machine/rev, a
// mixed overhead+LLM report -- any one of them must deny "accepted".

func f64(v float64) *float64 { return &v }

func denseSummary(n int) *SamplerSummary {
	s := &SamplerSummary{SampleCount: n, ProcCPUSecDelta: f64(3.2)}
	if n >= 3 {
		s.RSSSlopeBytesPerMin = f64(1)
		s.GoroutinesSlopePerMin = f64(0)
	}
	return s
}

func goodStep(conc, requests int) StepResult {
	return StepResult{Workload: "mixed", Concurrency: conc, Requests: requests,
		DurationSec: 30, P50: f64(4), P95: f64(9), P99: f64(12), Max: f64(30), Mean: f64(5),
		Samplers: denseSummary(20)}
}

func goodFinalReport() *Report {
	r := &Report{Kind: "overhead", Label: "final", RunID: "r-1",
		Machine: MachineInfo{Hostname: "sc-mac-02588", CPUModel: "Apple M4 Pro", PhysicalCores: 14, MemoryBytes: 48 << 30,
			ResourceLimits: CollectResourceLimits()},
		Instance: InstanceInfo{MeasuredProcess: DescribeProcess(os.Getpid(), PlatformRole)},
		Version: VersionInfo{SourceRevision: "abc123", SourceDirty: false, BinarySHA256: strings.Repeat("a", 64),
			ProductRevision: "prod9999", ProductDeltaOutsideProfiling: 0,
			BinaryPath: "/work/evidence/runs/20261109T000000Z-abc123/bin/ahp-profiling", BinaryPinned: true,
			ImageDigests: map[string]string{"pg": "sha256:" + strings.Repeat("b", 64)}}}
	for _, c := range []int{1, 5, 10, 20, 30} {
		r.Steps = append(r.Steps, goodStep(c, 200))
	}
	// capacity boundary evidence
	bs := goodStep(60, 400)
	bs.Workload = "mixed"
	r.Steps = append(r.Steps, bs)
	r.Notes = append(r.Notes, "mixed: boundary at concurrency 60: p99 10100.0ms > 10000ms")
	r.Summary = denseSummary(200)
	return r
}

func req() QualityRequirements {
	q := DefaultFinalRequirements()
	q.ExpectedHostname = "sc-mac-02588"
	q.ExpectedCPUModelSub = "M4 Pro"
	q.MinPhysicalCores = 14
	q.MinMemoryBytes = 40 << 30
	return q
}

func TestSparseProfileIsRejected(t *testing.T) {
	// The interim anti-example: one sample, one request, CPU delta "0".
	r := &Report{Kind: "llm-e2e", RunID: "r-0", Model: map[string]any{"usage": "collected"},
		Machine: MachineInfo{Hostname: "sc-mac-02588", CPUModel: "Apple M4 Pro", PhysicalCores: 14, MemoryBytes: 48 << 30,
			ResourceLimits: CollectResourceLimits()},
		Instance: InstanceInfo{MeasuredProcess: DescribeProcess(os.Getpid(), PlatformRole)},
		Version: VersionInfo{SourceRevision: "abc123", BinarySHA256: strings.Repeat("a", 64),
			ImageDigests: map[string]string{"pg": "sha256:" + strings.Repeat("b", 64)}}}
	s := StepResult{Workload: "llm", Concurrency: 1, Requests: 1,
		P50: f64(1), P95: f64(1), P99: f64(1), Max: f64(1), Mean: f64(1),
		Samplers: &SamplerSummary{SampleCount: 1, ProcCPUSecDelta: f64(0)}}
	r.Steps = []StepResult{s}
	r.Summary = s.Samplers
	q := req()
	v := r.QualityViolations(q)
	if len(v) == 0 {
		t.Fatal("interim-style sparse profile must produce data-quality violations")
	}
	r.Verdict = "PASS"
	r.ApplyQualityGuard(q)
	if r.Verdict != "FAIL" {
		t.Fatalf("verdict after guard = %s, want FAIL; reasons %v", r.Verdict, r.VerdictReasons)
	}
	joined := strings.Join(r.VerdictReasons, "|")
	for _, want := range []string{"sample_count", "ladder"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("reasons %q must mention %q", joined, want)
		}
	}
}

func TestCleanFinalProfileIsAccepted(t *testing.T) {
	r := goodFinalReport()
	if v := r.QualityViolations(req()); len(v) != 0 {
		t.Fatalf("clean report must have no violations, got %v", v)
	}
	r.Verdict = "PASS"
	r.ApplyQualityGuard(req())
	if r.Verdict != "PASS" {
		t.Fatalf("clean PASS must survive the guard: %s %v", r.Verdict, r.VerdictReasons)
	}
}

func TestMissingLadderRungAndBoundaryDenied(t *testing.T) {
	r := goodFinalReport()
	// drop c=20 and all boundary evidence
	var kept []StepResult
	for _, s := range r.Steps {
		if s.Concurrency == 20 || s.Concurrency == 60 {
			continue
		}
		kept = append(kept, s)
	}
	r.Steps = kept
	r.Notes = nil
	v := strings.Join(r.QualityViolations(req()), "|")
	if !strings.Contains(v, "ladder") || !strings.Contains(v, "boundary") {
		t.Fatalf("missing rung and boundary must both deny: %s", v)
	}
}

func TestMachineAndPinBindingDenied(t *testing.T) {
	cases := map[string]func(*Report){
		"wrong host":     func(r *Report) { r.Machine.Hostname = "someone-else" },
		"dirty source":   func(r *Report) { r.Version.SourceDirty = true },
		"unknown rev":    func(r *Report) { r.Version.SourceRevision = "unknown" },
		"no binary hash": func(r *Report) { r.Version.BinarySHA256 = "" },
		"no image pins":  func(r *Report) { r.Version.ImageDigests = nil },
		// A profile that does not say which PRODUCT code it measured is
		// anonymous: the vcs stamp proves the working tree, not the product.
		"no product rev":    func(r *Report) { r.Version.ProductRevision = "" },
		"product rev lying": func(r *Report) { r.Version.ProductDeltaOutsideProfiling = 3 },
	}
	for name, mut := range cases {
		r := goodFinalReport()
		mut(r)
		if len(r.QualityViolations(req())) == 0 {
			t.Fatalf("%s must deny acceptance", name)
		}
	}
}

func TestMixedOverheadAndLLMRejected(t *testing.T) {
	r := goodFinalReport() // kind overhead
	r.Model = map[string]any{"tokens": 1}
	v := strings.Join(r.QualityViolations(req()), "|")
	if !strings.Contains(v, "overhead") {
		t.Fatalf("overhead report carrying model usage must deny: %s", v)
	}
}

func TestAcceptFinalProfileRequiresSameRevAcrossReports(t *testing.T) {
	ladder := goodFinalReport()
	soak := goodFinalReport()
	soak.Kind = "soak"
	if v := AcceptFinalProfile(req(), ladder, soak); len(v) != 0 {
		t.Fatalf("consistent final set must be accepted: %v", v)
	}
	soak.Version.SourceRevision = "different"
	v := strings.Join(AcceptFinalProfile(req(), ladder, soak), "|")
	if !strings.Contains(v, "revision") {
		t.Fatalf("soak on another rev must deny: %s", v)
	}
}

func TestNullDisciplineUnderGuard(t *testing.T) {
	// A summary with one sample may not carry a numeric CPU delta (it would be
	// a fabricated zero), and slopes may only exist when they were fitted.
	r := goodFinalReport()
	r.Steps[0].Samplers = &SamplerSummary{SampleCount: 1, ProcCPUSecDelta: f64(0), RSSSlopeBytesPerMin: f64(7)}
	v := strings.Join(r.QualityViolations(req()), "|")
	if !strings.Contains(v, "delta") || !strings.Contains(v, "slope") {
		t.Fatalf("lying delta/slope on sparse data must deny: %s", v)
	}
}
