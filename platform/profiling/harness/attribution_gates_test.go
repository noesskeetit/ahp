package harness

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Three defects the 2026-11-09 overhead ladder exposed, pinned as gates:
//
//  1. process identity -- samples.csv carried no pid, so an RSS that jumped
//     152 MiB -> 34 MiB inside one step could not be told apart from the
//     sampler having sampled a different process. An artifact that cannot
//     name the process it measured cannot attribute CPU or memory to it.
//  2. run identity -- the trio carried the harness-generated id while the run
//     log carried the wrapper's id, so a published number could not be tied
//     to the log that produced it (this directly caused an unreconcilable
//     publication on 2026-11-09).
//  3. failure accounting -- percentiles are computed over successful calls
//     only, so a step with errors contributes its failures to the error rate
//     and to nothing else; the guard must refuse a verdict built that way.

func writeReportForColumns(t *testing.T) (stepsHeader, samplesHeader []string) {
	t.Helper()
	dir := t.TempDir()
	st := goodStep(1, 500)
	st.Errors = 3
	st.ErrP50 = nil
	rep := goodFinalReport()
	rep.Steps = []StepResult{st}
	rep.Samples = []Sample{{AtUTC: "2026-11-09T20:54:29.000000Z", ProcCPUSec: f64(1), ProcRSS: u64(2)}}
	if _, err := rep.WriteOutputs(dir, "probe"); err != nil {
		t.Fatalf("write outputs: %v", err)
	}
	readHeader := func(name string) []string {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		defer f.Close()
		recs, err := csv.NewReader(f).ReadAll()
		if err != nil || len(recs) == 0 {
			t.Fatalf("read %s: %v", name, err)
		}
		return recs[0]
	}
	return readHeader("probe-steps.csv"), readHeader("probe-samples.csv")
}

func has(list []string, want string) bool {
	for _, x := range list {
		if x == want {
			return true
		}
	}
	return false
}

// The ladder artifact must name the process behind every sample and every
// step, and must say which load pattern produced a step.
func TestArtifactsCarryProcessIdentityAndPattern(t *testing.T) {
	stepsHeader, samplesHeader := writeReportForColumns(t)
	for _, col := range []string{"load_pattern", "proc_pid", "process_identity_changed"} {
		if !has(stepsHeader, col) {
			t.Errorf("steps.csv has no %q column: step is not attributable to one process or one load pattern (columns: %s)", col, strings.Join(stepsHeader, ","))
		}
	}
	if !has(samplesHeader, "proc_pid") {
		t.Errorf("samples.csv has no %q column: a sample cannot be bound to the process it observed (columns: %s)", "proc_pid", strings.Join(samplesHeader, ","))
	}
}

// The report must record WHERE its run id came from, so the trio, the run log
// and the sidecar can be shown to be the same run.
func TestReportRecordsRunIDSource(t *testing.T) {
	rep := goodFinalReport()
	rep.RunID = "20261109T205426Z-mx46gs"
	data, err := json.Marshal(rep)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "run_id_source") {
		t.Errorf("report JSON does not say where run_id came from; wrapper id and artifact id cannot be reconciled: %s", data[:200])
	}
}

// A step that had failures must publish their latencies, otherwise the
// verdict is computed from the successes and silently understates the tail.
func TestQualityGuardRefusesSuccessOnlyVerdict(t *testing.T) {
	rep := goodFinalReport()
	st := goodStep(1, 500)
	st.Errors = 40
	st.ErrorClasses = map[string]int{"http_500": 40}
	st.ErrP50, st.ErrP95, st.ErrP99 = nil, nil, nil
	rep.Steps = []StepResult{st}
	for _, v := range rep.QualityViolations(DefaultFinalRequirements()) {
		if strings.Contains(v, "failure") && strings.Contains(v, "latenc") {
			return
		}
	}
	t.Errorf("guard accepted a step with errors=40 whose failure latencies were never recorded: the verdict describes only the 460 successful calls")
}

// ps must answer for the pid we asked about. Before this, `ps -o time=,rss=`
// returned a number for whatever the pid meant at that instant and the
// harness believed it.
func TestParsePsProcRejectsForeignPID(t *testing.T) {
	st, err := parsePsProc([]byte("  4321  01:23.45  98765\n"), 4321)
	if err != nil {
		t.Fatalf("matching pid must parse: %v", err)
	}
	if st.PID != 4321 || st.RSSBytes != 98765*1024 || st.CPUSec < 83 || st.CPUSec > 84 {
		t.Fatalf("wrong parse: %+v", st)
	}
	if _, err := parsePsProc([]byte("  9999  00:01.00  100\n"), 4321); err == nil {
		t.Fatal("ps answered for another pid and was accepted: identity is not established")
	}
	if _, err := parsePsProc([]byte(""), 4321); err == nil {
		t.Fatal("empty ps output must be an error, not a zero measurement")
	}
}

// One step must describe one process: a second pid, or cumulative cpu running
// backwards, is an identity change and must be reported, not averaged away.
func TestSummarizeFlagsProcessIdentityChange(t *testing.T) {
	mk := func(pid int, cpu, rss uint64, at string) Sample {
		return Sample{AtUTC: at, ProcPID: ip(pid), ProcCPUSec: f64(float64(cpu)), ProcRSS: u64(rss)}
	}
	same := []Sample{mk(111, 10, 150<<20, "2026-11-09T20:54:29.000000Z"), mk(111, 20, 150<<20, "2026-11-09T20:54:31.000000Z"), mk(111, 30, 149<<20, "2026-11-09T20:54:33.000000Z")}
	if s := SummarizeSamples(same); s.ProcessIdentityChanged || s.ProcPID == nil || *s.ProcPID != 111 {
		t.Fatalf("one process reported as changed: %+v", s)
	}
	// The 2026-11-09 shape: RSS falls from 152 MiB to 34 MiB mid-step.
	swapped := []Sample{mk(111, 154, 152<<20, "2026-11-09T21:09:01.000000Z"), mk(222, 155, 34<<20, "2026-11-09T21:09:03.000000Z"), mk(222, 157, 34<<20, "2026-11-09T21:09:05.000000Z")}
	s := SummarizeSamples(swapped)
	// ProcPIDChanges counts OBSERVATIONS that disagree with the pin (two here),
	// which is the conservative reading: every suspect tick is charged.
	if !s.ProcessIdentityChanged || s.ProcPIDChanges != 2 || s.ProcPID == nil || *s.ProcPID != 111 {
		t.Fatalf("a mid-step pid switch must be flagged: %+v", s)
	}
	// Same pid but cumulative cpu went down: impossible for one process.
	regressed := []Sample{mk(111, 30, 40<<20, "2026-11-09T21:09:01.000000Z"), mk(111, 12, 40<<20, "2026-11-09T21:09:03.000000Z"), mk(111, 13, 40<<20, "2026-11-09T21:09:05.000000Z")}
	if s2 := SummarizeSamples(regressed); !s2.ProcessIdentityChanged || s2.ProcCPURegressions == 0 {
		t.Fatalf("cpu going backwards is an identity switch and must be flagged: %+v", s2)
	}
}

// Identity is a gate, not a footnote: a step measured across two processes
// cannot carry a CPU/RSS claim.
func TestQualityGuardRefusesStepWithChangedProcessIdentity(t *testing.T) {
	rep := goodFinalReport()
	st := goodStep(1, 500)
	s := denseSummary(20)
	s.ProcPID, s.ProcPIDChanges, s.ProcessIdentityChanged = ip(111), 1, true
	st.Samplers = s
	rep.Steps = []StepResult{st}
	for _, v := range rep.QualityViolations(DefaultFinalRequirements()) {
		if strings.Contains(v, "identity") {
			return
		}
	}
	t.Error("guard accepted a step whose measured process changed mid-step")
}

// One run, one id: the wrapper's id wins so the run log and the artifacts
// agree; a self-generated id must be admitted in the artifact.
func TestResolveRunIDPrefersWrapperID(t *testing.T) {
	if id, src := ResolveRunID("20261109T205424Z-6caf42", "20261109T205426Z-mx46gs"); id != "20261109T205424Z-6caf42" || src != RunIDFromWrapper {
		t.Fatalf("wrapper id must win: %s/%s", id, src)
	}
	if id, src := ResolveRunID("  ", "gen-1"); id != "gen-1" || src != RunIDGenerated {
		t.Fatalf("fallback must be admitted as self-generated: %s/%s", id, src)
	}
}

// The flat CSV must carry the identity and pattern VALUES, not only columns.
func TestStepsAndSamplesRowsCarryIdentityValues(t *testing.T) {
	dir := t.TempDir()
	st := goodStep(1, 500)
	st.Samplers.ProcPID = ip(4321)
	st.LoadPattern = "closed-loop"
	rep := goodFinalReport()
	rep.Steps = []StepResult{st}
	rep.Samples = []Sample{{AtUTC: "2026-11-09T20:54:29.000000Z", ProcPID: ip(4321), ProcCPUSec: f64(1), ProcRSS: u64(2)}}
	if _, err := rep.WriteOutputs(dir, "probe"); err != nil {
		t.Fatal(err)
	}
	read := func(name string) [][]string {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		recs, err := csv.NewReader(f).ReadAll()
		if err != nil {
			t.Fatal(err)
		}
		return recs
	}
	col := func(recs [][]string, name string) int {
		for i, h := range recs[0] {
			if h == name {
				return i
			}
		}
		t.Fatalf("no column %s", name)
		return -1
	}
	steps := read("probe-steps.csv")
	if got := steps[1][col(steps, "proc_pid")]; got != "4321" {
		t.Errorf("steps.csv proc_pid=%q, want 4321", got)
	}
	if got := steps[1][col(steps, "load_pattern")]; got != "closed-loop" {
		t.Errorf("steps.csv load_pattern=%q", got)
	}
	samples := read("probe-samples.csv")
	if got := samples[1][col(samples, "proc_pid")]; got != "4321" {
		t.Errorf("samples.csv proc_pid=%q, want 4321", got)
	}
}
