package harness

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// METRICS CONTRACT (DEF-PROF-3, root acceptance rule).
//
// Three states, and they must stay distinguishable in every artifact:
//
//	measured     -- a number. JSON: the value; CSV: the value.
//	unobserved   -- the collector ran (or could not run) and answered nothing.
//	                JSON: null; CSV: "not_measured"; the source is named in the
//	                sample's "unobserved" list with a reason.
//	unsupported  -- this OS/deployment cannot have the metric at all (cgroup
//	                quotas on darwin). JSON: state "unsupported" + reason;
//	                never 0, never a host-wide guess.
//
// A 0 is a claim that the value was measured and was zero. Two interim profiles
// were rejected on exactly that: "CPU delta=0" from one sample, and a density
// smoke whose entire memory-accounting block read "0 MiB" over 17 samples.
// This file owns the vocabulary; the samplers, the summarizer and the quality
// guard all express absence through it, so a reader never has to guess whether
// a zero is a measurement.
//
// Shared with the density harness (Gauss): the same three states and the same
// markers apply to per-agent-step metrics, and pid-per-sample is mandatory
// there too -- an RSS that moves without a pid in the row is unattributable.

// Tail-quantile thresholds: the availability rule for a QUANTILE, i.e. how
// many observations a claim needs. They belong to this contract because
// "p99=761.174" and "this metric was not measured" are the same kind of
// statement -- both say what the data may be read as -- and a rule split
// across two files is a rule one of them will violate.
//
//	p50 -- from 1 observation (the median of what was seen is what was seen)
//	p95 -- from 20 observations
//	p99 -- from 100 observations
//
// n counts the observations of that family: requests for total latency, the
// calls that produced a first byte for TTFB, failed calls for err_*. Because a
// family can never have more observations than the step has requests, the
// write-time gate in report.go (EnforceTailSampleRule) can check every family
// against requests/errors without a per-family counter.
const (
	MinSamplesP50 = 1
	MinSamplesP95 = 20
	MinSamplesP99 = 100
)

// Collector source names, as they appear in Sample.Unobserved and in
// SamplerSummary.SourceObservations.
const (
	SourceProcess    = "ps"           // cpu-time / RSS of the measured pid
	SourceDebugStats = "debug_stats"  // platform /debug/stats runtime gauges
	SourcePostgres   = "pg"           // pg_stat_database + activity + size
	SourceRedis      = "redis"        // Redis INFO
	SourceDocker     = "docker_stats" // container block/net IO
	SourceMinio      = "minio_health" // MinIO liveness probe
)

// AllSources is the fixed vocabulary the guard checks observation counts for.
var AllSources = []string{SourceProcess, SourceDebugStats, SourcePostgres, SourceRedis, SourceDocker, SourceMinio}

// notMeasured is the CSV marker for "never measured". Shared by every writer so
// one glance at a column answers "is this a number or a hole".
const notMeasured = "not_measured"

func pU64(v uint64) *uint64 { return &v }
func pI64(v int64) *int64   { return &v }
func pInt(v int) *int       { return &v }
func pBool(v bool) *bool    { return &v }

// cellX render one possibly-unobserved metric for CSV. nil is never 0.
func cellU64(v *uint64) string {
	if v == nil {
		return notMeasured
	}
	return formatUint(*v)
}

func cellI64(v *int64) string {
	if v == nil {
		return notMeasured
	}
	return formatInt(*v)
}

func cellInt(v *int) string {
	if v == nil {
		return notMeasured
	}
	return formatInt(int64(*v))
}

func cellBool(v *bool) string {
	if v == nil {
		return notMeasured
	}
	return formatBool(*v)
}

// SourceObservationCounts says how many samples each collector contributed.
// It is computed from the VALUES, not from the unobserved markers: a source
// counts as observed only where a value exists, so a summary can never
// over-claim ("the markers said configured, the columns said nothing").
func SourceObservationCounts(samples []Sample) map[string]int {
	return map[string]int{
		SourceProcess:    countObserved(samples, func(x Sample) *uint64 { return x.ProcRSS }),
		SourceDebugStats: countObserved(samples, func(x Sample) *uint64 { return x.HeapAlloc }),
		SourcePostgres:   countObserved(samples, func(x Sample) *int64 { return x.PGBlksRead }),
		SourceRedis:      countObserved(samples, func(x Sample) *int64 { return x.RedisUsedMemory }),
		SourceDocker:     countObserved(samples, func(x Sample) *int64 { return x.PGContainerBlockRead }),
		SourceMinio:      countObserved(samples, func(x Sample) *bool { return x.MinioHealthOK }),
	}
}

// firstObserved / lastObserved return the metric of the first / last sample
// where it WAS observed. Anchoring a delta on an unobserved leading sample is
// how a 100 MiB memory ramp gets invented out of a sampler that started late.
func firstObserved[T any](samples []Sample, get func(Sample) *T) *T {
	for i := range samples {
		if v := get(samples[i]); v != nil {
			return v
		}
	}
	return nil
}

func lastObserved[T any](samples []Sample, get func(Sample) *T) *T {
	for i := len(samples) - 1; i >= 0; i-- {
		if v := get(samples[i]); v != nil {
			return v
		}
	}
	return nil
}

func countObserved[T any](samples []Sample, get func(Sample) *T) int {
	n := 0
	for i := range samples {
		if get(samples[i]) != nil {
			n++
		}
	}
	return n
}

// deltaObserved is the first-to-last change over OBSERVED points, and null when
// fewer than two were observed -- one endpoint is not a difference.
func deltaObserved[T uint64 | int64 | int | float64](samples []Sample, get func(Sample) *T) *T {
	if countObserved(samples, get) < 2 {
		return nil
	}
	a, b := firstObserved(samples, get), lastObserved(samples, get)
	if a == nil || b == nil {
		return nil
	}
	v := *b - *a
	return &v
}

func maxObserved[T uint64 | int64 | int | float64](samples []Sample, get func(Sample) *T) *T {
	var best *T
	for i := range samples {
		v := get(samples[i])
		if v == nil {
			continue
		}
		if best == nil || *v > *best {
			c := *v
			best = &c
		}
	}
	return best
}

// slopeObserved fits a least-squares slope in units-per-minute over the
// observed points only, from the first OBSERVED point. Null when the observed
// subset cannot carry a fit (< 3 points) -- never a placeholder -1.
func slopeObserved[T uint64 | int64 | int | float64](samples []Sample, get func(Sample) *T) *float64 {
	type pt struct{ min, val float64 }
	var pts []pt
	var base time.Time
	started := false
	for i := range samples {
		v := get(samples[i])
		if v == nil {
			continue
		}
		at := parseUTC(samples[i].AtUTC)
		if !started {
			base, started = at, true
		}
		pts = append(pts, pt{at.Sub(base).Minutes(), float64(*v)})
	}
	if len(pts) < 3 {
		return nil
	}
	n := float64(len(pts))
	var sx, sy, sxy, sxx float64
	for _, p := range pts {
		sx += p.min
		sy += p.val
		sxy += p.min * p.val
		sxx += p.min * p.min
	}
	denom := n*sxx - sx*sx
	if denom == 0 {
		return nil
	}
	v := (n*sxy - sx*sy) / denom
	return &v
}

// ResourceLimit is one quota/capacity figure with its observation state.
// Root's rule: a metric this platform cannot have says "unsupported" -- a 0
// here would read as "unlimited" or "no memory limit", both false.
type ResourceLimit struct {
	Name   string `json:"name"`
	State  string `json:"state"` // measured | unsupported | unavailable
	Value  string `json:"value,omitempty"`
	Source string `json:"source,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// CollectResourceLimits declares the limits the MEASURED PROCESS runs under.
//
// On darwin there is no cgroup v2 at all: container quotas are a Linux
// interface, and the harness measures a host process, so the quota fields are
// unsupported with a reason rather than silently absent (the density smoke
// reported "cgroup_limits_at_start = unavailable", which acceptance cannot tell
// apart from a broken collector). On Linux the real cgroup files are read.
func CollectResourceLimits() []ResourceLimit {
	var out []ResourceLimit
	hostMem := ResourceLimit{Name: "host_memory_bytes", State: "unavailable"}
	if runtime.GOOS == "darwin" {
		if out2, err := exec.Command("sysctl", "-n", "hw.memsize").Output(); err == nil {
			hostMem.State = "measured"
			hostMem.Value = strings.TrimSpace(string(out2))
			hostMem.Source = "sysctl hw.memsize"
		} else {
			hostMem.Reason = err.Error()
		}
	} else {
		// procfs hosts (Linux, including the container the acceptance gate runs
		// in): the machine's real memory is a readable number here, so it is
		// MEASURED. Density needs it next to the cgroup quota to separate
		// "quota-capacity" from "validation/censored" -- with this field empty
		// the two are indistinguishable. A failed read still says why: an
		// unexplained "unavailable" is the placeholder DEF-PROF-3 exists to kill,
		// and it is exactly what this Linux branch used to emit.
		if v, err := procMemTotalBytes(); err == nil {
			hostMem.State = "measured"
			hostMem.Value = v
			hostMem.Source = "/proc/meminfo MemTotal"
		} else {
			hostMem.Reason = err.Error()
		}
	}
	out = append(out, hostMem)
	out = append(out, ResourceLimit{
		Name:   "go_gomaxprocs",
		State:  "measured",
		Value:  formatInt(int64(runtime.GOMAXPROCS(0))),
		Source: "runtime.GOMAXPROCS (of " + formatInt(int64(runtime.NumCPU())) + " usable CPUs)",
	})
	quota := []struct{ name, path string }{
		{"cgroup_v2_memory_max", "/sys/fs/cgroup/memory.max"},
		{"cgroup_v2_cpu_max", "/sys/fs/cgroup/cpu.max"},
		{"cgroup_v2_pids_max", "/sys/fs/cgroup/pids.max"},
	}
	if runtime.GOOS != "linux" {
		for _, q := range quota {
			out = append(out, ResourceLimit{
				Name:   q.name,
				State:  "unsupported",
				Source: q.path,
				Reason: runtime.GOOS + " has no cgroup v2: the measured process is bounded by the host, not by a container quota; a zero here would be an invented limit",
			})
		}
		return out
	}
	for _, q := range quota {
		raw, err := os.ReadFile(q.path)
		if err != nil {
			out = append(out, ResourceLimit{Name: q.name, State: "unavailable", Source: q.path, Reason: err.Error()})
			continue
		}
		v := strings.TrimSpace(string(raw))
		lim := ResourceLimit{Name: q.name, State: "measured", Source: q.path}
		if v == "max" {
			lim.State = "measured"
			lim.Value = "max"
			lim.Reason = "no quota set: unlimited on this axis"
		} else {
			lim.Value = v
		}
		out = append(out, lim)
	}
	return out
}

// procMemTotalBytes reads MemTotal from procfs and returns it in BYTES (meminfo
// reports kB). Every failure returns a non-empty error, because a limit that is
// not measured has to explain itself: "unavailable" without a reason is
// indistinguishable from a broken collector, which is what DEF-PROF-3 forbids.
func procMemTotalBytes() (string, error) {
	const path = "/proc/meminfo"
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New(path + " unreadable: " + err.Error())
	}
	for _, line := range strings.Split(string(raw), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "MemTotal:" {
			continue
		}
		kb, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return "", errors.New(path + " MemTotal is not a number: " + f[1])
		}
		if kb == 0 {
			return "", errors.New(path + " MemTotal is zero: a machine with no memory is a parsing bug, not a measurement")
		}
		return formatInt(int64(kb * 1024)), nil
	}
	return "", errors.New(path + " has no MemTotal line")
}

// ProcessDescription names the process the CPU/RSS numbers belong to, and
// whether anything else (a child process) holds memory that those numbers do
// not include. With AHP_ROLE=all the gateway IS the runner, so one pid covers
// the product; that claim is recorded instead of assumed, and children are
// listed if they exist.
type ProcessDescription struct {
	PID          int      `json:"pid"`
	Role         string   `json:"role"`
	Command      string   `json:"command"`
	ChildPIDs    []int    `json:"child_pids"`
	ChildPIDsRaw string   `json:"child_pids_state"` // measured | unavailable
	Note         string   `json:"note"`
	IdentityCmd  []string `json:"identity_command,omitempty"`
}

// DescribeProcess records what pid the sampler will pin and whether it has
// children. Never returns an error: an unknown child set is reported as
// unavailable, and the sampler still pins the pid it was given.
func DescribeProcess(pid int, role string) ProcessDescription {
	d := ProcessDescription{PID: pid, Role: role, ChildPIDs: []int{}, ChildPIDsRaw: "unavailable"}
	if pid <= 0 {
		d.Note = "no measured process: every CPU/RSS metric of this run is unobserved"
		return d
	}
	if out, err := exec.Command("ps", "-o", "command=", "-p", formatInt(int64(pid))).Output(); err == nil {
		cmd := strings.TrimSpace(string(out))
		// A command line can carry an env-injected DSN on some launch styles;
		// keep the executable name only. Secrets never enter an artifact.
		if i := indexByte(cmd, ' '); i > 0 {
			cmd = cmd[:i]
		}
		d.Command = cmd
	}
	out, err := exec.Command("pgrep", "-P", formatInt(int64(pid))).Output()
	if err != nil {
		// pgrep exits 1 when there are no children, which IS an observation.
		if ee, ok := err.(*exec.ExitError); ok && ee.ExitCode() == 1 {
			d.ChildPIDsRaw = "measured"
			d.Note = "no child processes: the measured pid is the whole subject"
			return d
		}
		d.Note = "child processes could not be listed; RSS may exclude a child"
		return d
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if v, err2 := strconv.Atoi(line); err2 == nil {
			d.ChildPIDs = append(d.ChildPIDs, v)
		}
	}
	d.ChildPIDsRaw = "measured"
	if len(d.ChildPIDs) > 0 {
		d.Note = "measured pid has children: platform RSS below excludes them; sum child RSS before comparing with a container limit"
	}
	return d
}

func formatUint(v uint64) string { return strconv.FormatUint(v, 10) }
func formatInt(v int64) string   { return strconv.FormatInt(v, 10) }
func formatBool(v bool) string   { return strconv.FormatBool(v) }
