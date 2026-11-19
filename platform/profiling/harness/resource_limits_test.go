package harness

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// DEF-PROF-3 (root, part б): a metric this OS cannot have must say
// "unsupported", not 0 and not "unavailable" (which reads as a broken
// collector). The density smoke reported cgroup_limits_at_start="unavailable"
// on darwin -- acceptance cannot tell that apart from a bug.
func TestResourceLimitsStateVocabulary(t *testing.T) {
	limits := CollectResourceLimits()
	byName := map[string]ResourceLimit{}
	for _, l := range limits {
		byName[l.Name] = l
		switch l.State {
		case "measured", "unsupported", "unavailable":
		default:
			t.Fatalf("limit %q has state %q: only measured/unsupported/unavailable are allowed", l.Name, l.State)
		}
		if l.State != "measured" && l.Reason == "" {
			t.Fatalf("limit %q is %s without a reason -- an absent metric must explain itself", l.Name, l.State)
		}
		if l.State == "measured" && l.Value == "" {
			t.Fatalf("limit %q claims measured with no value", l.Name)
		}
		if l.State == "measured" && (l.Value == "0" || l.Value == "0\n") {
			t.Fatalf("limit %q reports a bare 0: quota figures that read as zero are almost always placeholders", l.Name)
		}
	}
	quota := []string{"cgroup_v2_memory_max", "cgroup_v2_cpu_max", "cgroup_v2_pids_max"}
	for _, name := range quota {
		lim, ok := byName[name]
		if !ok {
			t.Fatalf("quota metric %q is absent entirely: silence is read as no limit", name)
		}
		if runtime.GOOS != "linux" && lim.State != "unsupported" {
			t.Fatalf("%q on %s must be unsupported, got %q", name, runtime.GOOS, lim.State)
		}
		if runtime.GOOS != "linux" && !strings.Contains(lim.Reason, "cgroup") {
			t.Fatalf("%q reason must name the missing interface, got %q", name, lim.Reason)
		}
	}
	if lim, ok := byName["go_gomaxprocs"]; !ok || lim.State != "measured" {
		t.Fatalf("gomaxprocs is always measurable on this process: %+v", lim)
	}

	// host_memory_bytes is the one capacity figure that exists on every OS this
	// harness runs on, and density needs its REAL value: the VM size next to the
	// cgroup quota is what separates "quota-capacity" from "validation/censored".
	// The Linux branch used to leave it "unavailable" with an empty reason (the
	// gate caught it in the container, darwin could not see it), so the positive
	// contract is pinned here rather than inferred from the absence of a reason.
	lim, ok := byName["host_memory_bytes"]
	if !ok {
		t.Fatal("host_memory_bytes is absent: the machine's memory is never uncollectable on this platform")
	}
	if runtime.GOOS == "linux" {
		if lim.State != "measured" {
			t.Fatalf("host_memory_bytes on linux must be measured from procfs, got state %q reason %q", lim.State, lim.Reason)
		}
		if !strings.Contains(lim.Source, "/proc/meminfo") {
			t.Fatalf("host_memory_bytes on linux must name /proc/meminfo as its source, got %q", lim.Source)
		}
		if n, err := strconv.ParseInt(lim.Value, 10, 64); err != nil || n <= 0 {
			t.Fatalf("host_memory_bytes must be a positive byte count, got %q", lim.Value)
		}
	}
}

// TestMeasuredProcessDescribesOneSubject: the CPU/RSS numbers belong to a named
// pid, and the report must say whether other processes hold memory those
// numbers exclude (AHP_ROLE=all means one pid IS the whole product).
func TestMeasuredProcessDescribesOneSubject(t *testing.T) {
	d := DescribeProcess(osGetpid(), "all")
	if d.PID <= 0 || d.Role != "all" {
		t.Fatalf("measured process not described: %+v", d)
	}
	if d.ChildPIDsRaw != "measured" {
		t.Fatalf("child process state must be an observation, got %q", d.ChildPIDsRaw)
	}
	if d.Command == "" {
		t.Fatalf("the measured command must be named so a reader can tell platform from runner")
	}
	if strings.Contains(d.Command, "://") || strings.Contains(d.Command, "@") {
		t.Fatalf("command line leaked a locator that could carry credentials: %q", d.Command)
	}
	none := DescribeProcess(0, "all")
	if none.Note == "" {
		t.Fatalf("with no pid the artifact must say every CPU/RSS metric is unobserved")
	}
}

func osGetpid() int { return os.Getpid() }
