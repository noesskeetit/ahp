package harness

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Measurement protocol v1.1, rule 2 (density-baseline-ops/
// VM-CONTENTION-20261110T0019Z.md): observer probes that cost real CPU in the
// shared Docker VM -- above all `docker stats`, measured at 1-4s per call --
// run strictly OUTSIDE the measured window. Inside the window only read-only
// observation of the subject is allowed.
//
// Two things must hold, and the second is the one that gets forgotten: the
// probe must not run, AND the columns it would have filled must say why they
// are empty. A suppressed collector that just leaves nils is
// indistinguishable from a collector that broke, which is the exact defect
// DEF-PROF-3 exists to prevent.
func TestSuppressedContainerProbesDoNotRunAndNameThemselves(t *testing.T) {
	var calls atomic.Int64
	cfg := SamplerConfig{
		Interval:       60 * time.Millisecond,
		PGContainer:    "some-container",
		MinioContainer: "another-container",
		DockerEvery:    1,
		DockerRefresh:  10 * time.Millisecond,
		DockerStatsFn: func(names []string) (map[string]containerStat, error) {
			calls.Add(1)
			return map[string]containerStat{}, nil
		},
	}
	cfg.SuppressContainerProbes = true
	stream := StartSampling(cfg)
	time.Sleep(350 * time.Millisecond)
	samples := stream.Stop()
	if len(samples) < 3 {
		t.Fatalf("test harness: only %d samples observed, cannot judge probe policy", len(samples))
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("probe policy outside-only was set but the container probe ran %d times inside the window: observer cost would land inside the measurement it is supposed to stay out of", n)
	}
	suppressed := 0
	for _, s := range samples {
		for _, u := range s.Unobserved {
			if strings.HasPrefix(u, SourceDocker+":suppressed") {
				suppressed++
			}
		}
		if s.DockerSnapshotAgeMS != nil {
			t.Fatalf("suppressed stream still published a docker snapshot age (%d ms): a value from a probe that was not allowed to run", *s.DockerSnapshotAgeMS)
		}
	}
	if suppressed != len(samples) {
		t.Fatalf("%d of %d samples explain the suppressed docker collector: a reader cannot tell a refused probe from a broken one, and empty container columns will be read as zero IO", suppressed, len(samples))
	}
}

// The policy must be a policy, not a default: with the flag off the collector
// has to behave exactly as before, otherwise "outside-only" is being smuggled
// in and every existing profile silently loses its container columns.
func TestContainerProbesStillRunWhenPolicyAllowsThem(t *testing.T) {
	var calls atomic.Int64
	cfg := SamplerConfig{
		Interval:      60 * time.Millisecond,
		PGContainer:   "some-container",
		DockerEvery:   1,
		DockerRefresh: 10 * time.Millisecond,
		DockerStatsFn: func(names []string) (map[string]containerStat, error) {
			calls.Add(1)
			return map[string]containerStat{}, nil
		},
	}
	stream := StartSampling(cfg)
	time.Sleep(350 * time.Millisecond)
	samples := stream.Stop()
	if calls.Load() == 0 {
		t.Fatal("container probes did not run with the default in-window policy: the suppression flag must not become a default")
	}
	for _, s := range samples {
		for _, u := range s.Unobserved {
			if strings.HasPrefix(u, SourceDocker+":suppressed") {
				t.Fatalf("samples claim probes were suppressed while they ran: %q", u)
			}
		}
	}
}
