package harness

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A sample measured slowly (docker stats, PG, Redis under load) must be
// stamped and attributed by the time its measurement COMPLETED, not when it
// began. The rejected overhead run of 2026-11-09 19:53 lost every sample this
// way: each tick was stamped at entry, appended seconds later, and fell into
// the gap between step windows -- a run of minutes reported no sampler stream
// at all while ticks had in fact fired.
func TestSamplerStampsCompletedSamples(t *testing.T) {
	start := time.Now()
	s := StartSampling(SamplerConfig{Interval: 100 * time.Millisecond, DockerEvery: 1000, ObservationDelay: 250 * time.Millisecond})
	time.Sleep(1200 * time.Millisecond)
	got := s.Samples()
	s.Stop()
	if len(got) == 0 {
		t.Fatal("stream produced no samples at 100ms interval")
	}
	earliest := time.Now().Add(time.Hour)
	for _, x := range got {
		ts, err := time.Parse(time.RFC3339Nano, x.AtUTC)
		if err != nil {
			ts, err = time.Parse(time.RFC3339, x.AtUTC)
			if err != nil {
				t.Fatalf("unparseable sample time %q: %v", x.AtUTC, err)
			}
		}
		if ts.Before(earliest) {
			earliest = ts
		}
	}
	// A sample can only be honest about when it finished: with a 100ms tick
	// and a 250ms observation the earliest possible COMPLETION is start+350ms
	// (minus timestamp truncation slack of 1s for RFC3339 second precision).
	if earliest.Before(start.Add(250 * time.Millisecond)) {
		t.Fatalf("samples were stamped at measurement entry (%v), not completion: an entry-stamped sample lands outside the step window that waited for it", earliest.Sub(start))
	}
}

// The absence of a sampler stream must be visible in the serialized artifact:
// "samplers": null, never an omitted key a reader can mistake for
// "this schema has no samplers" (null-discipline applies to whole structures).
func TestStepJSONAlwaysCarriesSamplersKey(t *testing.T) {
	b, err := json.Marshal(StepResult{Workload: "w", Concurrency: 1, Requests: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "\"samplers\":null") {
		t.Fatalf("missing samplers must serialize as explicit null, got: %s", b)
	}
}

// "no sampler stream => the report is not a result" holds under MINIMAL
// requirements: it is not a threshold that tuning can satisfy.
func TestNoSamplerStreamIsUnconditionallyInvalid(t *testing.T) {
	r := &Report{Kind: "overhead", Steps: []StepResult{{Workload: "w", Concurrency: 1, Requests: 100000}}}
	req := QualityRequirements{MinSamplesPerStep: 1}
	v := strings.Join(r.QualityViolations(req), "|")
	if !strings.Contains(v, "no sampler stream") {
		t.Fatalf("a step without samplers is never a result, whatever the thresholds: %s", v)
	}
	r.Verdict = "PASS"
	r.ApplyQualityGuard(req)
	if r.Verdict != "FAIL" {
		t.Fatalf("PASS without sampler stream must not survive: %s", r.Verdict)
	}
}
