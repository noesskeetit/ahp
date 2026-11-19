package harness

import (
	"testing"
	"time"
)

// One request cannot produce a tail percentile. Acceptance caught this in the
// single-shot artifact: steps[submit_c1] carried ttfb_p99_ms=761.174 with
// requests=1, while p99_ms and model_usage.phases.ttfb_p99_ms were null. The
// same number in three places, asserted in one and unasserted in the others,
// means the rule lives in whoever wrote the line -- so the rule now lives here
// and every latency family goes through it.

func TestOneRequestCannotProduceATailPercentile(t *testing.T) {
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: 761.174, TTFBms: 761.174})
	s := rec.Summarize("submit_c1", 1, time.Second)
	if s.Requests != 1 {
		t.Fatalf("fixture must be exactly one request, got %d", s.Requests)
	}
	if s.TTFBP99 != nil || s.TTFBP95 != nil {
		t.Fatalf("ttfb tail asserted on 1 observation: p95=%v p99=%v -- one number is the maximum, not a tail", s.TTFBP95, s.TTFBP99)
	}
	if s.P99 != nil || s.P95 != nil {
		t.Fatalf("latency tail asserted on 1 observation: p95=%v p99=%v", s.P95, s.P99)
	}
	if s.P50 == nil || *s.P50 != 761.174 {
		t.Fatalf("p50 of one observation is that observation: %v", s.P50)
	}
	m := jsonMap(t, s)
	for _, k := range []string{"p95_ms", "p99_ms", "ttfb_p95_ms", "ttfb_p99_ms"} {
		if m[k] != nil {
			t.Fatalf("%s = %v on n=1; must serialize as null", k, m[k])
		}
	}
	if s.PercentileNote == "" {
		t.Fatalf("a nulled tail must say why (n), so a reader does not read null as a broken probe")
	}
}

func TestFailedCallTailFollowsTheSameRule(t *testing.T) {
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: false, Totalms: 15000, ErrClass: "timeout"})
	s := rec.Summarize("submit_c1", 1, time.Second)
	if s.ErrP99 != nil || s.ErrP95 != nil {
		t.Fatalf("failure tail asserted on 1 failed call: p95=%v p99=%v", s.ErrP95, s.ErrP99)
	}
	if s.ErrP50 == nil || *s.ErrP50 != 15000 {
		t.Fatalf("the failed call's p50 is its own latency: %v", s.ErrP50)
	}
	if s.ErrPercentileNote == "" {
		t.Fatalf("nulled failure tail must state n")
	}
}

// TestTailRuleIsUniform: the thresholds are one declared rule, not one per
// call site -- p50 from 1 observation, p95 from 20, p99 from 100.
func TestTailRuleIsUniform(t *testing.T) {
	// Values start at 1 ms: a 0 ttfb is "no first byte observed" and is
	// deliberately not recorded, so a fixture must not use it.
	series := func(n int) []float64 {
		v := make([]float64, 0, n)
		for i := 1; i <= n; i++ {
			v = append(v, float64(i))
		}
		return v
	}
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: 5, TTFBms: 5})
	one := rec.Summarize("x", 1, time.Second)
	rec2 := NewLatencyRecorder()
	for _, v := range series(19) {
		rec2.Record(Call{Op: "SubmitModel", OK: true, Totalms: v, TTFBms: v})
	}
	nineteen := rec2.Summarize("x", 1, time.Second)
	if nineteen.P95 != nil || nineteen.TTFBP95 != nil {
		t.Fatalf("p95 on 19 observations must be null: %v / %v", nineteen.P95, nineteen.TTFBP95)
	}
	rec3 := NewLatencyRecorder()
	for _, v := range series(20) {
		rec3.Record(Call{Op: "SubmitModel", OK: true, Totalms: v, TTFBms: v})
	}
	twenty := rec3.Summarize("x", 1, time.Second)
	if twenty.P95 == nil || twenty.TTFBP95 == nil {
		t.Fatalf("p95 asserted threshold reached but still null: %+v / %+v", twenty.P95, twenty.TTFBP95)
	}
	if twenty.P99 != nil {
		t.Fatalf("p99 on 20 observations must stay null: %v", twenty.P99)
	}
	rec4 := NewLatencyRecorder()
	for _, v := range series(100) {
		rec4.Record(Call{Op: "SubmitModel", OK: true, Totalms: v, TTFBms: v})
	}
	hundred := rec4.Summarize("x", 1, time.Second)
	if hundred.P99 == nil || hundred.TTFBP99 == nil {
		t.Fatalf("p99 must be asserted once 100 observations exist: %v / %v", hundred.P99, hundred.TTFBP99)
	}
	if one.P50 == nil {
		t.Fatalf("p50 stays available from the first observation")
	}
}
