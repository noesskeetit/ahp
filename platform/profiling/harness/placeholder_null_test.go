package harness

import (
	"testing"
	"time"
)

// DEF-PROF-1 follow-up: -1 sentinels for unmeasured figures read as data on a
// quick glance ("-1 ms p95" looks like a broken measurement, "-1 bytes/min
// slope" looks like memory being reclaimed). Absence must serialize as null and
// print as an explicit marker.

func TestLatencyPercentilesAreNullWhenUnmeasured(t *testing.T) {
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: false, ErrClass: "http_500"})
	s := rec.Summarize("submit_c1", 1, time.Second)
	if s.P50 != nil || s.P95 != nil || s.P99 != nil || s.Mean != nil || s.Max != nil {
		t.Fatalf("a step with zero successful calls must report null latency, not -1: %+v", s)
	}
	if s.ErrorRatePct != 100 {
		t.Fatalf("error_rate_pct = %v, want 100", s.ErrorRatePct)
	}
	if csv := f2ptr(s.P95); csv != "not_measured" {
		t.Fatalf("CSV for unmeasured p95 = %q, want not_measured", csv)
	}
	ok := NewLatencyRecorder()
	ok.Record(Call{Op: "SubmitModel", OK: true, Totalms: 100})
	if m := ok.Summarize("submit_c1", 1, time.Second); m.P50 == nil || *m.P50 != 100 || m.Max == nil || *m.Max != 100 {
		t.Fatalf("measured latency must survive as a number: %+v", m)
	}
}

func TestSlopesAreNullWithInsufficientSamples(t *testing.T) {
	thin := SummarizeSamples([]Sample{
		{AtUTC: "2026-11-09T10:00:00Z", ProcRSS: u64(100), Goroutines: ip(5)},
		{AtUTC: "2026-11-09T10:00:05Z", ProcRSS: u64(110), Goroutines: ip(6)},
	})
	if thin.RSSSlopeBytesPerMin != nil || thin.GoroutinesSlopePerMin != nil {
		t.Fatalf("two-sample slope is not a slope: rss=%v goroutines=%v, want null",
			*thin.RSSSlopeBytesPerMin, *thin.GoroutinesSlopePerMin)
	}
	three := SummarizeSamples([]Sample{
		{AtUTC: "2026-11-09T10:00:00Z", ProcRSS: u64(100), Goroutines: ip(5)},
		{AtUTC: "2026-11-09T10:01:00Z", ProcRSS: u64(200), Goroutines: ip(6)},
		{AtUTC: "2026-11-09T10:02:00Z", ProcRSS: u64(300), Goroutines: ip(7)},
	})
	if three.RSSSlopeBytesPerMin == nil || *three.RSSSlopeBytesPerMin != 100 {
		t.Fatalf("real slope lost: %+v", three.RSSSlopeBytesPerMin)
	}
	if three.GoroutinesSlopePerMin == nil || *three.GoroutinesSlopePerMin != 1 {
		t.Fatalf("goroutine slope lost: %+v", three.GoroutinesSlopePerMin)
	}
}
