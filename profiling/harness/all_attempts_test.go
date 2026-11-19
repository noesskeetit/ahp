package harness

import (
	"testing"
	"time"
)

// A step whose failures are slow cannot publish a tail that pretends they are
// absent. Acceptance rule: failed latency belongs in the distribution, not only
// the successful RPCs -- but the already-signed p95_ms keeps its meaning
// (successes only), so the all-attempt reading is published beside it rather
// than smuggled into it.

func TestSlowFailuresCannotHideBehindASuccessOnlyTail(t *testing.T) {
	rec := NewLatencyRecorder()
	for i := 0; i < 20; i++ {
		rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: 2, TTFBms: 1})
	}
	for i := 0; i < 5; i++ {
		rec.Record(Call{Op: "SubmitModel", OK: false, Totalms: 30000, ErrClass: "timeout"})
	}
	s := rec.Summarize("submit", 5, 10*time.Second)
	if s.P95 == nil || *s.P95 > 10 {
		t.Fatalf("p95_ms must stay the success-only figure the accepted profiles mean: %v", s.P95)
	}
	if s.AllP95 == nil {
		t.Fatalf("all_p95_ms must exist at 25 attempts (>=20): %s", s.AllPercentileNote)
	}
	if *s.AllP95 < 1000 {
		t.Fatalf("all_p95_ms=%v ignores the five 30 s timeouts: the tail the caller experienced is not represented", *s.AllP95)
	}
	if s.AllObservations == nil || *s.AllObservations != 25 {
		t.Fatalf("all_observations must count every attempt: %v", s.AllObservations)
	}
	if s.ErrP50 == nil || *s.ErrP50 != 30000 {
		t.Fatalf("failure latencies keep their own family too: %v", s.ErrP50)
	}
}

// The same tail rule applies to the new family: two attempts are not a p95.
func TestAllAttemptTailFollowsTheSameSampleRule(t *testing.T) {
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: false, Totalms: 15000, ErrClass: "timeout"})
	s := rec.Summarize("submit", 1, time.Second)
	if s.AllP95 != nil || s.AllP99 != nil {
		t.Fatalf("all-attempt tail asserted on one attempt: %v / %v", s.AllP95, s.AllP99)
	}
	if s.AllP50 == nil || *s.AllP50 != 15000 {
		t.Fatalf("the median of one attempt is that attempt: %v", s.AllP50)
	}
	if s.AllPercentileNote == "" {
		t.Fatal("a nulled all-attempt tail must state n")
	}
	m := jsonMap(t, s)
	for _, k := range []string{"all_p95_ms", "all_p99_ms"} {
		if m[k] != nil {
			t.Fatalf("%s must serialize as null at n=1, got %v", k, m[k])
		}
	}
}

// With no failures the two readings must be the same number: the extra family
// documents the failures, it must not invent a second distribution.
func TestAllAttemptReadingMatchesSuccessReadingWithoutFailures(t *testing.T) {
	rec := NewLatencyRecorder()
	for i := 0; i < 40; i++ {
		rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: float64(i), TTFBms: float64(i)})
	}
	s := rec.Summarize("submit", 2, 5*time.Second)
	if s.Errors != 0 {
		t.Fatalf("fixture must have no failures, got %d", s.Errors)
	}
	if s.P50 == nil || s.AllP50 == nil || *s.P50 != *s.AllP50 {
		t.Fatalf("with zero failures both readings are the same distribution: p50=%v all_p50=%v", s.P50, s.AllP50)
	}
	if s.P95 == nil || s.AllP95 == nil || *s.P95 != *s.AllP95 {
		t.Fatalf("with zero failures p95 and all_p95 must agree: %v vs %v", s.P95, s.AllP95)
	}
}
