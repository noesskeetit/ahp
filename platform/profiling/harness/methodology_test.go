package harness

import (
	"strings"
	"testing"
	"time"
)

// Failed calls used to be cut from the latency record (Record returned before
// appending), so a step full of 5s timeouts reported the tail of its healthy
// calls and looked fine. Error latency must be its own visible distribution.
func TestFailedCallLatencyIsRecordedSeparately(t *testing.T) {
	r := NewLatencyRecorder()
	for i := 0; i < 100; i++ {
		r.Record(Call{Op: "op", OK: true, Totalms: 10})
	}
	for i := 0; i < 20; i++ {
		r.Record(Call{Op: "op", OK: false, Totalms: 5000, ErrClass: "timeout"})
	}
	s := r.Summarize("op", 10, time.Second)
	if s.ErrP95 == nil || *s.ErrP95 < 4000 {
		t.Fatalf("failed-call latency must be recorded, got %v", s.ErrP95)
	}
	if s.P95 == nil || *s.P95 > 20 {
		t.Fatalf("success percentiles must stay over successes only, got %v", s.P95)
	}
	if s.ErrorClasses["timeout"] != 20 {
		t.Fatalf("error classes lost: %v", s.ErrorClasses)
	}
}

// n=20 LLM rungs quoted "p99" that was really the maximum. At small n a p99 is
// not a percentile and must be published as null with a stated reason.
func TestSmallSampleP99IsNotAsserted(t *testing.T) {
	r := NewLatencyRecorder()
	for i := 0; i < 20; i++ {
		r.Record(Call{Op: "llm", OK: true, Totalms: float64(100 + i)})
	}
	s := r.Summarize("llm", 1, 10*time.Second)
	if s.P99 != nil {
		t.Fatalf("p99 asserted on n=20 (value %v): it is the maximum, not a tail estimate", *s.P99)
	}
	if !strings.Contains(s.PercentileNote, "n=20") {
		t.Fatalf("the report must say why p99 is null: %q", s.PercentileNote)
	}
	if s.P95 == nil {
		t.Fatal("p95 on n=20 is still publishable")
	}
}

// A closed loop cannot show saturation: the generator slows down with the
// system. The open loop must show offered >> completed and shedded arrivals.
func TestOpenLoopRevealsSaturation(t *testing.T) {
	res := RunOpenLoop(OpenLoopOptions{RatePerSec: 400, Workers: 2, Duration: 400 * time.Millisecond}, func(worker int) Call {
		time.Sleep(40 * time.Millisecond) // 2 workers x 25/s = 50/s capacity vs 400/s offered
		return Call{Op: "op", OK: true, Totalms: 40}
	})
	if res.Offered < 60 {
		t.Fatalf("offered load must follow the schedule: offered=%d in %v", res.Offered, res.Wall)
	}
	if res.Dropped == 0 {
		t.Fatalf("offered 400/s against ~50/s capacity must shed arrivals; dropped=0 means saturation is still invisible (offered=%d completed=%d maxq=%d)", res.Offered, res.Completed, res.MaxQueue)
	}
	if res.MaxQueue < 1 {
		t.Fatalf("queue depth must be observed: %d", res.MaxQueue)
	}
	s := res.Rec.Summarize("op", 2, res.Wall)
	if s.Offered == nil || s.Dropped == nil {
		t.Fatal("step must carry the offered/completed/shedded triple")
	}
	if s.ErrorClasses["shedded_at_capacity"] == 0 {
		t.Fatalf("shedded arrivals must count as refusals: %v", s.ErrorClasses)
	}
}

func TestCacheProbeLabelsColdAndWarm(t *testing.T) {
	mk := func(hits, reads int64) []Sample {
		return []Sample{
			{PGBlksHit: i64(0), PGBlksRead: i64(0), RedisTotalCommands: i64(0)},
			{PGBlksHit: i64(hits), PGBlksRead: i64(reads), RedisTotalCommands: i64(10), ProcRSS: u64(1 << 30)},
		}
	}
	if got := ProbeCache(mk(9990, 10)); got.Label != "warm" {
		t.Fatalf("high hit ratio must read warm, got %s (%s)", got.Label, got.Note)
	}
	if got := ProbeCache(mk(500, 500)); got.Label != "cold" {
		t.Fatalf("50%% hit ratio must read cold, got %s", got.Label)
	}
	if got := ProbeCache([]Sample{{}}); got.Label != "unknown" {
		t.Fatalf("one sample cannot state cache state, got %s", got.Label)
	}
}
