package harness

import (
	"testing"
	"time"
)

// tail_justified tells a reader, without recomputing n, whether the quoted
// p95/p99 are tails or maxima. It is derived from the SAME thresholds that
// produce the nulls, so the flag and the nulls can never disagree.
func TestTailJustifiedTracksTheSampleSize(t *testing.T) {
	small := NewLatencyRecorder()
	for i := 0; i < 5; i++ {
		small.Record(Call{Op: "kv_write", OK: true, Totalms: float64(10 + i)})
	}
	s := small.Summarize("kv_write", 1, time.Second)
	if s.TailJustified {
		t.Fatal("tail_justified=true on 5 observations: these are maxima, not tails")
	}
	if s.P95 != nil || s.P99 != nil {
		t.Fatal("tails were asserted below the threshold while tail_justified is supposed to guard them")
	}

	big := NewLatencyRecorder()
	for i := 0; i < 100; i++ {
		big.Record(Call{Op: "kv_write", OK: true, Totalms: float64(10 + i%37)})
	}
	b := big.Summarize("kv_write", 1, time.Second)
	if b.P95 == nil || b.P99 == nil {
		t.Fatalf("100 observations must justify both tails: p95=%v p99=%v", b.P95, b.P99)
	}
	if !b.TailJustified {
		t.Fatal("tail_justified=false on 100 observations: the flag contradicts the asserted tails")
	}
	m := jsonMap(t, b)
	if m["tail_justified"] != true {
		t.Fatalf("tail_justified must serialize for the artifact reader, got %v", m["tail_justified"])
	}
}
