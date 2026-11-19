package harness

import (
	"testing"
	"time"
)

// DEF-PROF-1: in the LLM phase the counter `errors` covered only the transport
// layer, while the terminal outcome of a generation lived separately in
// generation_status_counts. The real run 20261109T161709Z-y9f8mc reported
// requests=12, errors=0 while 1 generation ended FAILED and 3 stream requests
// never resolved to a terminal durable status: the report could claim "zero
// logical failures" and a passing phase. Transport and terminal accounting must
// be counted apart and named apart, "streamed" is not a terminal status, and a
// phase with terminal failures or logical errors cannot PASS.

func statuses(counts map[string]int) []string {
	var out []string
	for status, n := range counts {
		for i := 0; i < n; i++ {
			out = append(out, status)
		}
	}
	return out
}

// TestAccountingAntiExampleY9F8MC pins the anti-example from the real evidence:
// 12 requests, 0 transport errors, durable statuses COMPLETED x8, FAILED x1 and
// 3 requests that the old harness logged as "streamed" without ever reading the
// durable terminal row.
func TestAccountingAntiExampleY9F8MC(t *testing.T) {
	got := AccountPhase(12, 0, statuses(map[string]int{
		"COMPLETED": 8,
		"FAILED":    1,
		"streamed":  3,
	}))
	if got.TransportErrors != 0 {
		t.Fatalf("transport_errors = %d, want 0 (every request reached the gateway)", got.TransportErrors)
	}
	if got.TerminalCompleted != 8 || got.TerminalFailed != 1 {
		t.Fatalf("terminal_completed/failed = %d/%d, want 8/1", got.TerminalCompleted, got.TerminalFailed)
	}
	if got.TerminalUnknown != 3 {
		t.Fatalf("terminal_unknown = %d, want 3: \"streamed\" is not terminal, those requests have no durable terminal status", got.TerminalUnknown)
	}
	if got.LogicalErrors != 4 {
		t.Fatalf("logical_errors = %d, want 4 (requests - terminal_completed = 12-8): the report must never show zero logical failures here", got.LogicalErrors)
	}
	if got.LogicalBreakdown["generation_failed"] != 1 {
		t.Fatalf("logical_breakdown lost the failed generation reason: %+v", got.LogicalBreakdown)
	}
	if got.LogicalBreakdown["no_terminal_status"] != 3 {
		t.Fatalf("logical_breakdown lost the unresolved stream requests: %+v", got.LogicalBreakdown)
	}
	sum := 0
	for _, n := range got.LogicalBreakdown {
		sum += n
	}
	if sum != got.LogicalErrors {
		t.Fatalf("logical_breakdown sums to %d, want logical_errors=%d", sum, got.LogicalErrors)
	}
	if got.Verdict != "FAIL" {
		t.Fatalf("verdict = %q, want FAIL: terminal_failed=1 and logical_errors=4 cannot pass", got.Verdict)
	}
}

func TestAccountingAllCompletedPasses(t *testing.T) {
	got := AccountPhase(12, 0, statuses(map[string]int{"COMPLETED": 12}))
	if got.LogicalErrors != 0 || got.Verdict != "PASS" {
		t.Fatalf("clean phase must PASS, got %+v", got)
	}
}

// TestAccountingStreamedIsNeverTerminal: the durable row after the stream is
// the only accepted terminal evidence; a raw "streamed" marker lands in unknown
// even when everything else completed.
func TestAccountingStreamedIsNeverTerminal(t *testing.T) {
	streamedOnly := AccountPhase(3, 0, statuses(map[string]int{"streamed": 3}))
	if streamedOnly.TerminalCompleted != 0 || streamedOnly.TerminalUnknown != 3 || streamedOnly.Verdict != "FAIL" {
		t.Fatalf("streamed counted as terminal: %+v", streamedOnly)
	}
	resolvedStream := AccountPhase(3, 0, statuses(map[string]int{"COMPLETED": 3}))
	if resolvedStream.Verdict != "PASS" {
		t.Fatalf("stream phase whose durable rows completed must PASS: %+v", resolvedStream)
	}
}

func TestAccountingTransportOnlyFailureFails(t *testing.T) {
	got := AccountPhase(2, 2, nil)
	if got.LogicalErrors != 2 || got.TerminalUnknown != 2 || got.Verdict != "FAIL" {
		t.Fatalf("transport-only failure must not pass: %+v", got)
	}
}

func TestAggregateAnyFailedPhaseFailsRun(t *testing.T) {
	agg := AggregateAccounting([]PhaseAccounting{
		AccountPhase(9, 0, statuses(map[string]int{"COMPLETED": 9})),
		AccountPhase(3, 0, statuses(map[string]int{"COMPLETED": 2, "FAILED": 1})),
	})
	if agg.Requests != 12 || agg.TerminalCompleted != 11 || agg.TerminalFailed != 1 || agg.LogicalErrors != 1 {
		t.Fatalf("aggregate wrong: %+v", agg)
	}
	if agg.Verdict != "FAIL" {
		t.Fatalf("run verdict = %q, want FAIL while any phase failed", agg.Verdict)
	}
}

// TestTTFBNotMeasuredIsNull: a non-streaming step has never measured TTFB, so
// the report must carry null, not a "0 ms" placeholder.
func TestTTFBNotMeasuredIsNull(t *testing.T) {
	rec := NewLatencyRecorder()
	rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: 100})
	rec.Record(Call{Op: "SubmitModel", OK: true, Totalms: 200})
	s := rec.Summarize("submit_c1", 1, time.Second)
	if s.TTFBP50 != nil || s.TTFBP95 != nil || s.TTFBP99 != nil {
		t.Fatalf("unmeasured TTFB must be null, got %v/%v/%v", *s.TTFBP50, *s.TTFBP95, *s.TTFBP99)
	}
	if csv := f2ptr(s.TTFBP50); csv != "not_measured" {
		t.Fatalf("CSV for unmeasured TTFB = %q, want \"not_measured\"", csv)
	}
	s2 := NewLatencyRecorder()
	s2.Record(Call{Op: "StreamModel", OK: true, Totalms: 300, TTFBms: 42})
	sum := s2.Summarize("stream_c1", 1, time.Second)
	if sum.TTFBP50 == nil || *sum.TTFBP50 != 42 {
		t.Fatalf("measured TTFB must survive: %+v", sum)
	}
}

// TestStreamTerminalStatus: the durable row arrives in the terminal frame;
// without it the stream resolved nothing.
func TestStreamTerminalStatus(t *testing.T) {
	completed := []byte(`{"type":"accepted","generation_id":"g1"}` + "\n" +
		`{"type":"delta","delta":{"generation_id":"g1","sequence":1,"text":"hi"}}` + "\n" +
		`{"type":"terminal","generation":{"id":"g1","status":"COMPLETED"}}` + "\n")
	if got := StreamTerminalStatus(completed); got != "COMPLETED" {
		t.Fatalf("terminal frame status = %q, want COMPLETED", got)
	}
	failed := []byte(`{"type":"accepted","generation_id":"g2"}` + "\n" +
		`{"type":"terminal","generation":{"id":"g2","status":"FAILED"}}` + "\n")
	if got := StreamTerminalStatus(failed); got != "FAILED" {
		t.Fatalf("terminal FAILED row = %q", got)
	}
	errored := []byte(`{"type":"accepted","generation_id":"g3"}` + "\n" +
		`{"type":"error","code":"provider_timeout"}` + "\n")
	if got := StreamTerminalStatus(errored); got != "FAILED" {
		t.Fatalf("error frame must resolve FAILED, got %q", got)
	}
	closedEarly := []byte(`{"type":"accepted","generation_id":"g4"}` + "\n" +
		`{"type":"delta","delta":{"generation_id":"g4","sequence":1,"text":"partial"}}` + "\n")
	if got := StreamTerminalStatus(closedEarly); got != "" {
		t.Fatalf("stream without terminal frame resolved nothing, got %q", got)
	}
	// The end-to-end anti-case: a stream phase where bytes streamed but the
	// durable row said FAILED must land in terminal_failed, not "0 errors".
	if got := AccountPhase(1, 0, []string{StreamTerminalStatus(failed)}); got.Verdict != "FAIL" {
		t.Fatalf("streamed request whose durable row FAILED passed: %+v", got)
	}
}
