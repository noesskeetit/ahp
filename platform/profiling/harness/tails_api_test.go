package harness

import (
	"fmt"
	"testing"
)

// TestSingleCallNeverQuotesATail guards the hand-built path (smoke phase): a
// caller result becomes a step with one request, and that step must not quote
// p95/p99 either.
func TestSingleCallNeverQuotesATail(t *testing.T) {
	tails := Tails([]float64{761.174}, nil)
	if tails.P95 != nil || tails.P99 != nil {
		t.Fatalf("Tails() invented a tail from one value: %+v", tails)
	}
	if tails.P50 == nil {
		t.Fatalf("Tails() lost the p50")
	}
	var note string
	_ = note
	n := fmt.Sprint(tails.Note())
	if n == "" {
		t.Fatalf("Tails() must be able to explain the nulled quantiles")
	}
}
