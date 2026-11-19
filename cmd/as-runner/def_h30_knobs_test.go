package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// DEF-H30-x: the two env knobs must change behavior only opt-in. These are pure parser
// tests - deterministic, no deps, run on darwin without PG/Docker.

func h30KnobLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// AS_TURN_DISPATCH_TIMEOUT_MS: unset => 0 => (space decodes to the default window);
// explicit positive => the stated window; negative => -1 => disabled.
func TestParseTurnDispatchTimeout_OptIn(t *testing.T) {
	l := h30KnobLogger()
	if got := parseTurnDispatchTimeout("", l); got != 0 {
		t.Fatalf("unset = %v, want 0 (=> space default)", got)
	}
	if got := parseTurnDispatchTimeout("  ", l); got != 0 {
		t.Fatalf("blank = %v, want 0", got)
	}
	if got := parseTurnDispatchTimeout("not-a-number", l); got != 0 {
		t.Fatalf("unparsable = %v, want 0 (falls back to default)", got)
	}
	if got := parseTurnDispatchTimeout("500", l); got != 500*time.Millisecond {
		t.Fatalf("500 = %v, want 500ms", got)
	}
	if got := parseTurnDispatchTimeout("-1", l); got != -1 {
		t.Fatalf("negative = %v, want -1 (disable)", got)
	}
}

// AS_CLAIM_INTERVAL_MS drives whether a dispatcher (the orphan claim loop) is declared:
// positive => declared (watchdog never arms), 0/negative => NOT declared (so the DEF-H30-x
// terminalization is the thing that ends a stuck turn). This pins the "dispatcher declared
// <=> sweep interval > 0" invariant the guard depends on.
func TestParseClaimInterval_DeclaresDispatcher(t *testing.T) {
	l := h30KnobLogger()
	if got := parseClaimInterval("2000", l); got <= 0 {
		t.Fatalf("positive interval must be a running dispatcher, got %v", got)
	}
	if got := parseClaimInterval("0", l); got > 0 {
		t.Fatalf("0 must not declare a dispatcher, got %v", got)
	}
	if got := parseClaimInterval("-1", l); got > 0 {
		t.Fatalf("negative must not declare a dispatcher, got %v", got)
	}
}
