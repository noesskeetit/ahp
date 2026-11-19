package toolsapi

import (
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/operations"
)

// TestRealMCP round-trip proves the whole gateway path over a REAL go-sdk MCP
// server on loopback: a read-only Call reaches the tool and returns SUCCEEDED,
// no egress, no external endpoint.
func TestRealMCPRoundTrip(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	status, body := e.call(t, "Call", tok, `{"key":"k-echo-1","tool":"echo","arguments":{"q":"hi"}}`)
	mustStatus(t, status, 200, body)
	if body["status"] != operations.StatusSucceeded {
		t.Fatalf("echo call = %v, want SUCCEEDED", body)
	}
	if body["key"] != "k-echo-1" {
		t.Fatalf("key not echoed: %v", body["key"])
	}
}

// TestSideEffectIdempotentByKeys: a side-effecting Call is default-deny until the
// canonical digest is allowed; once allowed it records exactly one effect, and a
// second Call with the SAME operation key must not repeat the effect.
func TestSideEffectIdempotentByKeys(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	args := `{"n":1}`
	// default deny
	status, body := e.call(t, "Call", tok, `{"key":"k-w-1","tool":"write","arguments":`+args+`}`)
	mustStatus(t, status, 403, body)

	e.allowDigest(t, "write", json.RawMessage(args))
	status, body = e.call(t, "Call", tok, `{"key":"k-w-1","tool":"write","arguments":`+args+`}`)
	mustStatus(t, status, 200, body)
	if body["status"] != operations.StatusSucceeded {
		t.Fatalf("allowed write = %v, want SUCCEEDED", body)
	}
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("effect ran %d times, want 1", n)
	}
	// repeat the SAME key: replay, no second effect.
	status, body = e.call(t, "Call", tok, `{"key":"k-w-1","tool":"write","arguments":`+args+`}`)
	mustStatus(t, status, 200, body)
	if body["status"] != operations.StatusSucceeded {
		t.Fatalf("replayed write = %v, want SUCCEEDED", body)
	}
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("replayed key repeated the effect: count=%d, want 1", n)
	}
	// The resolved credential actually reached the MCP server (no bypass).
	if got, _ := e.fixture.lastBearer.Load().(string); got == "" {
		t.Fatal("MCP server saw no Authorization header: credential did not flow through")
	}
}

// TestUnknownNotReplayed: an ambiguous side-effect (erroring transport) becomes
// SIDE_EFFECT_UNKNOWN, runs the effect exactly once, and a retry with the same
// key never re-dispatches it.
func TestUnknownNotReplayed(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	args := `{"n":1}`
	e.allowDigest(t, "errwrite", json.RawMessage(args))
	status, body := e.call(t, "Call", tok, `{"key":"k-d-1","tool":"errwrite","arguments":`+args+`}`)
	mustStatus(t, status, 200, body)
	if body["status"] != operations.StatusSideEffectUnknown {
		t.Fatalf("errwrite status = %v, want SIDE_EFFECT_UNKNOWN", body["status"])
	}
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("ambiguous effect ran %d times, want 1", n)
	}
	// Retry the same key: still UNKNOWN, effect never re-runs.
	status, body = e.call(t, "Call", tok, `{"key":"k-d-1","tool":"errwrite","arguments":`+args+`}`)
	mustStatus(t, status, 200, body)
	if body["status"] != operations.StatusSideEffectUnknown {
		t.Fatalf("retried unknown = %v, want SIDE_EFFECT_UNKNOWN", body["status"])
	}
	if n := atomic.LoadInt32(&e.fixture.effCount); n != 1 {
		t.Fatalf("retry re-dispatched the effect: count=%d, want 1", n)
	}
}
