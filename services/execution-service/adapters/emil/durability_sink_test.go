package emil

// durability_sink_test.go — CLIENT-CONFORMANCE only (not a live-proof): a fake State
// HTTP SERVER drives OUR StateClient through RunStoreDurabilitySink -> StateRunStore to
// assert the DurabilitySink contract (idempotent replay, permanent key-reuse reject,
// fail-closed missing run-context) and the wire hygiene (fence in body, scope/tenant/
// subject/deployment NEVER in body). Live State proof of the same sink lives in
// tests/space/sink_realstate_test.go (tag state_integration, real State+PG).

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

func TestRunStoreDurabilitySink_Contract(t *testing.T) {
	ctx := space.WithRunContext(context.Background(), space.RunContext{
		Fence:     space.Fence{RunID: "run1", Worker: "w1", Epoch: 3},
		ThreadID:  "th1",
		TaskToken: "TT",
	})

	var appendBodies []map[string]any
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) {
		switch op {
		case "AppendStream":
			appendBodies = append(appendBodies, body)
			// KEY_REUSE for a specifically poisoned key, mirroring State semantics.
			if body["key"] == "step-9" {
				return 409, map[string]any{"code": "KEY_REUSE", "message": "stream key reused with a different body"}
			}
			return 200, map[string]any{"id": "e1", "thread_id": "th1", "key": body["key"], "kind": body["kind"], "sequence": 1, "payload": body["payload"]}
		default:
			return 200, map[string]any{}
		}
	})
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))

	// fresh append -> replayed=false
	p := json.RawMessage(`{"x":1}`)
	rp, err := sink.AppendDurable(ctx, "step-0", "tool", "fp-a", p)
	if err != nil || rp {
		t.Fatalf("fresh: replayed=%v err=%v, want false/nil", rp, err)
	}
	// identical re-append -> replayed=true (same-process), record unchanged
	rp, err = sink.AppendDurable(ctx, "step-0", "tool", "fp-a", p)
	if err != nil || !rp {
		t.Fatalf("replay: replayed=%v err=%v, want true/nil", rp, err)
	}
	// reused key, different fingerprint -> space.ErrKeyReuse (permanent)
	_, err = sink.AppendDurable(ctx, "step-0", "tool", "fp-B", json.RawMessage(`{"x":2}`))
	if !errors.Is(err, space.ErrKeyReuse) {
		t.Fatalf("reuse: err=%v, want space.ErrKeyReuse", err)
	}
	// State-side KEY_REUSE (fresh key the sink hasn't seen) must SURFACE (fail-closed)
	_, err = sink.AppendDurable(ctx, "step-9", "tool", "fp-x", p)
	if err == nil || !strings.Contains(err.Error(), "KEY_REUSE") {
		t.Fatalf("state KEY_REUSE not surfaced: %v", err)
	}

	// wire hygiene: fence in body; scope/tenant/subject/deployment NEVER in body
	if len(appendBodies) == 0 {
		t.Fatal("no AppendStream observed")
	}
	for _, b := range appendBodies {
		if _, ok := b["fence"]; !ok {
			t.Fatalf("AppendStream body missing fence: %v", b)
		}
		for _, banned := range []string{"scope", "tenant", "subject", "deployment"} {
			if _, ok := b[banned]; ok {
				t.Fatalf("AppendStream body leaked %q: %v", banned, b)
			}
		}
	}
}

func TestRunStoreDurabilitySink_FailClosedNoRunContext(t *testing.T) {
	sc, _ := newFakeState(t, func(op string, body map[string]any) (int, any) { return 200, map[string]any{} })
	sink := NewRunStoreDurabilitySink(NewStateRunStore(sc))
	// No space.RunContext in ctx -> must fail-closed (never a silent skip / never an
	// un-fenced write), and never reach the State client.
	_, err := sink.AppendDurable(context.Background(), "step-0", "tool", "fp", json.RawMessage(`{}`))
	if err == nil || !strings.Contains(err.Error(), "fail-closed") {
		t.Fatalf("expected fail-closed error, got %v", err)
	}
}
