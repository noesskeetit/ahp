package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestCoordinatorStreamKeysUseCanonicalStepKey is a regression guard for the
// iv finding #74 fix as it is wired into the PRODUCTION Coordinator emit path.
//
// A State stream event is idempotent per (thread, key): a constant key would
// collapse every step onto one event (and a constant key carrying a different
// body would be rejected KEY_REUSE_MISMATCH after the first step). The Core
// loop therefore MUST key each emitted step distinctly. The canonical generator
// for that key is space.StepKey(loopID, step); the Coordinator -- the production
// driver of the durable thread stream -- must use it, not a hand-rolled format.
//
// This test runs a real multi-step turn through the Coordinator and asserts the
// recorded stream keys are exactly the canonical per-step keys, in order, with
// no repeats: a regression to a constant or non-canonical key fails here.
func TestCoordinatorStreamKeysUseCanonicalStepKey(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	coreDone := make(chan struct{})
	mock := &stubCore{done: coreDone, stepsBeforeDone: 3}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "stream-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"go"`),
		MaxSteps:  8,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-coreDone:
	case <-time.After(5 * time.Second):
		t.Fatal("core did not complete within 5s")
	}
	time.Sleep(200 * time.Millisecond)

	events, err := sc.ReadStream(context.Background(), "token", accept.ThreadID, 0, 100)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	// 3 model steps + 1 done + 1 status = 5 canonical stream events.
	if len(events) != 5 {
		t.Fatalf("expected 5 stream events, got %d", len(events))
	}
	seen := map[string]bool{}
	for i, ev := range events {
		want := space.StepKey(accept.TurnID, i)
		if ev.Key != want {
			t.Fatalf("stream event %d: key=%q, want canonical %q", i, ev.Key, want)
		}
		if seen[ev.Key] {
			t.Fatalf("duplicate stream key %q: per-step idempotency violated (#74)", ev.Key)
		}
		seen[ev.Key] = true
	}
}
