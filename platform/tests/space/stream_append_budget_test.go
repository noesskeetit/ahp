package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The durable stream is the run's source of truth. A real model turn legitimately runs for
// minutes, so the append budget MUST bound one AppendStream call and never the lifetime of the
// run: a shared window silently converted every healthy long run into status=failed with no
// error step and no terminal status event (measured on the composed stand, run
// 20261031T004610.095877438Z_mvslcmzsbcwmqke2hs7yanidky).

type appendObserver struct {
	base  http.RoundTripper
	delay time.Duration

	mu      sync.Mutex
	remains []time.Duration
}

func (o *appendObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/AppendStream") {
		rem := time.Duration(-1)
		if d, ok := r.Context().Deadline(); ok {
			rem = time.Until(d)
		}
		o.mu.Lock()
		o.remains = append(o.remains, rem)
		o.mu.Unlock()
		if o.delay > 0 {
			time.Sleep(o.delay)
		}
	}
	return o.base.RoundTrip(r)
}

func (o *appendObserver) snapshot() []time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]time.Duration, len(o.remains))
	copy(out, o.remains)
	return out
}

// pacedCore emits `steps` model results with a real gap between them, then the terminal done
// result - the same shape a model turn has when one step takes long.
type pacedCore struct {
	steps int
	gap   time.Duration
}

func (p *pacedCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	for i := 0; i < p.steps; i++ {
		select {
		case <-ctx.Done():
			results <- space.StepResult{StepType: space.StepError, Done: true}
			return ctx.Err()
		case <-time.After(p.gap):
		}
		results <- space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`{"tick":1}`)}
	}
	results <- space.StepResult{StepType: space.StepDone, Done: true}
	return nil
}

// countDurableStatus counts the terminal status events this turn managed to make durable.
// The mock keeps one flat stream for the whole process, so the StepKey prefix scopes it to
// this turn (StepKey is turnID + ":" + step).
func countDurableStatus(ms *mockState, turnID string) int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	prefix := turnID + ":"
	n := 0
	for _, item := range ms.streams {
		if item.Kind == "status" && strings.HasPrefix(item.Key, prefix) {
			n++
		}
	}
	return n
}

func waitForTerminal(t *testing.T, coord *space.Coordinator, turnID string, within time.Duration) space.Turn {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if snap, ok := coord.TurnSnapshot(turnID); ok &&
			(snap.Status == space.TurnDone || snap.Status == space.TurnFailed || snap.Status == space.TurnCanceled) {
			return snap
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("turn %s never reached a terminal status within %s", turnID, within)
	return space.Turn{}
}

// TestStreamAppendBudgetBoundsOneCallNotTheRun is the regression: the run's wall clock is made
// several times longer than the append budget. A per-call budget survives; a window shared over
// the run kills it, and that is exactly the failure we measured in production.
func TestStreamAppendBudgetBoundsOneCallNotTheRun(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)

	const budget = 60 * time.Millisecond
	obs := &appendObserver{base: http.DefaultTransport}
	sc := space.NewStateClient(srv.URL, &http.Client{Transport: obs})
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient:         sc,
		Core:                &pacedCore{steps: 6, gap: 60 * time.Millisecond},
		WorkerID:            "w-budget",
		StreamAppendTimeout: budget,
	})

	scope := contracts.Scope{Tenant: "t-budget", Deployment: "d1", Subject: "user-budget"}
	started := time.Now()
	resp, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "budget-" + contracts.NewID(),
		ContextID: "ctx-budget",
		Input:     json.RawMessage(`{"role":"user","content":"go"}`),
		MaxSteps:  8,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	snap := waitForTerminal(t, coord, resp.TurnID, 15*time.Second)
	elapsed := time.Since(started)
	if snap.Status != space.TurnDone {
		t.Fatalf("long run status = %s, want done (a run longer than the append budget must still close)", snap.Status)
	}
	if elapsed < 3*budget {
		t.Fatalf("test lost its teeth: run took %s, must exceed 3x budget %s", elapsed, 3*budget)
	}
	// The local turn projection is set before the final AppendStream RPC.
	// Await the durable boundary under test before examining its calls or
	// closing the test server; a missing/duplicate terminal event still fails.
	deadline := time.Now().Add(2 * time.Second)
	for countDurableStatus(ms, resp.TurnID) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	remains := obs.snapshot()
	if len(remains) < 7 {
		t.Fatalf("observed %d AppendStream calls, want >= 7 (6 steps + terminal done)", len(remains))
	}
	for i, rem := range remains {
		if rem <= 0 {
			t.Fatalf("append %d carried an already-expired deadline (%s): the budget is being shared across the run", i, rem)
		}
		if rem > budget*2 {
			t.Fatalf("append %d deadline %s exceeds twice the budget %s: budget not applied per append", i, rem, budget)
		}
	}

	if got := countDurableStatus(ms, resp.TurnID); got != 1 {
		t.Fatalf("terminal status events = %d, want exactly 1 durable close", got)
	}
}

// TestStreamAppendBudgetStillFailsClosed proves the fix did not remove the guarantee: one append
// that overruns its own budget is a durability hole and must stop the run.
func TestStreamAppendBudgetStillFailsClosed(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)

	obs := &appendObserver{base: http.DefaultTransport, delay: 250 * time.Millisecond}
	sc := space.NewStateClient(srv.URL, &http.Client{Transport: obs})
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient:         sc,
		Core:                &pacedCore{steps: 2, gap: 10 * time.Millisecond},
		WorkerID:            "w-hole",
		StreamAppendTimeout: 40 * time.Millisecond,
	})

	scope := contracts.Scope{Tenant: "t-hole", Deployment: "d1", Subject: "user-hole"}
	resp, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "hole-" + contracts.NewID(),
		ContextID: "ctx-hole",
		Input:     json.RawMessage(`{"role":"user","content":"go"}`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	snap := waitForTerminal(t, coord, resp.TurnID, 15*time.Second)
	if snap.Status == space.TurnDone {
		t.Fatal("a stream hole was reported as done: fail-closed was lost")
	}
	if got := countDurableStatus(ms, resp.TurnID); got != 0 {
		t.Fatalf("durable status events = %d on a hole, want 0 (the hole itself is unrecordable)", got)
	}
}
