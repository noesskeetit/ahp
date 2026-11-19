package space_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEFECT-1 (core-before-hwm) has a second half: whenever executeTurn stops consuming the Core
// step channel or detects a durable hazard, it MUST cancel Core and JOIN it, otherwise the Core
// goroutine blocks forever on `results` and the run leaks (holding a lease-sized amount of work
// in memory and never reporting a terminal state).
//
// Both cores below are leak detectors: they record whether Run actually RETURNED. A coordinator
// that stops reading the channel, or that never cancels Core, leaves returned=0 forever.

// spLoopingCore produces steps indefinitely until its context is cancelled or the consumer drains
// every write. Used to prove a durable stream hole stops execution instead of only stopping
// emission.
type spLoopingCore struct {
	calls     int32
	returned  int32
	sawCancel int32
	steps     int32
}

func (c *spLoopingCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	atomic.AddInt32(&c.calls, 1)
	defer atomic.StoreInt32(&c.returned, 1)
	for {
		select {
		case <-ctx.Done():
			atomic.StoreInt32(&c.sawCancel, 1)
			return ctx.Err()
		default:
		}
		select {
		case results <- space.StepResult{StepType: space.StepTool, Content: json.RawMessage(`{"n":"tool"}`)}:
			atomic.AddInt32(&c.steps, 1)
		case <-ctx.Done():
			atomic.StoreInt32(&c.sawCancel, 1)
			return ctx.Err()
		}
	}
}

// spErrorFloodCore emits StepError first and then keeps producing more steps than the buffered
// channel can hold: exactly what a model/tool loop does while nobody drains it. The old
// `break`-on-StepError left such a Core blocked forever.
type spErrorFloodCore struct {
	calls     int32
	returned  int32
	sawCancel int32
}

func (c *spErrorFloodCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	atomic.AddInt32(&c.calls, 1)
	defer atomic.StoreInt32(&c.returned, 1)
	send := func(r space.StepResult) bool {
		select {
		case results <- r:
			return true
		case <-ctx.Done():
			atomic.StoreInt32(&c.sawCancel, 1)
			return false
		}
	}
	if !send(space.StepResult{StepType: space.StepError, Content: json.RawMessage(`{"error":"injected step failure"}`)}) {
		return ctx.Err()
	}
	for i := 0; i < 200; i++ {
		if !send(space.StepResult{StepType: space.StepTool, Content: json.RawMessage(`{"n":"tool"}`)}) {
			return ctx.Err()
		}
	}
	return nil
}

func newSPJoinCoordinator(m *spFaultState, core space.CoreRunner) *space.Coordinator {
	return space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(m.srv.URL, nil),
		Core:        core,
		WorkerID:    "w1",
	})
}

// TestStreamAppendFaultCancelsAndJoinsCore: the first durable append succeeds, the next one fails.
// The run must be reported failed, must present no further durable writes past the hole, must
// CANCEL Core (no more tool side effects on an unrecordable run) and must JOIN it (no leak).
func TestStreamAppendFaultCancelsAndJoinsCore(t *testing.T) {
	m := newSPFaultState(t)
	m.failAppendsAfter = 1
	core := &spLoopingCore{}
	coord := newSPJoinCoordinator(m, core)
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := m.waitRelease(t, 10*time.Second)
	if !ok {
		t.Fatal("no Release observed: executeTurn never completed after the durable stream hole")
	}
	if status != "failed" {
		t.Fatalf("a durable stream hole must fail the run, got %q", status)
	}
	if atomic.LoadInt32(&core.calls) != 1 {
		t.Fatalf("Core must have been started exactly once, calls=%d", atomic.LoadInt32(&core.calls))
	}
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt32(&core.returned) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&core.sawCancel) != 1 {
		t.Fatal("DEFECT-1: a durable stream hole must cancel Core, not merely stop emitting")
	}
	if atomic.LoadInt32(&core.returned) != 1 {
		t.Fatal("DEFECT-1: executeTurn returned without joining the Core goroutine (leaked goroutine)")
	}
	if got := atomic.LoadInt32(&m.appends); got > 1 {
		t.Fatalf("no successful append is allowed past the durable hole; appends=%d", got)
	}
}

// TestStepErrorDrainsAndJoinsCore: a step error must not stop draining the Core channel. The run
// stays failed, but Core must be able to finish and be joined (the old `break` leaked it).
func TestStepErrorDrainsAndJoinsCore(t *testing.T) {
	m := newSPFaultState(t)
	core := &spErrorFloodCore{}
	coord := newSPJoinCoordinator(m, core)
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := m.waitRelease(t, 10*time.Second)
	if !ok {
		t.Fatal("no Release observed after the errored step")
	}
	if status != "failed" {
		t.Fatalf("an errored step must fail the run, got %q", status)
	}
	if atomic.LoadInt32(&core.returned) != 1 {
		t.Fatal("DEFECT-1: Core was left blocked on the step channel (break-on-error leak); it must be drained and joined")
	}
	if got := atomic.LoadInt32(&m.appends); got < 1 {
		t.Fatalf("the errored step and the terminal status must still be recorded; appends=%d", got)
	}
}

// TestCleanRunJoinsCore is the positive control for the two leak tests above: a Core that returns
// normally is joined and the run completes, so the returned=1 assertions are non-vacuous.
func TestCleanRunJoinsCore(t *testing.T) {
	m := newSPFaultState(t)
	core := &spCountingCore{}
	coord := newSPJoinCoordinator(m, core)
	if err := coord.DispatchTurn(context.Background(), "tok-sp-ingress", contracts.Scope{}); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	if status, ok := m.waitRelease(t, 10*time.Second); !ok || status != "done" {
		t.Fatalf("positive control must complete cleanly, status=%q ok=%v", status, ok)
	}
	if got := atomic.LoadInt32(&core.calls); got != 1 {
		t.Fatalf("positive control: Core calls=%d, want 1", got)
	}
}
