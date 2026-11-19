package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// fifoCore records the turn each Core run was invoked with and then blocks
// until released, so a single dispatch->claim happens in lockstep. This makes
// the order in which the Coordinator hands accepted turns to the State FIFO
// queue deterministically observable, independent of goroutine release timing.
type fifoCore struct {
	entered chan string
	release chan struct{}
}

func (c *fifoCore) Run(_ context.Context, req *space.CoreRequest, _ chan<- space.StepResult) error {
	c.entered <- req.TurnID
	<-c.release
	return nil
}

// TestCoordinatorDispatchesInFIFOOrder proves contract item 1 "FIFO" at the
// Coordinator layer: accepted turns must be claimed/dispatched in acceptance
// order (oldest pending first), as the State queue guarantees. The Coordinator
// keeps turns in a map, so without a FIFO guarantee a later refactor could
// dispatch out of order; this test pins the contract.
func TestCoordinatorDispatchesInFIFOOrder(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	core := &fifoCore{entered: make(chan string, 8), release: make(chan struct{})}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        core,
		WorkerID:    "w-fifo",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx := context.Background()

	accepted := make([]string, 3)
	for i := range accepted {
		resp, err := coord.AcceptTurn(ctx, "token", scope, space.AcceptTurnRequest{
			Key:       "fifo-key-" + contracts.NewID(),
			ContextID: "fifo-ctx-" + contracts.NewID(),
			Input:     json.RawMessage(`"go"`),
			MaxSteps:  4,
		})
		if err != nil {
			t.Fatalf("AcceptTurn %d: %v", i, err)
		}
		if resp.TurnID == "" {
			t.Fatalf("AcceptTurn %d returned empty turn id", i)
		}
		accepted[i] = resp.TurnID
	}

	// Dispatch one at a time; each Core run blocks at entry until the next
	// ClaimRun, so the i-th dispatch must claim the i-th accepted turn.
	for i, want := range accepted {
		if err := coord.DispatchTurn(ctx, "token", scope); err != nil {
			t.Fatalf("DispatchTurn %d: %v", i, err)
		}
		select {
		case got := <-core.entered:
			if got != want {
				t.Fatalf("dispatch %d entered turn %s, but FIFO expects %s (accepted order: %v)", i, got, want, accepted)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("dispatch %d: Core never entered (claim did not start the turn)", i)
		}
	}

	close(core.release)
	// Let the three executeTurn goroutines drain, finalize status and Release.
	time.Sleep(200 * time.Millisecond)
}
