package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests drive the REAL coordinator terminal path (executeTurn) against the
// in-memory State double, with the experimental checkpoint boundary set to turn.
// They need no PostgreSQL: they prove the COORDINATOR's success gating against an
// injected flush failure, not only a store-level owner refusal (Root blocker #2) --
// that a failed boundary flush, or a terminal status frame that cannot commit, must
// never be published as a successful terminal, while a fully durable turn still
// completes.

func waitForTerminalTurn(t *testing.T, coord *space.Coordinator, turnID string) *space.Turn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if turn := getTurnHelper(coord, turnID); turn != nil && turn.Status.IsTerminal() {
			return turn
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("turn never reached a terminal status")
	return nil
}

func runOneTurn(t *testing.T, coord *space.Coordinator, coreDone chan struct{}, subject, key string) string {
	t.Helper()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: subject}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       key + "-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"go"`),
		MaxSteps:  4,
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
		t.Fatal("core did not finish")
	}
	return accept.TurnID
}

// TestCheckpointBoundaryCoordinatorFlushFailureNotDone: a turn whose boundary stream
// flush fails must NOT be reported done -- the failure flips the run to failed before
// any successful terminal is published or released.
func TestCheckpointBoundaryCoordinatorFlushFailureNotDone(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	ms := newMockState()
	ms.armFlushFault()
	srv := ms.Server()
	defer srv.Close()

	coreDone := make(chan struct{})
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        &stubCore{done: coreDone},
		WorkerID:    "w-flush",
	})
	turnID := runOneTurn(t, coord, coreDone, "s-flush", "cb-flush")

	turn := waitForTerminalTurn(t, coord, turnID)
	if turn.Status == space.TurnDone {
		t.Fatal("a failed boundary flush must not publish a successful terminal")
	}
	if turn.Status != space.TurnFailed {
		t.Fatalf("failed flush must yield failed, got %s", turn.Status)
	}
}

// TestCheckpointBoundaryCoordinatorTerminalFrameFailureNotDone: even when the tail
// flush succeeds, a terminal "status" frame that cannot be committed must not leave
// the turn done -- success is gated on that commit (Root: no terminal success exposed
// before its required commit succeeded).
func TestCheckpointBoundaryCoordinatorTerminalFrameFailureNotDone(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	ms := newMockState()
	ms.armStatusAppendFault() // FlushStream succeeds; only the terminal frame fails
	srv := ms.Server()
	defer srv.Close()

	coreDone := make(chan struct{})
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        &stubCore{done: coreDone},
		WorkerID:    "w-term",
	})
	turnID := runOneTurn(t, coord, coreDone, "s-term", "cb-term")

	turn := waitForTerminalTurn(t, coord, turnID)
	if turn.Status == space.TurnDone {
		t.Fatal("a turn whose terminal frame could not commit must not be done")
	}
	if turn.Status != space.TurnFailed {
		t.Fatalf("uncommitted terminal frame must flip to failed, got %s", turn.Status)
	}
}

// TestCheckpointBoundaryCoordinatorTurnModeCompletes is the positive control: with
// the boundary flush and the terminal frame both durable, turn mode still reaches
// done -- the success gate does not fail spuriously and turn mode is not broken.
func TestCheckpointBoundaryCoordinatorTurnModeCompletes(t *testing.T) {
	t.Setenv(contracts.EnvCheckpointBoundary, "turn")
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	coreDone := make(chan struct{})
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        &stubCore{done: coreDone},
		WorkerID:    "w-ok",
	})
	turnID := runOneTurn(t, coord, coreDone, "s-ok", "cb-ok")

	turn := waitForTerminalTurn(t, coord, turnID)
	if turn.Status != space.TurnDone {
		t.Fatalf("a fully durable turn-mode run must still complete done, got %s", turn.Status)
	}
}
