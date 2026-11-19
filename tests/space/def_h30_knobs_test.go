package space_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-H30-x deterministic proofs (no PostgreSQL, State double). Unlike the literal-window
// file, these drive the coordinator's dispatcher knobs directly, so they assert the guard
// and the opt-in contract without relying on the 10s default elapsing. They use the new
// config fields, so they compile only on the fixed base - which is the point: they pin the
// knobs. The wall-clock outcome is deterministic because the "registered dispatcher" guard
// is a boolean check (dispatcherKnown), not a race.

func h30Coord(t *testing.T, core space.CoreRunner, cfg func(*space.CoordinatorConfig)) *space.Coordinator {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	c := space.CoordinatorConfig{StateClient: space.NewStateClient(srv.URL, nil), Core: core, WorkerID: "h30-knob"}
	if cfg != nil {
		cfg(&c)
	}
	return space.NewCoordinator(c)
}

func h30AcceptDirect(t *testing.T, c *space.Coordinator, key string) (string, error) {
	t.Helper()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	acc, err := c.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: key, ContextID: "ctx-" + key, Input: []byte(`{"role":"user","content":"go"}`), MaxSteps: 2,
	})
	if err != nil {
		return "", err
	}
	return acc.TurnID, nil
}

// TestDefH30x_DispatcherRegisteredDoesNotFailRunningTurn: a claim loop is registered and
// Core blocks forever, so the turn is genuinely running. With an aggressive tiny window the
// watchdog would fire within 50ms IF it were armed beside a dispatcher; assert it never
// fails the running turn with no_turn_dispatcher. The guard is a boolean, so this is not a
// timing race: we only wait to confirm the loop claimed it, then add fixed margin.
func TestDefH30x_DispatcherRegisteredDoesNotFailRunningTurn(t *testing.T) {
	core := &stubCore{blockForever: true}
	coord := h30Coord(t, core, func(c *space.CoordinatorConfig) { c.TurnDispatchTimeout = 50 * time.Millisecond })
	turnID, err := h30AcceptDirect(t, coord, "h30-run")
	if err != nil {
		t.Fatalf("AcceptTurn with a dispatcher about to run must be accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coord.StartOrphanClaimLoop(ctx, 5*time.Millisecond)

	running := false
	for i := 0; i < 200 && !running; i++ {
		if snap, ok := coord.TurnSnapshot(turnID); ok && snap.Status == space.TurnRunning {
			running = true
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !running {
		t.Fatal("the dispatcher never started the turn; cannot observe the guard")
	}
	// Far past the tiny window. The watchdog must NOT have touched a running turn.
	time.Sleep(400 * time.Millisecond)
	snap, _ := coord.TurnSnapshot(turnID)
	if snap.Status == space.TurnFailed && snap.FailureReason == space.ReasonNoTurnDispatcher {
		t.Fatalf("DEF-H30-x regression: watchdog failed a turn a registered dispatcher is running (status=%q reason=%q)", snap.Status, snap.FailureReason)
	}
}

// TestDefH30x_RequireDispatcher_RefusesWhenNone: with AS_REQUIRE_TURN_DISPATCHER's effect
// (RequireDispatcher=true) and NO dispatcher declared, AcceptTurn must honestly refuse with
// 503 (contracts.ErrUnavailable) naming no_turn_dispatcher, before any durable write.
func TestDefH30x_RequireDispatcher_RefusesWhenNone(t *testing.T) {
	coord := h30Coord(t, &stubCore{}, func(c *space.CoordinatorConfig) { c.RequireDispatcher = true })
	_, err := h30AcceptDirect(t, coord, "h30-req-none")
	if err == nil {
		t.Fatal("RequireDispatcher with no dispatcher must refuse, got success")
	}
	if contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("require-dispatcher refusal code = %q, want %q (503)", contracts.Code(err), contracts.ErrUnavailable)
	}
	if !strings.Contains(err.Error(), space.ReasonNoTurnDispatcher) {
		t.Fatalf("require-dispatcher refusal must name %q, got %q", space.ReasonNoTurnDispatcher, err.Error())
	}
}

// TestDefH30x_RequireDispatcher_AcceptsWhenDeclared: the same requirement is satisfied once
// a dispatcher is declared - proving the refusal tracks the dispatcher, not just config.
func TestDefH30x_RequireDispatcher_AcceptsWhenDeclared(t *testing.T) {
	coord := h30Coord(t, &stubCore{}, func(c *space.CoordinatorConfig) { c.RequireDispatcher = true })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coord.StartOrphanClaimLoop(ctx, time.Hour) // declares a dispatcher without ticking fast
	if _, err := h30AcceptDirect(t, coord, "h30-req-declared"); err != nil {
		t.Fatalf("RequireDispatcher with a declared dispatcher must accept, got %v", err)
	}
}

// TestDefH30x_DefaultDoesNotRefuse: opt-in proof. With RequireDispatcher left at its
// default (false), AcceptTurn accepts the turn (into the bounded watchdog) rather than
// refusing - the default deployment never 503s a human turn.
func TestDefH30x_DefaultDoesNotRefuse(t *testing.T) {
	coord := h30Coord(t, &stubCore{}, nil) // RequireDispatcher=false (default)
	if _, err := h30AcceptDirect(t, coord, "h30-default"); err != nil {
		t.Fatalf("the default must not refuse a human turn, got %v (opt-in contract broken)", err)
	}
}

// TestDefH30x_TurnDispatchTimeoutSmall_Terminalizes: setting AS_TURN_DISPATCH_TIMEOUT_MS
// small makes the no-dispatcher terminalization observable quickly (fast GREEN proof).
func TestDefH30x_TurnDispatchTimeoutSmall_Terminalizes(t *testing.T) {
	coord := h30Coord(t, &stubCore{}, func(c *space.CoordinatorConfig) { c.TurnDispatchTimeout = 150 * time.Millisecond })
	turnID, err := h30AcceptDirect(t, coord, "h30-small")
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var status space.TurnStatus
	var reason string
	for time.Now().Before(deadline) {
		snap, _ := coord.TurnSnapshot(turnID)
		status, reason = snap.Status, snap.FailureReason
		if status.IsTerminal() {
			if status != space.TurnFailed || reason != space.ReasonNoTurnDispatcher {
				t.Fatalf("small-window terminalization produced status=%q reason=%q, want failed/%q", status, reason, space.ReasonNoTurnDispatcher)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("small TurnDispatchTimeout did not terminalize within 3s (status=%q)", status)
}

// TestDefH30x_TurnDispatchTimeoutDisabled_KeepsPending: setting the window negative
// (AS_TURN_DISPATCH_TIMEOUT_MS<0) disables the watchdog. Nothing else advances a turn with
// no dispatcher, so the turn must stay pending - proving the knob only changes behavior
// opt-in and the default is the only thing that ever terminalizes here.
func TestDefH30x_TurnDispatchTimeoutDisabled_KeepsPending(t *testing.T) {
	coord := h30Coord(t, &stubCore{}, func(c *space.CoordinatorConfig) { c.TurnDispatchTimeout = -1 })
	turnID, err := h30AcceptDirect(t, coord, "h30-disabled")
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	time.Sleep(400 * time.Millisecond) // > would-be window; disabled watchdog must not fire
	snap, ok := coord.TurnSnapshot(turnID)
	if !ok {
		t.Fatal("coordinator lost the turn")
	}
	if snap.Status != space.TurnPending {
		t.Fatalf("with the watchdog disabled and no dispatcher, turn should stay pending, got status=%q reason=%q", snap.Status, snap.FailureReason)
	}
}
