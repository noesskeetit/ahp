package space_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// DEF-H30-x, base-compilable offline proof over the in-memory State double (no PostgreSQL,
// so it runs natively without the Linux gate). This file uses only PRE-FIX public API (no
// new symbols), so it also COMPILES on the pre-fix base, where the test below is RED for
// exactly the reason its failure string names. The window/reason are literals mirroring
// space.DefaultTurnDispatchTimeout (10s) and ReasonNoTurnDispatcher; widen the window
// literal if that default ever grows. The deterministic knob/guard coverage lives in
// def_h30_knobs_test.go (uses the new config fields, fixed-base only); the PostgreSQL HTTP
// sibling def_h30_no_dispatcher_http_test.go is the acceptance gate.

// h30WatchdogWindowLiteral must exceed space.DefaultTurnDispatchTimeout so that, on the
// fixed code, the test observes the watchdog's terminal; on the pre-fix code (no watchdog)
// the whole window elapses and the test fails for the pending-forever reason.
const h30WatchdogWindowLiteral = 15 * time.Second

// h30ReasonLiteral mirrors space.ReasonNoTurnDispatcher.
const h30ReasonLiteral = "no_turn_dispatcher"

// TestDefH30x_Offline_TurnWithNoDispatcherReachesTerminal accepts a turn with NO claim loop
// and no client DispatchTurn.
//   RED (pre-fix): still status="pending" failure_reason="" after the whole window ->
//     t.Fatalf names it verbatim.
//   GREEN (post-fix): the no-dispatcher watchdog turns it to
//     status="failed" failure_reason="no_turn_dispatcher" within the default window.
func TestDefH30x_Offline_TurnWithNoDispatcherReachesTerminal(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil), Core: &stubCore{}, WorkerID: "h30-nodev",
	})
	turnID, _ := acceptTurn(t, coord, "ctx-h30-nodev", "h30-nodev")

	deadline := time.Now().Add(h30WatchdogWindowLiteral)
	var status space.TurnStatus
	var reason string
	for time.Now().Before(deadline) {
		snap, ok := coord.TurnSnapshot(turnID)
		if !ok {
			t.Fatal("DEF-H30-x: coordinator lost the turn it accepted")
		}
		status, reason = snap.Status, snap.FailureReason
		if status.IsTerminal() {
			if status == space.TurnFailed && reason == "" {
				t.Fatalf("DEF-H30-x: terminal failed carries no failure_reason")
			}
			if status == space.TurnFailed && reason != h30ReasonLiteral {
				t.Fatalf("DEF-H30-x: no-dispatcher turn failed with reason %q, want %q", reason, h30ReasonLiteral)
			}
			return // GREEN
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("DEF-H30-x: with no turn dispatcher the accepted turn stayed status=%q failure_reason=%q past %s with no terminal state (expected a determined terminal with a cause)",
		status, reason, h30WatchdogWindowLiteral)
}
