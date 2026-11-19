//go:build state_integration

package space_test

// coordinator_recovery_realstate_test.go — the REAL-API upgrade of the Coordinator
// "recovery" leg of acceptance #1 ("...FIFO/outbox/steering/recovery"). The mock
// coverage drives Coordinator.Recover against newMockState; this drives the SAME
// production space.Coordinator over a REAL StateClient whose State service runs on
// PostgreSQL (mTLS certs + Ed25519 task tokens), so the turns being recovered are
// bound to REAL, PG-leased runs (the lease is granted by the authoritative State
// service; it cannot be held for a run that is not durably in PostgreSQL).
//
// It pins the two legally-recoverable Coordinator transitions against the
// authoritative State scope/lease:
//   - a turn whose worker vanished while RUNNING becomes TurnUnknown (not silently
//     completed, not lost) so it can be resumed under a fresh epoch;
//   - a turn still PENDING (never claimed) is recovered for re-dispatch.
//
// Honesty: this proves the recovery CONTRACT over real State for turns that are
// loaded into the coordinator's index. Reload-after-full-process-restart is the
// supervisor's job (execution/guardian lane): it must scan State for orphaned runs
// and call Coordinator.RehydrateTurn(...) before Recover(). That integration point is
// reported to the director, NOT silently reimplemented here, and nothing here is a
// real-LLM proof. Missing real PG => cNewEnv t.Skip (state_integration harness).
import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCoordinatorRecover_RealState(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())

	// A REAL thread + run committed to PostgreSQL via the authoritative State API.
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "recover-thread")
	if err != nil {
		t.Fatalf("CreateThread over REAL State: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "recover-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatalf("CreateRun over REAL State: %v", err)
	}

	// A real lease from the authoritative State service (proves the run is durable in
	// PG and yields the authoritative TaskToken/RunID/Worker/Epoch).
	rs := emil.NewStateRunStore(e.client)
	l, err := rs.ClaimLease(ctx, "", "w-recover", 120*time.Second)
	if err != nil {
		t.Fatalf("ClaimLease over REAL State: %v", err)
	}
	if l.RunID != run.ID {
		t.Fatalf("lease RunID %q != created run %q (authoritative lease must bind the real run)", l.RunID, run.ID)
	}

	// Production Coordinator over the REAL StateClient. Register two turns bound to
	// the real PG-leased thread, WITHOUT running the model loop:
	//   tRunning -- a worker that owned a RUNNING turn and vanished (orphaned).
	//   tPending -- a turn accepted but never claimed.
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client,
		Core:        &stubCore{},
		WorkerID:    "w-recover",
	})
	tRunning := "turn-run-" + contracts.NewID()
	tPending := "turn-pend-" + contracts.NewID()
	coord.RehydrateTurn(space.Turn{ID: tRunning, ThreadID: l.ThreadID, Status: space.TurnRunning, MaxSteps: 8})
	coord.RehydrateTurn(space.Turn{ID: tPending, ThreadID: l.ThreadID, Status: space.TurnPending, MaxSteps: 8})

	rec, err := coord.Recover(ctx, l.TaskToken)
	if err != nil {
		t.Fatalf("Recover over REAL State: %v", err)
	}

	var sawUnknown, sawRecovered bool
	for _, tu := range rec.Unknown {
		if tu.ID == tRunning {
			sawUnknown = true
			if tu.Status != space.TurnUnknown {
				t.Fatalf("recovered running turn status = %q, want %q", tu.Status, space.TurnUnknown)
			}
			if tu.ThreadID != l.ThreadID {
				t.Fatalf("unknown turn thread=%s not bound to the real PG run thread %s (run %s)", tu.ThreadID, l.ThreadID, l.RunID)
			}
		}
	}
	for _, tu := range rec.Recovered {
		if tu.ID == tPending {
			sawRecovered = true
			if tu.ThreadID != l.ThreadID {
				t.Fatalf("recovered pending turn thread = %s, want real %s", tu.ThreadID, l.ThreadID)
			}
		}
	}
	if !sawUnknown {
		t.Fatalf("Recover did not mark the orphaned RUNNING turn (bound to real PG run %s) as Unknown; unknown=%+v", l.RunID, rec.Unknown)
	}
	if !sawRecovered {
		t.Fatalf("Recover did not recover the PENDING turn; recovered=%+v", rec.Recovered)
	}

	// Authoritative read-back: the orphan run the coordinator just classified is a
	// real record durably readable from PostgreSQL via the State API (GetRun), not an
	// in-process double. CreateRun + ClaimLease + GetRun all round-trip the SAME run id
	// through the authoritative service, so the recovered turn is bound to real PG state.
	got, err := e.client.GetRun(ctx, l.TaskToken, run.ID)
	if err != nil {
		t.Fatalf("GetRun over REAL State (run=%s): %v", run.ID, err)
	}
	if got == nil || got.ID != run.ID {
		t.Fatalf("REAL State returned the wrong run for the recovered turn: got %+v want id %s", got, run.ID)
	}
}
