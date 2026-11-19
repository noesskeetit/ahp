//go:build state_integration

package space_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The third R4 requirement, from the root audit: a run parked on its own gate must
// not be able to reach COMPLETED under somebody else's fence, and must not be able
// to reach it AFTER a cancel either.
//
// Both halves ride on one mechanism, and this test names it instead of trusting the
// answer: a park RELEASES the thread lease (the run is waiting, nobody is executing
// it), and State's Release validates the caller against `threads.lease_run_id /
// lease_worker / lease_epoch` (internal/state/store.go currentLease). So an
// intruder - and, on the second half, the run's own former lease holder - arrives
// with a fence that is no longer the current one. What must not happen is the
// silent version of this: a run that gets stamped `done`/`model_finished` by a
// writer who is not holding it, over a user decision that said `canceled`.
//
// Everything is read out of band from ahp_state.runs, because the question is what
// the durable row says, not what a plane reported.
func TestParkedRunCannotReachCompletedUnderAForeignFenceOrAfterCancel(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	e := cNewEnv(t)
	tok := e.signTask(t, "ingress")
	th, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-1", "r4-fence-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.client.CreateRun(e.ctx, tok, th.ID, "r4-fence-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "fence-worker", 2*time.Minute)
	if err != nil || claim == nil || claim.Lease == nil {
		t.Fatalf("claim: %v", err)
	}
	worker, epoch := claim.Lease.Worker, claim.Lease.Epoch

	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: worker})
	coord.RehydrateTurn(space.Turn{ID: run.ID, RunID: run.ID, ThreadID: th.ID, Status: space.TurnRunning, LeaseWorker: worker, LeaseEpoch: epoch})
	if _, err := coord.SubmitHITL(e.ctx, tok, space.HITLSubmitRequest{TurnID: run.ID, Kind: "approval", Prompt: json.RawMessage(`{"question":"continue?"}`)}, run.ID, worker, epoch); err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.New(e.ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	durable := func() (status, closeReason, waitFor string) {
		t.Helper()
		err := pool.QueryRow(e.ctx, `SELECT status, COALESCE(payload->>'close_reason',''), wait_for
   FROM ahp_state.runs WHERE id=$1`, run.ID).Scan(&status, &closeReason, &waitFor)
		if err != nil {
			t.Fatalf("out-of-band run read: %v", err)
		}
		return
	}
	release := func(attemptedWorker string, attemptedEpoch int64) (int, map[string]any) {
		t.Helper()
		return e.raw("Release", tok, []byte(`{"run_id":"`+run.ID+`","worker":"`+attemptedWorker+
			`","epoch":`+fmt.Sprintf("%d", attemptedEpoch)+`,"status":"done","close_reason":"model_finished"}`))
	}

	if status, _, waitFor := durable(); status != "waiting" || waitFor == "" {
		t.Fatalf("fixture is not parked: status=%q wait_for=%q", status, waitFor)
	}

	// (a) A foreign fence cannot complete a parked run.
	if code, body := release("intruder-worker", epoch+7); code == http.StatusOK {
		t.Fatalf("a stranger's fence completed a parked run: %v", body)
	}
	if status, closeReason, _ := durable(); status == "done" || closeReason == "model_finished" {
		t.Fatalf("the run row moved to a verdict nobody was entitled to write: status=%q close_reason=%q", status, closeReason)
	}

	// (b) After the user cancels, not even the run's OWN former fence may stamp a
	// verdict. This is the case that would turn a cancel into a lost user decision:
	// the row says canceled, a late writer says model_finished, and a reader of the
	// transcript sees a run that finished after being stopped.
	if err := coord.CancelTurn(e.ctx, run.ID, tok); err != nil {
		t.Fatalf("cancel of the parked run: %v", err)
	}
	before, beforeReason, _ := durable()
	if before != "canceled" {
		t.Fatalf("cancel left the run %q", before)
	}
	if code, body := release(worker, epoch); code == http.StatusOK {
		t.Fatalf("the former lease holder completed a canceled run: %v", body)
	}
	after, afterReason, _ := durable()
	if after != "canceled" || afterReason != beforeReason {
		t.Fatalf("a late Release rewrote the canceled run: before %q/%q after %q/%q", before, beforeReason, after, afterReason)
	}
}

// The audit half of the cancel contract, measured on both answers.
//
// An accepted cancel is durable by construction: the run row ends canceled with
// close_reason, which the user can read back. A refused one must mutate nothing -
// TestCancelACompletedTurnIsRefusedAndChangesNothing requires exactly that - and a
// refusal that mutates nothing while also logging nothing is indistinguishable, three
// days later, from a request that never arrived. So the DECISION must be visible even
// where the durable record must not move.
func TestCancelDecisionIsAuditableOnBothAnswers(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	e := cNewEnv(t)
	tok := e.signTask(t, "ingress")
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "audit-worker"})

	// Refused: a turn whose outcome was already decided. No run row is created for it,
	// which is also the proof that the refusal touched nothing durable.
	doneTurn := "h22-audit-done-" + contracts.NewID()
	coord.RehydrateTurn(space.Turn{ID: doneTurn, RunID: doneTurn, ThreadID: "h22-audit-thread", Status: space.TurnDone})
	logs.Reset()
	if err := coord.CancelTurn(e.ctx, doneTurn, tok); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("cancel of a done turn: %v", err)
	}
	if !strings.Contains(logs.String(), `cancel_decision=refused`) || !strings.Contains(logs.String(), "turn_id="+doneTurn) {
		t.Fatalf("a refused cancel left no audit line: %s", logs.String())
	}
	if strings.Contains(logs.String(), "cancel_decision=accepted") {
		t.Fatalf("a refused cancel was audited as accepted: %s", logs.String())
	}
	// And the audit line carries no payload: statuses and ids only.
	if strings.Contains(logs.String(), "question") || strings.Contains(logs.String(), "prompt") {
		t.Fatalf("the cancel audit carried gate content: %s", logs.String())
	}

	// Accepted: the parked run from the fixture above.
	th, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-1", "cancel-audit-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.client.CreateRun(e.ctx, tok, th.ID, "cancel-audit-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "audit-worker", 2*time.Minute)
	if err != nil || claim == nil || claim.Lease == nil {
		t.Fatalf("claim: %v", err)
	}
	worker, epoch := claim.Lease.Worker, claim.Lease.Epoch
	live := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: worker})
	live.RehydrateTurn(space.Turn{ID: run.ID, RunID: run.ID, ThreadID: th.ID, Status: space.TurnRunning, LeaseWorker: worker, LeaseEpoch: epoch})
	if _, err := live.SubmitHITL(e.ctx, tok, space.HITLSubmitRequest{TurnID: run.ID, Kind: "approval", Prompt: json.RawMessage(`{"question":"continue?"}`)}, run.ID, worker, epoch); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	if err := live.CancelTurn(e.ctx, run.ID, tok); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), `cancel_decision=accepted`) || !strings.Contains(logs.String(), "run_id="+run.ID) {
		t.Fatalf("an accepted cancel left no audit line: %s", logs.String())
	}
}
