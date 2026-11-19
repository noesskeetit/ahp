//go:build state_integration

package space_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// R4, the part the cancellation itself cannot see from inside its own answer.
//
// Cancelling a run parked on its own HITL gate is a user decision to STOP, not a
// decision to UNDO: the tool results and artifact records the run already emitted
// durably before it parked were accepted by State while the lease was live, and a
// cancel that cleared them would turn "the user stopped the turn" into "the effects
// the user already saw never happened" - the exact fabrication this project refuses
// everywhere else (the ledger keeps SIDE_EFFECT_UNKNOWN after a crash for the same
// reason). The mirror hazard is the other direction: a canceled run that stays
// claimable gets taken up by the FIFO again and performs its accepted effect a
// SECOND time.
//
// So this test pins both halves against the real State over PostgreSQL, and reads
// the evidence out of band - straight from ahp_state.stream_events / ahp_state.runs,
// not from the plane that wrote them:
//
//  1. every accepted effect row survives the cancel with identical key, sequence
//     and digest, and the stream grows by exactly ONE row: the terminal frame;
//  2. the canceled run is never handed out again, while the next run of the same
//     thread is - the queue unblocks without the canceled run re-executing;
//  3. the terminal verdict is durable and NAMED (close_reason=canceled), so a reader
//     can tell "the user stopped it" from "the model finished";
//  4. a repeat cancel is idempotent: still one terminal frame, still one of each effect.
//
// Scope note, said out loud rather than assumed: this environment runs the
// coordinator without a RunGrantController, so the identity-side revoke leg is not
// what is under test here (cancel_durable_close_test.go and cancel_failclosed_test.go
// hold that). The contribution of this file is the durable-effect boundary.
type durableRow struct {
	Key      string
	Kind     string
	Sequence int64
	Digest   string
}

func streamRowsForRun(t *testing.T, threadID, runID string) []durableRow {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("set AHP_TEST_DATABASE_URL or DATABASE_URL")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("out-of-band pool: %v", err)
	}
	defer pool.Close()
	rows, err := pool.Query(t.Context(), `SELECT key,kind,sequence,digest FROM ahp_state.stream_events
   WHERE thread_id=$1 AND run_id=$2 ORDER BY sequence`, threadID, runID)
	if err != nil {
		t.Fatalf("out-of-band read: %v", err)
	}
	defer rows.Close()
	var out []durableRow
	for rows.Next() {
		var r durableRow
		if err := rows.Scan(&r.Key, &r.Kind, &r.Sequence, &r.Digest); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func runRowStatus(t *testing.T, runID string) (status, closeReason, waitFor string) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatalf("out-of-band pool: %v", err)
	}
	defer pool.Close()
	err = pool.QueryRow(t.Context(), `SELECT status, COALESCE(payload->>'close_reason',''), wait_for
   FROM ahp_state.runs WHERE id=$1`, runID).Scan(&status, &closeReason, &waitFor)
	if err != nil {
		t.Fatalf("out-of-band run read: %v", err)
	}
	return
}

func TestCancelOfAParkedRunKeepsAcceptedEffectsDurableRealState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "ingress")
	th, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-1", "r4-effects-thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.client.CreateRun(e.ctx, tok, th.ID, "r4-effects-run", json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "effect-worker", 2*time.Minute)
	if err != nil || claim == nil || claim.Lease == nil {
		t.Fatalf("claim: %v", err)
	}
	worker, epoch := claim.Lease.Worker, claim.Lease.Epoch

	// Two accepted side effects, emitted while the lease is live. Distinct bodies, so a
	// key collision could not hide a missing row behind a dedupe.
	accepted := []struct {
		key  string
		body json.RawMessage
	}{
		{space.StepKey(run.ID, 0), json.RawMessage(`{"tool":"write_artifact","object":"h22/r4-first.txt","version":"v1"}`)},
		{space.StepKey(run.ID, 1), json.RawMessage(`{"tool":"write_artifact","object":"h22/r4-second.txt","version":"v1"}`)},
	}
	for _, step := range accepted {
		if _, err := e.client.AppendStream(e.ctx, claim.TaskToken, th.ID, run.ID, worker, epoch, step.key, "tool", step.body); err != nil {
			t.Fatalf("accepted effect %s not durable: %v", step.key, err)
		}
	}
	before := streamRowsForRun(t, th.ID, run.ID)
	if len(before) != len(accepted) {
		t.Fatalf("pre-cancel stream rows=%d want %d: %+v", len(before), len(accepted), before)
	}

	// The run parks on its OWN gate, releasing the lease - the state R4 is about.
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: worker})
	coord.RehydrateTurn(space.Turn{ID: run.ID, RunID: run.ID, ThreadID: th.ID, Status: space.TurnRunning, LeaseWorker: worker, LeaseEpoch: epoch})
	gate, err := coord.SubmitHITL(e.ctx, tok, space.HITLSubmitRequest{TurnID: run.ID, Kind: "approval", Prompt: json.RawMessage(`{"question":"continue?"}`)}, run.ID, worker, epoch)
	if err != nil {
		t.Fatal(err)
	}
	// A second turn queued behind the park: with the park unresolved the thread's FIFO is
	// blocked, which is the queue-locking half of R4.
	if _, err := e.client.CreateRun(e.ctx, tok, th.ID, "r4-next-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatal(err)
	}

	if err := coord.CancelTurn(e.ctx, run.ID, tok); err != nil {
		t.Fatalf("cancel of a run parked on its own gate: %v", err)
	}

	// (1) Out of band: nothing the run already got accepted was eaten, and the only new
	// row is the terminal frame.
	after := streamRowsForRun(t, th.ID, run.ID)
	cancelKey := space.CancelKey(run.ID)
	byKey := map[string]durableRow{}
	terminalFrames := 0
	for _, row := range after {
		if row.Key == cancelKey {
			terminalFrames++
			continue
		}
		byKey[row.Key] = row
	}
	if terminalFrames != 1 {
		t.Fatalf("terminal frames=%d want exactly 1: %+v", terminalFrames, after)
	}
	for _, step := range accepted {
		got, ok := byKey[step.key]
		if !ok {
			t.Fatalf("cancel ate accepted effect %s: stream now %+v", step.key, after)
		}
		var want durableRow
		for _, b := range before {
			if b.Key == step.key {
				want = b
			}
		}
		if got.Sequence != want.Sequence || got.Digest != want.Digest || got.Kind != want.Kind {
			t.Fatalf("accepted effect %s was rewritten by the cancel: before %+v after %+v", step.key, want, got)
		}
	}
	if len(after) != len(before)+1 {
		t.Fatalf("stream grew by %d rows, want exactly 1 (the terminal frame): before %d after %+v", len(after)-len(before), len(before), after)
	}

	// (3) The terminal verdict is durable and named.
	status, closeReason, waitFor := runRowStatus(t, run.ID)
	if status != "canceled" {
		t.Fatalf("durable run status=%q want canceled", status)
	}
	if closeReason != string(space.CloseReasonCanceled) {
		t.Fatalf("close_reason=%q want %q: a terminal state without a reason makes the reader guess", closeReason, space.CloseReasonCanceled)
	}
	if waitFor != "" {
		t.Fatalf("canceled run still waits on %q: the gate was not released", waitFor)
	}

	// (2) The canceled run is not re-executed, the queue behind it is not blocked.
	next, err := e.client.ClaimRun(e.ctx, "", "next-worker", 2*time.Minute)
	if err != nil || next == nil || next.Lease == nil {
		t.Fatalf("next run not claimable after the cancel: %+v %v", next, err)
	}
	if next.Lease.Run.ID == run.ID {
		t.Fatal("the canceled run was handed out again: its accepted effect would run a second time")
	}

	// (4) Idempotence: the same decision arriving twice must not double anything.
	if err := coord.CancelTurn(e.ctx, run.ID, tok); err != nil {
		t.Fatalf("repeat cancel: %v", err)
	}
	repeat := streamRowsForRun(t, th.ID, run.ID)
	if len(repeat) != len(after) {
		t.Fatalf("repeat cancel grew the stream: before %+v after %+v", after, repeat)
	}
	// And the answered gate must not resurrect the canceled run.
	if err := coord.RespondHITL(e.ctx, tok, space.HITLRespondRequest{HITLID: gate.ID, Approved: true}, th.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("answer to a canceled gate accepted: %v", err)
	}
	if status, _, _ := runRowStatus(t, run.ID); status != "canceled" {
		t.Fatalf("answering the canceled gate moved the run to %q", status)
	}
}
