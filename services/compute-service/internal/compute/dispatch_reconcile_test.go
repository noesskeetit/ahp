package compute

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-COMPUTE-6, on the real ledger.
//
// A dispatch without a bound is the compute-plane twin of a run that stays WORKING
// forever: the row says "somebody is executing this", the somebody is gone, and every
// consumer that trusts the row inherits that lie for good - the export guard refuses the
// Computer, deletion counts the command as outstanding, an operator cannot tell a live
// command from one that died an hour ago. This file pins the four things that have to be
// true instead, against PostgreSQL rather than a stub:
//
//  1. a dispatch carries a deadline, and a live holder renews it;
//  2. an expired dispatch reaches a TERMINAL verdict, and the verdict is the strongest
//     statement the evidence supports: LOST where no backend was ever invoked,
//     SIDE_EFFECT_UNKNOWN where one was (and the export guard keeps holding that one);
//  3. the verdict does not re-execute the command and does not invent success: a replay
//     of the idempotency key returns the closed row, Restart refuses, a late answer
//     cannot overwrite the verdict, and a zombie holder is refused before it reaches a
//     backend;
//  4. a reconciled LOST stops blocking its owner's export, because the ledger now holds
//     proof that nothing on this dispatch could still be writing.
//
// The deadline is aged out-of-band (an UPDATE moving it into the past) rather than by
// sleeping for the budget: the fact under test is "the budget expired", and waiting
// 30s for it would only make the test slower and just as simulated.
func expireDispatch(t *testing.T, s *Store, computerID, commandID string) {
	t.Helper()
	tag, err := s.Pool().Exec(context.Background(),
		`UPDATE ahp_compute.commands SET dispatch_deadline = clock_timestamp() - interval '1 second'
		 WHERE computer_id = $1 AND id = $2 AND status = 'dispatched'`, computerID, commandID)
	if err != nil {
		t.Fatalf("age the dispatch budget out-of-band: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("the command was not dispatched, so nothing was aged: %d rows", tag.RowsAffected())
	}
}

// closedOf picks this test's own verdict out of a sweep report. The sweep is global by
// design, so a report in a shared database can carry other tenants' expired dispatches;
// filtering by id keeps the assertion about the row this test created.
func closedOf(report ReconcileReport, commandID string) *ReconciledCommand {
	for i := range report.Closed {
		if report.Closed[i].CommandID == commandID {
			return &report.Closed[i]
		}
	}
	return nil
}

func TestStaleDispatchReconcilesToALostTerminalAndUnblocksExport(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "reconcile-lost")
	if err != nil {
		t.Fatal(err)
	}
	rec, mayDispatch, err := s.Exec(ctx, scope, c.ID, "h22-lost", "echo h22", "")
	if err != nil || !mayDispatch {
		t.Fatalf("record the command: %v mayDispatch=%v", err, mayDispatch)
	}
	dispatched, err := s.MarkDispatched(ctx, scope, c.ID, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	// (1) the dispatch is bounded the moment it exists.
	if dispatched.DispatchDeadline.IsZero() {
		t.Fatal("a dispatched command carries no deadline: it could never be reconciled")
	}
	if budget := time.Until(dispatched.DispatchDeadline); budget <= 0 || budget > CommandDispatchBudget+time.Minute {
		t.Fatalf("dispatch budget is not the promised bound: %s", budget)
	}
	if _, err := s.beginExportGuard(ctx, scope, c.ID); contracts.Code(err) != contracts.ErrConflict ||
		contracts.ReasonOf(err) != "export_source_busy" {
		t.Fatalf("a live dispatch must hold the export guard, got %v", err)
	}

	// A sweep with a live budget must not touch THAT dispatch. The sweep is an operator
	// sweep over the whole ledger - it has no tenant scope, because a dispatch whose host
	// died has to be closed whoever owned it - so in a shared database other rows may
	// legitimately appear here, and the claim under test is only about this one.
	before, err := s.ReconcileStaleDispatches(ctx, 64)
	if err != nil {
		t.Fatal(err)
	}
	if closedOf(before, rec.ID) != nil {
		t.Fatalf("a sweep closed a dispatch that had not expired: %+v", before)
	}
	if still, err := s.GetCommand(ctx, scope, c.ID, rec.ID); err != nil || still.Status != CommandDispatched {
		t.Fatalf("the live dispatch moved: %+v err=%v", still, err)
	}

	// A heartbeat renews the bound: a healthy long command is never reconciled out from
	// under its holder, which is what makes the budget a liveness proof and not a timeout.
	live, renewed, err := s.HeartbeatCommand(ctx, scope, c.ID, rec.ID, CommandDispatchBudget)
	if err != nil || !renewed {
		t.Fatalf("heartbeat on a live dispatch: live=%v err=%v", renewed, err)
	}
	if !live.DispatchDeadline.After(dispatched.DispatchDeadline.Add(time.Millisecond)) {
		t.Fatalf("the heartbeat did not extend the budget: %s -> %s",
			dispatched.DispatchDeadline.Format(time.RFC3339Nano), live.DispatchDeadline.Format(time.RFC3339Nano))
	}

	expireDispatch(t, s, c.ID, rec.ID)

	// (2) the verdict: no backend was ever invoked, so the ledger says LOST - and says
	// which evidence it had.
	report, err := s.ReconcileStaleDispatches(ctx, 64)
	if err != nil {
		t.Fatal(err)
	}
	closed := closedOf(report, rec.ID)
	if closed == nil {
		t.Fatalf("the expired dispatch was not closed: %+v", report)
	}
	if closed.Status != CommandLost || closed.Reason != TerminalReasonDispatchDeadline || closed.BackendInvoked {
		t.Fatalf("unexpected verdict: %+v", closed)
	}
	row, err := s.GetCommand(ctx, scope, c.ID, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !row.Status.Terminal() {
		t.Fatalf("the reconciled row is not terminal: %q", row.Status)
	}
	if row.TerminalReason != TerminalReasonDispatchDeadline {
		t.Fatalf("the closed row does not say who closed it: %q", row.TerminalReason)
	}

	// (3) no second execution, no invented success.
	if replay, may, err := s.Exec(ctx, scope, c.ID, "h22-lost", "echo h22", ""); err != nil || may || replay.Status != CommandLost {
		t.Fatalf("a closed command was offered for dispatch again: may=%v rec=%+v err=%v", may, replay, err)
	}
	if _, err := s.Restart(ctx, scope, c.ID, rec.ID); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("Restart accepted a closed command: %v", err)
	}
	if _, marked, err := s.MarkBackendStarted(ctx, scope, c.ID, rec.ID); err != nil || marked {
		t.Fatalf("a zombie holder was allowed to reach a backend: marked=%v err=%v", marked, err)
	}
	if late, err := s.CompleteWithOutput(ctx, scope, c.ID, rec.ID, 0, "succeeded!", "", false); err != nil ||
		late.Status != CommandLost || late.HasExit {
		t.Fatalf("a late answer overwrote the verdict: %+v err=%v", late, err)
	}
	if _, liveAgain, err := s.HeartbeatCommand(ctx, scope, c.ID, rec.ID, CommandDispatchBudget); err != nil || liveAgain {
		t.Fatalf("a closed command reported itself live: live=%v err=%v", liveAgain, err)
	}

	// (4) the owner is no longer held hostage: a command the ledger proved never reached
	// a backend cannot still be writing.
	guard, err := s.beginExportGuard(ctx, scope, c.ID)
	if err != nil {
		t.Fatalf("a reconciled LOST still blocks its owner's export: %v", err)
	}
	guard.close()

	// A second sweep is a no-op, not a restamp.
	again, err := s.ReconcileStaleDispatches(ctx, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Closed) != 0 {
		t.Fatalf("a closed command was reconciled twice: %+v", again)
	}
}

// The mirror case: a backend HAD been invoked, so an effect may have landed. The ledger
// closes the row - it must never stay open-ended - but with SIDE_EFFECT_UNKNOWN, and the
// export guard keeps refusing the source. That refusal is the accepted contract for an
// unproven effect, and this test is what stops the fix above from being generalised into
// "reconciliation always unblocks export".
func TestStaleDispatchAfterBackendStartStaysSideEffectUnknownAndHoldsExport(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "reconcile-unknown")
	if err != nil {
		t.Fatal(err)
	}
	rec, _, err := s.Exec(ctx, scope, c.ID, "h22-unknown", "echo h22", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.MarkDispatched(ctx, scope, c.ID, rec.ID); err != nil {
		t.Fatal(err)
	}
	marked, ok, err := s.MarkBackendStarted(ctx, scope, c.ID, rec.ID)
	if err != nil || !ok || marked.BackendStartedAt.IsZero() {
		t.Fatalf("record the backend invocation: ok=%v rec=%+v err=%v", ok, marked, err)
	}
	// Idempotent: the evidence boundary must not drift later on a retry.
	second, _, err := s.MarkBackendStarted(ctx, scope, c.ID, rec.ID)
	if err != nil || !second.BackendStartedAt.Equal(marked.BackendStartedAt) {
		t.Fatalf("the backend-invoked marker moved on retry: %s vs %s err=%v",
			marked.BackendStartedAt, second.BackendStartedAt, err)
	}

	expireDispatch(t, s, c.ID, rec.ID)
	report, err := s.ReconcileStaleDispatches(ctx, 64)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Closed) != 1 || report.Closed[0].Status != CommandSideEffectUnknown ||
		report.Closed[0].Reason != TerminalReasonBackendUnanswered || !report.Closed[0].BackendInvoked {
		t.Fatalf("a dispatch that reached a backend must close as side_effect_unknown: %+v", report)
	}
	if _, err := s.beginExportGuard(ctx, scope, c.ID); contracts.Code(err) != contracts.ErrConflict ||
		contracts.ReasonOf(err) != "export_source_busy" {
		t.Fatalf("an unproven effect was allowed to export: %v", err)
	}
	// And it is still terminal: reconciliation reached a verdict, it just refused to
	// claim the effect did not happen.
	if row, err := s.GetCommand(ctx, scope, c.ID, rec.ID); err != nil || !row.Status.Terminal() {
		t.Fatalf("the row is not terminal: %+v err=%v", row, err)
	}
}
