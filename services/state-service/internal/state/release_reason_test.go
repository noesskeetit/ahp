package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// release_reason_test.go — DEF-RELEASE-REASON-1.
//
// PDF rev3.2 §2.5 makes an ended run owe its reason durably, and DEF-BATCH-1 wrote that
// rule into the batch door: PutBatch cannot end a run without close_reason and/or
// failure_cause. Release never got it. The measurement on candidate aec3fc9d
// (evidence/state-release-reason-measure-20261109T205448Z.log) showed what the asymmetry
// costs: Release(..., "failed") with no reason returned nil and left runs.status=failed
// with no close_reason and no failure_cause in the payload - a terminal run nobody
// explained, through the door every worker uses. The same request through PutBatch got
// INVALID_ARGUMENT.
//
// The director's contract closes it symmetrically for the statuses that owe an
// explanation under this rule - failed and canceled - with one policy sentence on both
// doors. Release of a run to done without a stated reason stays legal by that same
// decision (the model verdict that ended it is recorded by the transcript), so this file
// pins the scope too: a rule nobody wrote down drifts into a surprise later.

const reasonPolicyClause = "may only end a run with a stated reason (close_reason and/or failure_cause)"

// TestReleaseFailedWithoutReasonChangesNothing is the behavior half on a real run row:
// the refused transition must not move the status, the payload, or the outbox.
func TestReleaseFailedWithoutReasonChangesNothing(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool, WithRunCompletionOutbox())
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("release-reason-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	task := "task-" + contracts.NewID()
	run, err := s.CreateRun(ctx, scope, thread.ID, task, "run-"+task,
		json.RawMessage(`{"type":"turn","input":"end me without a reason"}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatal(lease, err)
	}
	claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, Task: task, Root: task, Context: thread.ContextID, Grant: "grant"}
	signed := transport.WithPrincipal(ctx, transport.Principal{Scope: scope, Claims: claims})
	before := runPayload(t, ctx, pool, run.ID)

	// The DEF-RELEASE-REASON-1 shape: terminal, no close_reason, no failure_cause.
	if err := s.Release(signed, scope, run.ID, lease.Worker, lease.Epoch, RunFailed); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("DEF-RELEASE-REASON-1: Release(failed) with no reason returned %v, want %s - a run may not "+
			"reach a terminal state that nobody explained (§2.5, same rule the batch door enforces)", err, contracts.ErrInvalid)
	}
	got, err := s.GetRun(ctx, scope, run.ID)
	if err != nil || got.Status != RunRunning {
		t.Fatalf("the refused transition escaped: run=%+v err=%v", got, err)
	}
	if after := runPayload(t, ctx, pool, run.ID); string(after) != string(before) {
		t.Fatalf("the refused transition touched the payload: before=%s after=%s", before, after)
	}
	if n := completionCountFor(t, pool, run.ID); n != 0 {
		t.Fatalf("a refused transition published %d completions, want 0", n)
	}

	// The same door with an honest reason still ends the run, and the reason lands.
	if err := s.ReleaseWithReasonCause(signed, scope, run.ID, lease.Worker, lease.Epoch,
		RunFailed, "error", "worker lost its backing store"); err != nil {
		t.Fatalf("ReleaseWithReasonCause(failed): %v", err)
	}
	if n := completionCountFor(t, pool, run.ID); n != 1 {
		t.Fatalf("the accepted terminal published %d completions, want 1", n)
	}
	var p struct {
		CloseReason  string `json:"close_reason"`
		FailureCause string `json:"failure_cause"`
	}
	if err := json.Unmarshal(runPayload(t, ctx, pool, run.ID), &p); err != nil {
		t.Fatal(err)
	}
	if p.CloseReason != "error" || p.FailureCause != "worker lost its backing store" {
		t.Fatalf("the accepted terminal recorded close_reason=%q failure_cause=%q", p.CloseReason, p.FailureCause)
	}
	// One policy on both doors: the SAME request shape refused by release must be refused
	// by the batch door with the same policy clause, so a client cannot see two rules for
	// one act. Captured by retrying the refused transition and comparing sentences.
	noReason := s2ReleaseNoReason(t, s, signed, scope, run.ID, lease)
	_, batchErr := s.PutBatch(signed, scope,
		Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		BatchRequest{Key: "k-release-reason-" + contracts.NewID(), RunStatus: &RunStatusOp{Status: RunCanceled}})
	for name, err := range map[string]error{"release": noReason, "batch": batchErr} {
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("%s door accepted an unexplained terminal: %v", name, err)
		}
		if !strings.Contains(err.Error(), reasonPolicyClause) {
			t.Fatalf("%s door refusal %q does not carry the policy clause %q", name, err, reasonPolicyClause)
		}
	}
}

// s2ReleaseNoReason attempts the contract-violating transition again, after the run has
// already been ended legitimately, and returns whatever the door says. Either refusal is
// acceptable to this helper - what the caller checks is that the door still speaks the
// policy, not that it accepts a second terminal.
func s2ReleaseNoReason(t *testing.T, s *Store, signed context.Context, scope contracts.Scope,
	runID string, lease *Lease) error {
	t.Helper()
	return s.Release(signed, scope, runID, lease.Worker, lease.Epoch, RunCanceled)
}

// TestReleaseCanceledWithoutReasonIsRefused covers the second status of the contract and
// the in-tree caller that motivated it: stateapi's claim cleanup cancels a grant it could
// not attach a token to, and now has to say so.
func TestReleaseCanceledWithoutReasonIsRefused(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("release-cancel-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(ctx, scope, thread.ID, "task-"+contracts.NewID(), "k-"+contracts.NewID(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "worker", time.Minute)
	if err != nil || lease == nil {
		t.Fatal(lease, err)
	}
	if lease.Run.ID != run.ID {
		t.Fatalf("claimed %s, want the run just created %s", lease.Run.ID, run.ID)
	}
	if err := s.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, RunCanceled); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("Release(canceled) with no reason returned %v, want %s", err, contracts.ErrInvalid)
	}
	got, err := s.GetRun(ctx, scope, run.ID)
	if err != nil || got.Status != RunRunning {
		t.Fatalf("the refused cancel escaped: run=%+v err=%v", got, err)
	}
	// With a stated reason it stays lawful: this is exactly what claim.go now does.
	if err := s.ReleaseWithReasonCause(ctx, scope, run.ID, lease.Worker, lease.Epoch,
		RunCanceled, "", "stateapi: task grant could not be issued"); err != nil {
		t.Fatalf("ReleaseWithReasonCause(canceled): %v", err)
	}
}

func runPayload(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) json.RawMessage {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM ahp_state.runs WHERE id=$1`, runID).Scan(&raw); err != nil {
		t.Fatalf("read payload of %s: %v", runID, err)
	}
	return json.RawMessage(raw)
}
