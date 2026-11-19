package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// batch_completion_test.go — DEF-BATCH-2.
//
// PDF rev3.2 §2.5 states the purpose of the atomic batch in one line: operations of one
// thread can be packaged - a checkpoint, a transcript append, a run status change -
// "Так outbox событий не расходится с состоянием" (so the event outbox does not diverge
// from state). Release honoured that: closing a run done/failed published the durable
// run-completion event in the SAME transaction. The batch's run_status half did NOT: a
// run a batch ended reached terminal in ahp_state.runs while ahp_state.run_completions
// stayed empty - State terminal, outbox silent, the exact divergence §2.5 forbids. On
// the composed stand the outbox is live (AHP_ENABLE_MEMORY=1 + hindsight memory backend,
// cmd/ahp/state_completion.go), so a batch-terminalized run would never reach completion
// consumers; this is a product defect, not a tidiness preference.
//
// These tests pin the fix's obligations: a batch terminal publishes EXACTLY the
// completion release would (same door, same predicate, same record); the two doors
// together can never produce two events for one run (the completion id IS the run id,
// ON CONFLICT DO NOTHING); and a batch terminal that cannot publish (no verified task
// authority) rolls the terminal back rather than leaving a silent dead run - the
// symmetric half of "не расходится" that release already enforced.

func completionCountFor(t *testing.T, pool *pgxpool.Pool, runID string) int {
	t.Helper()
	// The completion's primary key IS the run id, so "exactly one row" is simultaneously
	// "published at all" and "not double-published". A direct count keyed by run id is
	// used instead of ClaimRunCompletions because that API is thread-ordered across the
	// whole table and the isolated database is shared across the package's tests.
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_state.run_completions WHERE id=$1`, runID).Scan(&n); err != nil {
		t.Fatalf("count completions for %s: %v", runID, err)
	}
	return n
}

// forgetCompletion deletes THIS test's own outbox row when the test ends.
// ClaimRunCompletions claims across the whole table in sequence order, and the isolated
// database is shared by the package's tests, so a pending row left here is handed to the
// next test as if it were its own - it broke completion_test.go's thread-order assertion
// (three pending rows where that test expects one). A test that publishes into a shared
// outbox owes the next test an unchanged outbox.
func forgetCompletion(t *testing.T, pool *pgxpool.Pool, runID string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(),
			`DELETE FROM ahp_state.run_completions WHERE id=$1`, runID); err != nil {
			t.Errorf("clean up the completion row for %s: %v", runID, err)
		}
	})
}

func completionBodyFor(t *testing.T, pool *pgxpool.Pool, runID string) json.RawMessage {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(context.Background(),
		`SELECT body FROM ahp_state.run_completions WHERE id=$1`, runID).Scan(&raw); err != nil {
		t.Fatalf("read completion body for %s: %v", runID, err)
	}
	return json.RawMessage(raw)
}

func TestBatchTerminalPublishesTheCompletionReleaseWould(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool, WithRunCompletionOutbox())
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("completion-batch-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	task := "task-" + contracts.NewID()
	run, err := s.CreateRun(ctx, scope, thread.ID, task, "run-"+task,
		json.RawMessage(`{"type":"turn","input":"end me through a batch"}`))
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
	forgetCompletion(t, pool, run.ID)

	if _, err := s.PutBatch(signed, scope,
		Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		BatchRequest{Key: "k-batch-terminal",
			RunStatus: &RunStatusOp{Status: RunFailed, CloseReason: "error", FailureCause: "injected batch terminal"}}); err != nil {
		t.Fatalf("PutBatch terminal: %v", err)
	}
	if n := completionCountFor(t, pool, run.ID); n != 1 {
		t.Fatalf("DEF-BATCH-2 / PDF §2.5: the batch ended run %s terminal (failed) but published %d "+
			"completion events, want exactly 1 - a batch that flips a run terminal while the outbox "+
			"stays silent is the divergence §2.5 forbids (release publishes; the batch must too)", run.ID, n)
	}
	// It is the SAME record release would have written: this run, terminal, in the body.
	var body struct {
		Run struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"run"`
	}
	if err := json.Unmarshal(completionBodyFor(t, pool, run.ID), &body); err != nil {
		t.Fatalf("decode completion body: %v", err)
	}
	if body.Run.ID != run.ID || body.Run.Status != RunFailed {
		t.Fatalf("completion carries run %+v, want this run terminal %s", body.Run, RunFailed)
	}
}

func TestBatchAndReleaseTogetherCannotDoublePublish(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool, WithRunCompletionOutbox())
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("completion-dual-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	task := "task-" + contracts.NewID()
	run, err := s.CreateRun(ctx, scope, thread.ID, task, "run-"+task,
		json.RawMessage(`{"type":"turn","input":"close me twice"}`))
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
	forgetCompletion(t, pool, run.ID)

	// First door: a batch terminal publishes once.
	if _, err := s.PutBatch(signed, scope,
		Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		BatchRequest{Key: "k-dual", RunStatus: &RunStatusOp{Status: RunDone, CloseReason: "model_finished"}}); err != nil {
		t.Fatalf("PutBatch done: %v", err)
	}
	if n := completionCountFor(t, pool, run.ID); n != 1 {
		t.Fatalf("batch terminal published %d completions, want 1", n)
	}

	// Second door, driven directly in one transaction: calling the shared publish door
	// again for the SAME run must not add a row - the completion id IS the run id.
	if err := s.inTx(signed, func(tx pgx.Tx) error {
		if err := s.publishRunCompletionIfTerminal(signed, tx, scope, run.ID, RunDone); err != nil {
			return err
		}
		return s.publishRunCompletionIfTerminal(signed, tx, scope, run.ID, RunDone)
	}); err != nil {
		t.Fatalf("re-publish through the door: %v", err)
	}
	if n := completionCountFor(t, pool, run.ID); n != 1 {
		t.Fatalf("a second publish produced %d completions for one run, want exactly 1 - the two doors "+
			"share one idempotent insert, so a run can never double-publish", n)
	}

	// A client that retries the same batch after the terminal is refused by the lease fence
	// (the terminal cleared the lease), and the outbox still holds exactly one row. Both
	// halves belong to §2.5: a retry cannot manufacture a second event, and the shared
	// insert would not add one even if a fence let the retry through - that is what the
	// direct double call through the door above proves.
	if _, err := s.PutBatch(signed, scope,
		Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		BatchRequest{Key: "k-dual", RunStatus: &RunStatusOp{Status: RunDone, CloseReason: "model_finished"}}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("a batch replayed after its terminal returned %v, want %s - the terminal cleared the "+
			"lease that the fence validates", err, contracts.ErrStaleEpoch)
	}
	if n := completionCountFor(t, pool, run.ID); n != 1 {
		t.Fatalf("a replayed batch terminal produced %d completions for one run, want exactly 1", n)
	}
}

func TestBatchTerminalWithoutAuthorityRollsBackAndPublishesNothing(t *testing.T) {
	pool, _ := isolatedPool(t)
	s := New(pool, WithRunCompletionOutbox())
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := eventScope("completion-unsigned-" + contracts.NewID())
	thread, err := s.CreateThread(ctx, scope, "conversation", "thread")
	if err != nil {
		t.Fatal(err)
	}
	task := "task-" + contracts.NewID()
	run, err := s.CreateRun(ctx, scope, thread.ID, task, "run-"+task,
		json.RawMessage(`{"type":"turn","input":"end me unsigned"}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatal(lease, err)
	}

	// No verified task authority in ctx: the batch cannot publish the completion, so it
	// must refuse the whole terminal and roll the run back - never a silent terminal.
	// This is release's existing guarantee (completion_test.go), now shared by the batch.
	if _, err := s.PutBatch(ctx, scope,
		Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
		BatchRequest{Key: "k-unsigned",
			RunStatus: &RunStatusOp{Status: RunFailed, CloseReason: "error", FailureCause: "unsigned"}}); err == nil {
		t.Fatal("an unsigned batch terminal was accepted under the outbox; §2.5 makes the terminal and its " +
			"event one atomic truth, so it must be refused instead of diverging")
	}
	got, err := s.GetRun(ctx, scope, run.ID)
	if err != nil || got.Status != RunRunning {
		t.Fatalf("the refused batch escaped rollback: run=%+v err=%v", got, err)
	}
	if n := completionCountFor(t, pool, run.ID); n != 0 {
		t.Fatalf("a refused batch published %d completions, want 0", n)
	}
}
