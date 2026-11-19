package state

// Crash/recovery coverage for the durability claim: a State mutation is ONE
// PostgreSQL transaction, so a process that dies in the middle of one must
// leave neither half of it behind. The crash is made deterministic instead of
// raced: a blocker transaction holds the thread row, so the victim's batch is
// provably still blocked (and therefore un-committed) at the moment the
// PostgreSQL backend is killed underneath it.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// laneConfig resolves this run's database config (isolation_test.go). It fails
// rather than skipping: this lane asserts that it ran live. The DSN never
// appears in an error message because pgx error text embeds it.
func laneConfig(t *testing.T) *pgxpool.Config {
	t.Helper()
	return isolatedConfig(t)
}

// TestCrashMidTransactionLeavesNoPartialState kills the database backend of a
// PutBatch that is blocked before its commit, then proves the store recovers:
// nothing was written, the lease and its epoch are untouched, and the same
// batch succeeds once a live connection retries it.
func TestCrashMidTransactionLeavesNoPartialState(t *testing.T) {
	e := cpFixture(t)
	laneCfg := laneConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	blocker, err := pgx.ConnectConfig(ctx, laneCfg.ConnConfig.Copy())
	if err != nil {
		// A pgx connection error can quote the connection string, so the
		// credential-carrying text is never printed - only the fact of failure.
		t.Fatal("blocker could not connect to the lane database (DSN not printed)")
	}
	defer func() { _ = blocker.Close(ctx) }()
	blockTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatalf("blocker begin: %v", err)
	}
	// Hold the exact row withLease locks first: the victim cannot reach its
	// commit while this is open.
	if _, err := blockTx.Exec(ctx,
		`SELECT 1 FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, e.threadID); err != nil {
		t.Fatalf("blocker lock: %v", err)
	}

	// Same database as the blocker above and as cpFixture's store: a split
	// across two databases would block nothing and measure nothing.
	victimCfg := laneCfg.Copy()
	// One connection: the pid observed below is the pid the batch will run on.
	victimCfg.MaxConns = 1
	victimPool, err := pgxpool.NewWithConfig(ctx, victimCfg)
	if err != nil {
		t.Fatal("victim pool could not be created (DSN not printed)")
	}
	defer victimPool.Close()
	var victimPid int64
	if err := victimPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&victimPid); err != nil {
		t.Fatalf("victim pid: %v", err)
	}
	victim := New(victimPool)

	batchKey := "batch-crash-" + contracts.NewID()
	streamKey := "evt-crash-" + contracts.NewID()
	// The carried status transition is deliberately terminal: RunDone releases
	// the lease, so if the crashed batch had been partially applied the fence
	// check below could not pass. ClaimRun has already moved this run to
	// running, which is the status it must still hold after the crash.
	request := BatchRequest{
		Checkpoint: &Checkpoint{ID: "cp-crash", Namespace: "ns", Version: 1,
			Parts: []Part{{Channel: "values", Version: "v1", Data: []byte("crash-proof")}}},
		Key:    batchKey,
		Stream: &StreamOp{Key: streamKey, Kind: "token", Payload: json.RawMessage(`{"i":1}`)},
		// DEF-BATCH-1: a terminal batch status owes a reason, so this batch states
		// one exactly like a real ending caller would.
		RunStatus: &RunStatusOp{Status: RunDone, CloseReason: "model_finished"},
	}

	errc := make(chan error, 1)
	go func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, err := victim.PutBatch(cctx, e.scope, e.fence, request)
		errc <- err
	}()

	// Wait until the victim is provably waiting on the blocker's lock.
	blocked := false
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !blocked {
		var waiting bool
		if err := blocker.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			  WHERE pid = $1 AND wait_event_type = 'Lock')`, victimPid).Scan(&waiting); err != nil {
			t.Fatalf("lock wait probe: %v", err)
		}
		blocked = waiting
		if !blocked {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !blocked {
		t.Fatalf("the batch never blocked on the lease row, so the crash point is not proven")
	}

	// Crash: PostgreSQL itself drops the backend mid-transaction.
	var killed bool
	if err := blocker.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, victimPid).Scan(&killed); err != nil {
		t.Fatalf("terminate_backend: %v", err)
	}
	if !killed {
		t.Fatalf("pg_terminate_backend did not kill the victim backend")
	}
	select {
	case err := <-errc:
		if err == nil {
			t.Fatalf("a killed backend must fail the batch")
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("the killed batch never returned")
	}

	// Nothing half-applied: no snapshot, no stream event, no status drift.
	parts, err := e.s.ListCheckpoints(ctx, e.scope, e.threadID, "ns", 10)
	if err != nil {
		t.Fatalf("ListCheckpoints after the crash: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("the crashed batch left %d snapshot rows behind", len(parts))
	}
	events, err := e.s.ReadStream(ctx, e.scope, e.threadID, 0, 10)
	if err != nil {
		t.Fatalf("ReadStream after the crash: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("the crashed batch left %d stream events behind", len(events))
	}
	run, err := e.s.GetRun(ctx, e.scope, e.fence.RunID)
	if err != nil {
		t.Fatalf("GetRun after the crash: %v", err)
	}
	if run.Status != RunRunning {
		t.Fatalf("the crashed batch moved the run to %s, want %s", run.Status, RunRunning)
	}
	// The reads above ran WHILE the blocker still held the thread row, so the
	// rollback is proven immediate rather than eventual. Now release the
	// blocker: the lease checks below need the row themselves.
	if err := blockTx.Rollback(ctx); err != nil {
		t.Fatalf("blocker rollback: %v", err)
	}
	// The fence survived: a crash must not consume or bump an epoch, and the
	// aborted batch's terminal status must not have released the lease either.
	if _, err := e.s.Heartbeat(ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		time.Minute); err != nil {
		t.Fatalf("the lease must still be valid after the crash: %v", err)
	}
	// Recovery: the same batch commits cleanly on a live connection.
	res, err := e.s.PutBatch(ctx, e.scope, e.fence, request)
	if err != nil {
		t.Fatalf("the retry after recovery must succeed: %v", err)
	}
	if !res.OK || res.Checkpoint == nil || res.StreamEvent == nil {
		t.Fatalf("the recovered batch result is incomplete: %+v", res)
	}
	parts, err = e.s.ListCheckpoints(ctx, e.scope, e.threadID, "ns", 10)
	if err != nil || len(parts) != 1 {
		t.Fatalf("the recovered batch is not durable: %d snapshots, err=%v", len(parts), err)
	}
	if run, err = e.s.GetRun(ctx, e.scope, e.fence.RunID); err != nil || run.Status != RunDone {
		t.Fatalf("the recovered batch did not apply its status transition: %s, err=%v", run.Status, err)
	}
}
