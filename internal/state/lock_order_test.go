package state

// Lock-order regressions for the lease cycle.
//
// THE DEFECT THESE PINS. A claim locked the head RUN row (FOR UPDATE OF r) and
// then the THREAD row; every lease writer - release, heartbeat, suspend, batch -
// locks the THREAD row and only then the RUN row. Two workers on one thread were
// therefore an A->B / B->A cycle, and PostgreSQL resolved it by aborting one side
// (SQLSTATE 40P01). The claim loop hid its own abort behind a replay, so the abort
// that reached a client was the release side.
//
// WHY THE FIRST TEST IS A TEXT ASSERTION. The locking clause is where the order
// lives, so pinning it textually is the precise regression guard: it goes red the
// moment a run row is written back into the scan. It does not stand alone - the
// two behavioural tests below show what the clause prevents and that skipping a
// busy thread loses no claim.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// lockingClause returns the FOR UPDATE clause of a query, for readable failures.
func lockingClause(sql string) string {
	i := strings.Index(sql, "FOR UPDATE")
	if i < 0 {
		return "<no FOR UPDATE>"
	}
	end := i + len("FOR UPDATE")
	if j := strings.Index(sql[i:], "\n"); j > 0 && i+j < len(sql) {
		end = i + j
	}
	return strings.TrimSpace(sql[i:end])
}

// TestClaimCandidatesSQLLocksTheThread pins the lock target of the candidate
// scan: the thread, never the head run, and always SKIP LOCKED.
func TestClaimCandidatesSQLLocksTheThread(t *testing.T) {
	clause := lockingClause(claimCandidatesSQL)
	if strings.Contains(claimCandidatesSQL, "FOR UPDATE OF r") {
		t.Fatalf("the candidate scan locks the run row (%s); every lease writer locks "+
			"the thread first, and the two orders together are the cycle", clause)
	}
	if clause != "FOR UPDATE OF t SKIP LOCKED" {
		t.Fatalf("the candidate scan must lock the thread with SKIP LOCKED, so a busy "+
			"thread is skipped instead of waited on while another row is held; got %q", clause)
	}
}

// TestClaimAndReleaseDoNotFormALockCycle is the two-session reproduction, made
// deterministic instead of raced.
//
// Session B holds the thread row exactly as currentLease does. Session A then runs
// the product's own candidate SQL and, if that SQL handed it a candidate, asks for
// the thread row the way grant does. Only once A is provably waiting on that lock -
// observed in pg_stat_activity from B, never slept for - does B write the run row,
// which is what applyRunStatusJournal does.
//
// One order means B's write is granted and nothing is aborted. Two orders mean
// PostgreSQL reports the cycle, and this test fails naming the SQLSTATE it saw.
//
// WHAT IT DOES NOT PROVE, SAID OUT LOUD. The claim side is the product's own
// claimCandidatesSQL; the release side is a hand-written UPDATE ahp_state.runs
// shaped like applyRunStatusJournal, because nothing outside a transaction can
// interleave with statements inside one. So this test proves that the two DOORS
// form a cycle when the scan locks a run row; it does not prove that
// Store.Release itself touches the thread before the run - a regression inside
// the body of release, outside currentLease, is not caught here. The product
// door for that claim is TestLeaseWritersLockTheThreadBeforeTheRun, which
// observes the statement order a real Heartbeat/Suspend/Release actually sends;
// TestClaimAndReleaseReportsNoDatabaseAbort adds breadth over both doors at once.
// What remains unproven after all three: an interleaving forced *inside* a
// product transaction, which would need a test seam in release/suspend - not
// added, because a seam in the hot lease path is a change of its own.
func TestClaimAndReleaseDoNotFormALockCycle(t *testing.T) {
	e := cpFixture(t)
	laneCfg := laneConfig(t)
	ctx := e.ctx

	// A thread of its own with a pending run: no lease at all, so it is a
	// legitimate claim candidate for the whole test.
	scope := contracts.Scope{
		Tenant:     "t-abba-" + contracts.NewID(),
		Deployment: "d-abba-" + contracts.NewID(),
		Subject:    "subj-abba-" + contracts.NewID(),
	}
	thread, err := e.s.CreateThread(ctx, scope, "ctx-abba-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.s.CreateRun(ctx, scope, thread.ID, "task-abba", "k-"+contracts.NewID(), json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	connB, err := pgx.ConnectConfig(ctx, laneCfg.ConnConfig.Copy())
	if err != nil {
		t.Fatal("session B could not connect to the lane database (DSN not printed)")
	}
	defer func() { _ = connB.Close(ctx) }()
	tx, err := connB.Begin(ctx)
	if err != nil {
		t.Fatalf("session B begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT 1 FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, thread.ID); err != nil {
		t.Fatalf("session B thread lock: %v", err)
	}

	// The wait is observed from a third backend. Asking the transaction that holds
	// the lock whether anyone is queued behind it is the mistake this helper exists
	// to prevent - see lockWaitObserver.
	observer := lockWaitObserver(t, ctx, laneCfg)

	connA, err := pgx.ConnectConfig(ctx, laneCfg.ConnConfig.Copy())
	if err != nil {
		t.Fatal("session A could not connect to the lane database (DSN not printed)")
	}
	defer func() { _ = connA.Close(ctx) }()
	claimTx, err := connA.Begin(ctx)
	if err != nil {
		t.Fatalf("session A begin: %v", err)
	}
	defer func() { _ = claimTx.Rollback(ctx) }()
	// The backend pid travels as int64: the wire protocol hands it out unsigned
	// (pgconn.PgConn.PID is uint32) while pg_stat_activity.pid is a SIGNED
	// 32-bit integer, because the server stores it in an int. Widening to int64
	// is the only lossless step - narrowing to int32 would silently wrap a pid
	// above MaxInt32 into a negative number that matches no backend, and the
	// ABBA leg would then "never see the wait" for a reason that has nothing to
	// do with the code under test. probeLockWait refuses such a pid outright.
	aPID := int64(connA.PgConn().PID())

	// The product's own scan, with the product's own locking clause.
	rows, err := claimTx.Query(ctx, claimCandidatesSQL, scope.Tenant, scope.Deployment, "", "")
	if err != nil {
		t.Fatalf("candidate scan: %v", err)
	}
	var sawThread, sawRun string
	for rows.Next() {
		var c claimCandidate
		if err := rows.Scan(&c.scope.Tenant, &c.scope.Deployment, &c.scope.Subject, &c.threadID, &c.runID); err != nil {
			rows.Close()
			t.Fatalf("candidate scan: %v", err)
		}
		sawThread, sawRun = c.threadID, c.runID
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("candidate scan: %v", err)
	}

	// B holds the thread row right now. A scan that respects the lock order skips
	// it and offers nothing. A scan that locks the run row first offers this thread
	// anyway - and then queues behind B for the thread while holding that run.
	claimDone := make(chan error, 1)
	if sawThread != "" {
		if sawThread != thread.ID || sawRun != run.ID {
			t.Fatalf("the candidate scan offered %s/%s, want %s/%s", sawThread, sawRun, thread.ID, run.ID)
		}
		go func() {
			var ignored string
			claimDone <- connA.QueryRow(ctx,
				`SELECT id FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, thread.ID).Scan(&ignored)
		}()
		if err := waitForLockWait(ctx, observer, aPID); err != nil {
			t.Fatalf("the observer backend never saw session A queued on the thread lock, so this run "+
				"would prove nothing: %v", err)
		}
	}

	// B now writes the run row: release's move.
	_, releaseErr := tx.Exec(ctx,
		`UPDATE ahp_state.runs SET updated_at = clock_timestamp() WHERE id = $1`, run.ID)

	// End B first - whichever side PostgreSQL chose, the other must not be left
	// waiting on a lock this test is holding - then collect A's outcome.
	_ = tx.Rollback(ctx)
	var claimErr error
	if sawThread != "" {
		select {
		case claimErr = <-claimDone:
		case <-time.After(10 * time.Second):
			t.Fatal("session A never came back from the thread lock")
		}
	}
	if releaseErr != nil || claimErr != nil {
		t.Fatalf("lock cycle between the two doors: claim side = %s; release side = %s",
			describeAbort(claimErr), describeAbort(releaseErr))
	}
	if sawThread != "" {
		t.Fatalf("the scan offered a thread whose row another writer holds (%s); it must skip it",
			lockingClause(claimCandidatesSQL))
	}

	// Skipping is not losing: the moment B lets go, the same run is claimable
	// through the product door.
	lease, err := e.s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w-after-release", time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun after the other writer let go: %v", err)
	}
	if lease == nil || lease.Run.ID != run.ID {
		t.Fatalf("ClaimRun after the lock was let go: got %+v, want run %s", lease, run.ID)
	}
}

// lockWaitReader is the narrow surface the wait probe needs, so the observation can
// be made through whichever connection is valid for it: pgx.Tx and *pgx.Conn both
// satisfy it.
type lockWaitReader interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// lockWaitObserver opens the connection the wait probe reads through: a third
// backend, neither the lock holder nor the blocked waiter.
//
// THIS IS NOT A STYLE CHOICE, AND IT WAS LEARNED THE HARD WAY. The probe used to
// read pg_stat_activity through the transaction that holds the lock. On the
// acceptance server (PostgreSQL 17.11, measured 2026-11-10) that read does not
// return the blocked backend at all: the view is served from the reading
// transaction's snapshot, and the waiting backend joined after that snapshot was
// taken, so the probe answered "not waiting" while the waiter sat blocked on the
// row for the whole polling window. Measured side by side, same instant, same
// query text: through the holder's open transaction - 2 backends, waiter absent;
// through a third connection - 3 backends, waiter present with wait_event_type =
// 'Lock' and wait_event = 'transactionid'. Read through any connection that is not
// a party to the lock, and the observation is reliable.
//
// The consequence for the ABBA leg is worth stating plainly: with the old plumbing,
// "session A never reached the thread-lock wait" was an instrumentation failure
// waiting to be mistaken for a code verdict. The failure message now says which
// connection was asked.
func lockWaitObserver(t *testing.T, ctx context.Context, cfg *pgxpool.Config) *pgx.Conn {
	t.Helper()
	conn, err := pgx.ConnectConfig(ctx, cfg.ConnConfig.Copy())
	if err != nil {
		t.Fatal("the lock-wait observer could not connect to the lane database (DSN not printed)")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(ctx)
	})
	var self int
	if err := conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&self); err != nil {
		t.Fatalf("the lock-wait observer cannot name itself: %v", err)
	}
	t.Logf("lock-wait observer backend pid=%d", self)
	return conn
}

// probeLockWait asks PostgreSQL, once, whether the backend named by pid is
// blocked on a ROW lock held by another transaction. It is read through a
// connection that is not the blocked one, because a connection cannot report on
// a statement it is still running.
//
// The predicate is deliberately narrow. An observation that matches anything
// proves nothing, and this one is what makes the ABBA leg real rather than
// decorative:
//   - wait_event_type = 'Lock' excludes IO, timeout and client waits;
//   - wait_event IN ('transactionid','tuple') is the row-level signature, the
//     waiter standing behind another transaction's lock on a row. An advisory
//     lock reports 'object' and a table-level lock reports 'relation', so
//     neither can satisfy this predicate and let the test conclude on luck;
//   - the running statement must be a locking read, so a backend that merely
//     holds a lock while idle in transaction is never mistaken for a waiter.
//
// pid is int64 because pgconn hands the backend pid out unsigned while
// pg_stat_activity.pid is a SIGNED 32-bit integer (the server keeps it in an
// int). A value outside that range cannot name a backend, so it is reported as
// the instrumentation fault it is instead of being wrapped into a number that
// matches nothing.
func probeLockWait(ctx context.Context, from lockWaitReader, pid int64) (bool, error) {
	if pid < 0 || pid > math.MaxInt32 {
		return false, fmt.Errorf("backend pid %d is outside pg_stat_activity.pid (signed 32-bit): the wait could not be observed even in principle", pid)
	}
	var waiting bool
	if err := from.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			 WHERE pid = $1
			   AND wait_event_type = 'Lock'
			   AND wait_event IN ('transactionid', 'tuple')
			   AND query LIKE '%FOR UPDATE%')`,
		int32(pid)).Scan(&waiting); err != nil {
		return false, err
	}
	return waiting, nil
}

// waitForLockWait polls probeLockWait until the block is observed or the deadline
// passes. The test never sleeps "for" the wait: the database is the witness.
func waitForLockWait(ctx context.Context, from lockWaitReader, pid int64) error {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		waiting, err := probeLockWait(ctx, from, pid)
		if err != nil {
			return err
		}
		if waiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return context.DeadlineExceeded
}

// describeAbort names the SQLSTATE without quoting driver text.
func describeAbort(err error) string {
	if err == nil {
		return "no error"
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return "SQLSTATE " + pe.Code
	}
	return "error without a SQLSTATE (code " + contracts.Code(err) + ")"
}

// TestClaimAndReleaseReportsNoDatabaseAbort soaks the shape that produced the
// aborts in the field: a lease that lapses while its holder is still working, and a
// second worker claiming the whole time. The claim loop hides its own aborts, so
// what is asserted is the part a client actually sees - the release must never
// report a database abort, and the release replay budget must never be spent.
//
// This is a soak, not the RED evidence. Whether a release overlaps a claim at all
// depends on where the lease expiry falls inside the release; the two-session test
// above forces that overlap on purpose. What this test earns is breadth: on
// field-shaped churn no abort may reach either door. Release and suspend are
// exercised against a real database by the rest of this package in any case.
//
// One thread per round, on purpose: a run the poller happened to take stays
// unfinished, and a thread's unfinished head run blocks later runs of that thread by
// design. Sharing one thread across rounds would test that FIFO rule, not this one.
func TestClaimAndReleaseReportsNoDatabaseAbort(t *testing.T) {
	e := cpFixture(t)
	ctx := e.ctx
	scope := contracts.Scope{
		Tenant:     "t-cycle-" + contracts.NewID(),
		Deployment: "d-cycle-" + contracts.NewID(),
		Subject:    "subj-cycle-" + contracts.NewID(),
	}

	const rounds = 40
	errCh := make(chan error, 1)
	stop := make(chan struct{})
	defer close(stop)

	// The poller claims with a lease short enough that it never holds anything it
	// is not about to lose: a minute-long poll lease would park a thread for the
	// rest of the test and turn this into a test of the FIFO rule.
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := e.s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w-poller", 5*time.Millisecond); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	for round := 0; round < rounds; round++ {
		thread, err := e.s.CreateThread(ctx, scope, "ctx-cycle-"+contracts.NewID(), "k-"+contracts.NewID())
		if err != nil {
			t.Fatalf("CreateThread round %d: %v", round, err)
		}
		run, err := e.s.CreateRun(ctx, scope, thread.ID, "task-cycle", "k-"+contracts.NewID(), json.RawMessage(`{"round":1}`))
		if err != nil {
			t.Fatalf("CreateRun round %d: %v", round, err)
		}
		// A lease short enough to lapse before its holder finishes, which is what
		// puts a fresh claim and an old release on one thread at the same moment.
		// The poller may win this race; it loses the lease just as fast.
		var lease *Lease
		for try := 0; try < 200 && lease == nil; try++ {
			lease, err = e.s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "w-holder", run.ID, 5*time.Millisecond)
			if err != nil {
				t.Fatalf("ClaimRunFor round %d: %v", round, err)
			}
			if lease == nil {
				time.Sleep(time.Millisecond)
			}
		}
		if lease == nil {
			t.Fatalf("ClaimRunFor round %d: the run this test named was never offered", round)
		}
		time.Sleep(15 * time.Millisecond) // the lease expires in database time
		if err := e.s.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, RunDone); err != nil {
			// A lapsed lease legitimately costs its holder the fence: STALE_EPOCH is
			// a decision. A database abort is not a decision, and no client can act
			// on it except by replaying - which is what this test refuses to accept.
			if isTransientAbort(err) {
				t.Fatalf("round %d: Release reported a database abort: %s", round, describeAbort(err))
			}
			if code := contracts.Code(err); code != contracts.ErrStaleEpoch {
				t.Fatalf("round %d: Release: unexpected %s: %v", round, code, err)
			}
		}
	}
	// Give the poller a moment to report anything it was mid-way through.
	select {
	case err := <-errCh:
		if isTransientAbort(err) {
			t.Fatalf("ClaimRun reported a database abort: %s", describeAbort(err))
		}
	case <-time.After(200 * time.Millisecond):
	}
	if counts := e.s.LeaseRetryCounts(); counts["release"].GiveUps != 0 {
		t.Fatalf("release ran out of replay budget %d time(s): the aborts are still there, only bounded",
			counts["release"].GiveUps)
	}
}

// TestWithTransientAbortRetryIsBoundedAndCounted proves the replay loop without a
// database: it replays only the two abort states, it stops at its bound, it counts
// both the replays and the give-up, and it leaves every honest rejection exactly as
// it was found.
func TestWithTransientAbortRetryIsBoundedAndCounted(t *testing.T) {
	restore := leaseAbortSleep
	leaseAbortSleep = func(context.Context, time.Duration) bool { return true }
	t.Cleanup(func() { leaseAbortSleep = restore })

	abort := func(code string) error {
		return classifyStoreError(&pgconn.PgError{
			Code: code, Message: "deadlock detected on a name nobody may echo"})
	}

	t.Run("replays then succeeds", func(t *testing.T) {
		s := New(nil)
		const failures = 2
		calls := 0
		err := s.withTransientAbortRetry(context.Background(), "release", &s.leaseRetries.release, func() error {
			calls++
			if calls <= failures {
				return abort("40P01")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("a replay that eventually succeeded must succeed, got %v", err)
		}
		if calls != failures+1 {
			t.Fatalf("want %d attempts, got %d", failures+1, calls)
		}
		if counts := s.LeaseRetryCounts()["release"]; counts.Replays != failures || counts.GiveUps != 0 {
			t.Fatalf("want %d replays and no give-up, got %+v", failures, counts)
		}
	})

	t.Run("bounded then reported", func(t *testing.T) {
		s := New(nil)
		calls := 0
		err := s.withTransientAbortRetry(context.Background(), "suspend", &s.leaseRetries.suspend, func() error {
			calls++
			return abort("40001")
		})
		if !isTransientAbort(err) {
			t.Fatalf("the abort must be reported, not swallowed, got %v", err)
		}
		if code := contracts.Code(err); code != contracts.ErrUnavailable {
			t.Fatalf("a database abort is availability, got %s", code)
		}
		if !strings.Contains(err.Error(), "SQLSTATE 40001") {
			t.Fatalf("the SQLSTATE must survive to the caller so a triage can tell the two aborts apart, got %q", err.Error())
		}
		if calls != maxLeaseAbortAttempts {
			t.Fatalf("want exactly %d attempts, got %d", maxLeaseAbortAttempts, calls)
		}
		if counts := s.LeaseRetryCounts()["suspend"]; counts.Replays != maxLeaseAbortAttempts-1 || counts.GiveUps != 1 {
			t.Fatalf("want %d replays and one give-up, got %+v", maxLeaseAbortAttempts-1, counts)
		}
	})

	t.Run("honest rejection is not replayed", func(t *testing.T) {
		s := New(nil)
		rejected := contracts.NewError(contracts.ErrConflict, "state: a uniqueness rule rejected the write")
		calls := 0
		err := s.withTransientAbortRetry(context.Background(), "release", &s.leaseRetries.release, func() error {
			calls++
			return rejected
		})
		if !errors.Is(err, rejected) || calls != 1 {
			t.Fatalf("a rejection the caller must act on was replayed: calls=%d err=%v", calls, err)
		}
		if counts := s.LeaseRetryCounts()["release"]; counts.Replays != 0 || counts.GiveUps != 0 {
			t.Fatalf("no replay was earned, got %+v", counts)
		}
	})

	t.Run("a cancelled caller stops replaying", func(t *testing.T) {
		s := New(nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		leaseAbortSleep = func(context.Context, time.Duration) bool { cancel(); return false }
		calls := 0
		err := s.withTransientAbortRetry(ctx, "release", &s.leaseRetries.release, func() error {
			calls++
			return abort("40P01")
		})
		if !isTransientAbort(err) {
			t.Fatalf("the pending abort must still be reported when the caller goes away, got %v", err)
		}
		if calls != 1 {
			t.Fatalf("a cancelled caller must not keep replaying, got %d attempts", calls)
		}
	})
}

// TestLockWaitProbeSeesOnlyTheRealWaiter tests the INSTRUMENT, not the product.
//
// WHY IT EXISTS. The ABBA test above concludes "A is queued behind B" from one
// probe, and it only runs that probe on the regressed order - on the fixed order
// the scan skips the busy thread and the wait is never reached. A probe that
// answered "waiting" to anything would therefore stay green forever while making
// the ABBA leg decorative. This test pins the probe on a shape it can force
// through the product's own door: a real row lock, a real second backend, real
// pg_stat_activity.
//
// Three claims, all measured:
//   - a backend genuinely queued on the row IS reported, and its statement ends
//     exactly when the holder lets the row go (so it was queued on THIS row, not
//     on something unrelated);
//   - a backend that holds a lock while idle in transaction is NOT reported;
//   - a pid that pg_stat_activity cannot represent is an instrumentation fault
//     and is reported as an error, not silently answered "not waiting".
func TestLockWaitProbeSeesOnlyTheRealWaiter(t *testing.T) {
	e := cpFixture(t)
	laneCfg := laneConfig(t)
	ctx := e.ctx

	scope := contracts.Scope{
		Tenant:     "t-probe-" + contracts.NewID(),
		Deployment: "d-probe-" + contracts.NewID(),
		Subject:    "subj-probe-" + contracts.NewID(),
	}
	thread, err := e.s.CreateThread(ctx, scope, "ctx-probe-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}

	// The holder: an open transaction that locked the thread row and then does
	// nothing - the state a lease writer is in between its two statements.
	holder, err := pgx.ConnectConfig(ctx, laneCfg.ConnConfig.Copy())
	if err != nil {
		t.Fatal("holder could not connect to the lane database (DSN not printed)")
	}
	defer func() { _ = holder.Close(ctx) }()
	holderTx, err := holder.Begin(ctx)
	if err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	defer func() { _ = holderTx.Rollback(ctx) }()
	if _, err := holderTx.Exec(ctx,
		`SELECT 1 FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, thread.ID); err != nil {
		t.Fatalf("holder thread lock: %v", err)
	}
	// Every read below goes through a third backend. The holder's own transaction
	// cannot answer this question: its snapshot predates the waiter, and
	// pg_stat_activity served from it omits the waiting backend entirely (measured
	// on PostgreSQL 17.11 - see lockWaitObserver).
	observer := lockWaitObserver(t, ctx, laneCfg)

	if waiting, err := probeLockWait(ctx, observer, int64(holder.PgConn().PID())); err != nil || waiting {
		t.Fatalf("an idle lock holder was reported as a waiter: waiting=%v err=%v", waiting, err)
	}

	// The waiter: the same product locking read on the same row, from a second
	// backend. It cannot return while the holder is open.
	waiter, err := pgx.ConnectConfig(ctx, laneCfg.ConnConfig.Copy())
	if err != nil {
		t.Fatal("waiter could not connect to the lane database (DSN not printed)")
	}
	defer func() { _ = waiter.Close(ctx) }()
	waitDone := make(chan error, 1)
	go func() {
		var ignored int
		waitDone <- waiter.QueryRow(ctx,
			`SELECT 1 FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, thread.ID).Scan(&ignored)
	}()
	if err := waitForLockWait(ctx, observer, int64(waiter.PgConn().PID())); err != nil {
		// Release the row before failing: closing a backend that is still queued on
		// a lock waits for a statement that will never finish, and the whole package
		// would hang in teardown instead of reporting.
		_ = holderTx.Rollback(ctx)
		t.Fatalf("a backend genuinely queued on the row was not observed by the observer: %v", err)
	}

	// What the observer saw, for the receipt. Informational on purpose: the point of
	// this test is that the observer CAN see it, not that PostgreSQL keeps answering
	// the same way from a party to the lock.
	var seenWet, seenWe string
	if err := observer.QueryRow(ctx, `SELECT coalesce(wait_event_type,'<null>'), coalesce(wait_event,'<null>')
		FROM pg_stat_activity WHERE pid = $1`, int32(waiter.PgConn().PID())).Scan(&seenWet, &seenWe); err != nil {
		t.Fatalf("the observer could not read the waiting backend's state: %v", err)
	}
	t.Logf("waiter observed as wait_event_type=%s wait_event=%s", seenWet, seenWe)

	// Letting the row go must be what releases it - the causal half of the claim.
	if err := holderTx.Rollback(ctx); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("the queued locking read failed instead of completing: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the queued locking read never completed after the holder let go")
	}

	// A pid outside pg_stat_activity's signed 32-bit range cannot name a backend.
	// Answering "not waiting" would let the ABBA leg conclude from nothing.
	if _, err := probeLockWait(ctx, observer, int64(math.MaxInt32)+1); err == nil {
		t.Fatal("an unrepresentable pid must be reported as an instrumentation fault")
	}
}

// statementRecorder is a pgx QueryTracer that keeps the SQL text of everything
// run while it is switched on. Only the statement text is kept, never arguments,
// so no run payload, token or credential can reach a failure message; the SQL of
// this package is static text with $n placeholders.
type statementRecorder struct {
	mu        sync.Mutex
	recording bool
	sqls      []string
}

func (r *statementRecorder) start() {
	r.mu.Lock()
	r.recording, r.sqls = true, nil
	r.mu.Unlock()
}

func (r *statementRecorder) stop() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recording = false
	return append([]string(nil), r.sqls...)
}

func (r *statementRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	if r.recording {
		r.sqls = append(r.sqls, data.SQL)
	}
	r.mu.Unlock()
	return ctx
}

func (r *statementRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// flatten keeps a statement comparable and printable on one line.
func flatten(sql string) string { return strings.Join(strings.Fields(sql), " ") }

// TestLeaseWritersLockTheThreadBeforeTheRun is the product-door half of the lock
// order, and it is what closes the gap the two-session test leaves open.
//
// WHAT THE TWO-SESSION TEST DOES NOT PROVE. It drives the claim side with the
// product's own candidate SQL, but the release side is a hand-written
// `UPDATE ahp_state.runs` shaped like applyRunStatusJournal, because nothing
// outside a transaction can interleave with statements inside one. So it shows
// that the ORDER OF THE TWO DOORS is a cycle when the scan locks a run; it does
// not show that Store.Release itself touches the thread before the run.
//
// This test shows exactly that, through the product's doors and with no seam in
// the product: a pgx QueryTracer on the store's own pool records the statements
// one real Heartbeat / Suspend / Release call issues, and the assertion is about
// the order they were actually sent in - the first row lock must name
// ahp_state.threads, and no write to ahp_state.runs may precede it. It is
// deterministic because the statement order inside one transaction is not a
// race, and it cannot pass vacuously: seeing no statements, seeing no thread
// lock, or (for the doors that end a run) seeing no run write are each a failure.
func TestLeaseWritersLockTheThreadBeforeTheRun(t *testing.T) {
	e := cpFixture(t)
	ctx := e.ctx
	cfg := laneConfig(t)
	recorder := &statementRecorder{}
	cfg.ConnConfig.Tracer = recorder
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("the traced pool could not be built (DSN not printed)")
	}
	defer pool.Close()
	// The per-run database is already migrated by cpFixture; this store is a
	// second door onto the same schema, not a second schema.
	s := New(pool)

	doors := []struct {
		name string
		// wantRunWrite is true for the doors that must write the run row, so the
		// ordering claim has two endpoints to order.
		wantRunWrite bool
		call         func(runID, worker string, epoch int64, scope contracts.Scope) error
	}{
		{"Heartbeat", false, func(runID, worker string, epoch int64, scope contracts.Scope) error {
			_, err := s.Heartbeat(ctx, scope, runID, worker, epoch, time.Minute)
			return err
		}},
		{"Suspend", true, func(runID, worker string, epoch int64, scope contracts.Scope) error {
			// Any well-formed wait target: this test is about statement order,
			// and nothing needs to arrive on this key for the door to run.
			return s.Suspend(ctx, scope, runID, worker, epoch, "wait-order", nil)
		}},
		{"Release", true, func(runID, worker string, epoch int64, scope contracts.Scope) error {
			return s.Release(ctx, scope, runID, worker, epoch, RunDone)
		}},
	}

	for _, door := range doors {
		scope := contracts.Scope{
			Tenant:     "t-order-" + contracts.NewID(),
			Deployment: "d-order-" + contracts.NewID(),
			Subject:    "subj-order-" + contracts.NewID(),
		}
		thread, err := s.CreateThread(ctx, scope, "ctx-order-"+contracts.NewID(), "k-"+contracts.NewID())
		if err != nil {
			t.Fatalf("%s: CreateThread: %v", door.name, err)
		}
		run, err := s.CreateRun(ctx, scope, thread.ID, "task-order", "k-"+contracts.NewID(), json.RawMessage(`{"a":1}`))
		if err != nil {
			t.Fatalf("%s: CreateRun: %v", door.name, err)
		}
		lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "w-"+door.name, run.ID, time.Minute)
		if err != nil {
			t.Fatalf("%s: ClaimRunFor: %v", door.name, err)
		}
		if lease == nil {
			t.Fatalf("%s: the run this test named was never offered", door.name)
		}

		recorder.start()
		callErr := door.call(run.ID, lease.Worker, lease.Epoch, scope)
		stmts := recorder.stop()
		if callErr != nil {
			t.Fatalf("%s: %v", door.name, callErr)
		}

		threadLock, runWrite := -1, -1
		for i, sql := range stmts {
			flat := flatten(sql)
			if threadLock < 0 && strings.Contains(flat, "FOR UPDATE") && strings.Contains(flat, "ahp_state.threads") {
				threadLock = i
			}
			if runWrite < 0 && (strings.Contains(flat, "UPDATE ahp_state.runs") || strings.Contains(flat, "INSERT INTO ahp_state.runs")) {
				runWrite = i
			}
		}
		if len(stmts) == 0 {
			t.Fatalf("%s: the tracer recorded no statement, so this test would pass whatever the order is", door.name)
		}
		if threadLock < 0 {
			t.Fatalf("%s locked no thread row in %d statements, so it holds no lease fence:\n%s",
				door.name, len(stmts), flatten(strings.Join(stmts, " ;; ")))
		}
		if door.wantRunWrite && runWrite < 0 {
			t.Fatalf("%s wrote no run row in %d statements, so the ordering claim has nothing to order",
				door.name, len(stmts))
		}
		if runWrite >= 0 && runWrite < threadLock {
			t.Fatalf("%s writes the run before it locks the thread: statement %d %q precedes the thread lock at %d %q - "+
				"that is the A->B/B->A cycle this package exists to prevent",
				door.name, runWrite, flatten(stmts[runWrite]), threadLock, flatten(stmts[threadLock]))
		}
	}
}
