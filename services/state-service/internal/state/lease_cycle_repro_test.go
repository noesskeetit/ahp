package state

// Closure evidence for the raw-data lease-cycle failure: SQLSTATE 40P01 deadlocks
// and "release -> CONFLICT" measured on a live deployment, where one thread was
// locked in two opposite orders - A->B by the claim (head run, then thread) and
// B->A by every lease writer (thread, then run).
//
// WHAT THIS IS. A load reproduction against real PostgreSQL that uses ONLY
// product doors - ClaimRun/ClaimRunFor on one side, Heartbeat/Suspend/Release/
// PutBatch/CancelWaitingRun on the other - driven N rounds x 5 workers against
// one thread, plus a tally that turns every failure into a counted state, so the
// receipt carries numbers instead of adjectives. It asserts exactly the two
// things the raw data says must not happen: a database abort reaching a client
// (40P01/40001, whatever code it wears), and a CONFLICT that is not a decision
// about the request.
//
// WHY IT IS STATISTICAL, SAID OUT LOUD. A deadlock needs two backends to hold
// and want the same two rows in opposite orders at the same instant, and nothing
// outside a transaction can be scheduled between two statements inside one. So
// this file drives the race and fails on the state that comes out of it, while
// the deterministic two-session proof of the ordering lives in
// TestClaimAndReleaseDoNotFormALockCycle. What this file adds over that one is
// the cancel-waiting and batch doors, worker counts near the measured regime, and
// the counters the R2 decision is taken from (is a replay loop still needed once
// one order exists?).
//
// COMPILES ON BOTH SIDES OF THE FIX, on purpose: it names no symbol the fix
// introduced, so the same file runs RED on the pre-fix tree and GREEN on HEAD with
// no edit. The counters a replay loop would move are read in a sibling file,
// because those symbols exist only on the fixed side.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// cycleWorkersPerRound is the shape of the measured cycle: one holder that renews
// and parks the head run, one holder that comes back to its run AFTER its lease
// lapsed, two pollers claiming whatever the deployment offers, one canceller on
// the waiting head, and one claimer aimed at the second run of the same thread
// (which FIFO must refuse to offer while the head is unfinished).
//
// THE LATE HOLDER IS NOT DECORATION, AND THE FIRST VERSION OF THIS FILE PROVED IT.
// Run against the pre-fix tree as originally written - every writer acting while
// its own lease was still live - the cycle never appeared: 0 deadlocks, 0 CONFLICT
// in 40 rounds, on exactly the tree that deadlocked in production. The reason is in
// the candidate filter: a thread whose lease is still live is not a claim
// candidate at all, so the claim never locks its head run and the two orders can
// not meet. A thread whose lease has EXPIRED is a candidate again while its old
// holder is still walking towards the run row - that is the field shape, and it is
// the only shape in which claim(run -> thread) and writer(thread -> run) hold and
// want the same pair. A repro that omits it does not reproduce the defect, it only
// declines to look for it.
const cycleWorkersPerRound = 6

// cycleRoundsOrDefault lets a receipt ask for a deeper run without an edit; the
// default is the depth this closure was measured at.
func cycleRoundsOrDefault(t *testing.T, def int) int {
	t.Helper()
	v := os.Getenv("AHP_STATE_CYCLE_ROUNDS")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("AHP_STATE_CYCLE_ROUNDS=%q is not a positive round count", v)
	}
	return n
}

// newCyclePool builds the pool the load runs on. The failure text never names the
// DSN: it carries credentials.
func newCyclePool(t *testing.T, ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	t.Helper()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("the cycle pool could not be built (DSN not printed)")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("the cycle database is not reachable: %v", err)
	}
	return pool, nil
}

// cycleTally counts what the load saw. The split matters: deadlock/serialization
// are raw database aborts, conflict/unavailable are what a client was told. A fix
// that renames an abort into a CONFLICT (the original masking) moves the conflict
// counter; a fix that only renames it into UNAVAILABLE moves the unavailable one.
// Both are failures here, for different reasons.
type cycleTally struct {
	calls         atomic.Int64
	deadlock      atomic.Int64
	serialization atomic.Int64
	conflict      atomic.Int64
	unavailable   atomic.Int64
	staleEpoch    atomic.Int64
	notFound      atomic.Int64
	forbidden     atomic.Int64
	other         atomic.Int64
	// cancelDecision is CancelWaitingRun refusing for its own stated reason ("run
	// must be pending or waiting without an active thread lease", cancel_waiting.go).
	// That CONFLICT is a decision about the request, so it is allowed, and counted
	// apart so it can never hide an abort.
	cancelDecision atomic.Int64
	claimNil       atomic.Int64
	contended      atomic.Int64
	secondOffered  atomic.Int64
	fifoProbes     atomic.Int64

	mu      sync.Mutex
	samples []string
}

func (c *cycleTally) sample(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.samples) < 12 {
		c.samples = append(c.samples, line)
	}
}

func (c *cycleTally) sampleList() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.samples...)
}

// cycleAbortState reports the SQLSTATE of a database abort, whether the store kept
// the driver error in the chain or only named it in the detail text. It is
// deliberately independent of the AHP code, because the defect being closed is
// precisely that the code and the state disagreed.
func cycleAbortState(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		if st := pg.SQLState(); st == "40P01" || st == "40001" {
			return st
		}
	}
	msg := err.Error()
	for _, st := range []string{"40P01", "40001"} {
		if strings.Contains(msg, st) {
			return st
		}
	}
	return ""
}

// recordCycleError classifies one door failure. cancelDoor relaxes only the
// CONFLICT class, and never relaxes a database abort: an abort is forbidden on
// every door, including that one.
func recordCycleError(c *cycleTally, door string, err error, cancelDoor bool) {
	if err == nil {
		return
	}
	c.calls.Add(1)
	state := cycleAbortState(err)
	code := contracts.Code(err)
	switch {
	case state == "40P01":
		c.deadlock.Add(1)
		c.sample(fmt.Sprintf("%s: deadlock %s code=%s", door, state, code))
	case state == "40001":
		c.serialization.Add(1)
		c.sample(fmt.Sprintf("%s: serialization %s code=%s", door, state, code))
	case code == contracts.ErrConflict && cancelDoor:
		c.cancelDecision.Add(1)
	case code == contracts.ErrConflict:
		c.conflict.Add(1)
		c.sample(fmt.Sprintf("%s: CONFLICT without a decision: %s", door, firstCycleLine(err.Error())))
	case code == contracts.ErrUnavailable:
		c.unavailable.Add(1)
		c.sample(fmt.Sprintf("%s: UNAVAILABLE reached a client: %s", door, firstCycleLine(err.Error())))
	case code == contracts.ErrStaleEpoch:
		c.staleEpoch.Add(1)
	case code == contracts.ErrNotFound:
		c.notFound.Add(1)
	case code == contracts.ErrForbidden:
		c.forbidden.Add(1)
		c.sample(fmt.Sprintf("%s: FORBIDDEN: %s", door, firstCycleLine(err.Error())))
	default:
		c.other.Add(1)
		c.sample(fmt.Sprintf("%s: unexpected %s: %s", door, code, firstCycleLine(err.Error())))
	}
}

func firstCycleLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 220 {
		s = s[:220]
	}
	return s
}

// runLeaseCycleLoad drives the cycle and returns the tally. Every round is a fresh
// thread, so a run a poller happened to keep cannot park the thread for the rest of
// the run and turn this into a test of the FIFO rule instead.
func runLeaseCycleLoad(t *testing.T, s *Store, ctx context.Context, rounds int) *cycleTally {
	t.Helper()
	count := &cycleTally{}

	for round := 0; round < rounds; round++ {
		// One scope per round, so a poller in this round is aimed at this round's
		// thread instead of at whichever thread the FIFO happens to rank first in a
		// shared queue. Contention on the pair of rows is what is being reproduced.
		scope := contracts.Scope{
			Tenant:     "t-lc-" + contracts.NewID(),
			Deployment: "d-lc-" + contracts.NewID(),
			Subject:    "subj-lc-" + contracts.NewID(),
		}
		thread, err := s.CreateThread(ctx, scope, "ctx-lc-"+contracts.NewID(), "k-lc-"+contracts.NewID())
		if err != nil {
			t.Fatalf("round %d CreateThread: %v", round, err)
		}
		head, err := s.CreateRun(ctx, scope, thread.ID, "task-lc-head", "k-lc-"+contracts.NewID(), json.RawMessage(`{"round":1}`))
		if err != nil {
			t.Fatalf("round %d CreateRun(head): %v", round, err)
		}
		second, err := s.CreateRun(ctx, scope, thread.ID, "task-lc-second", "k-lc-"+contracts.NewID(), json.RawMessage(`{"round":2}`))
		if err != nil {
			t.Fatalf("round %d CreateRun(second): %v", round, err)
		}
		// The watchdog's assertion requires an unfinished head. Without this
		// barrier a terminal writer could win first, making the later second-run
		// claim entirely valid while the test still counted it as a FIFO defect.
		// No database transaction is held while waiting on the test-only barrier.
		fifoChecked := make(chan struct{})

		// Overlap detection. A cycle can only exist while a claim and a lease writer
		// are inside the database at the same moment on this thread, so the test
		// records whether that ever happened. If it never did, every assertion below
		// would be vacuously true and this file would prove nothing.
		var inFlight [2]atomic.Int64 // 0: claim side, 1: lease-writer side
		enter := func(side int) func() {
			opposite := &inFlight[1-side]
			inFlight[side].Add(1)
			if opposite.Load() > 0 {
				count.contended.Add(1)
			}
			var once sync.Once
			return func() { once.Do(func() { inFlight[side].Add(-1) }) }
		}

		var wg sync.WaitGroup
		spawn := func(fn func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fn()
			}()
		}

		// Holder: claims the head, renews it, parks it waiting, then cancels it.
		spawn(func() {
			leave := enter(0)
			lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "w-holder", head.ID, 5*time.Millisecond)
			recordCycleError(count, "ClaimRunFor(holder)", err, false)
			if err != nil {
				leave()
				return
			}
			if lease == nil {
				count.claimNil.Add(1)
				leave()
				return
			}
			leave()

			leave = enter(1)
			_, err = s.Heartbeat(ctx, scope, head.ID, lease.Worker, lease.Epoch, 20*time.Millisecond)
			recordCycleError(count, "Heartbeat(holder)", err, false)
			err = s.Suspend(ctx, scope, head.ID, lease.Worker, lease.Epoch, "ext-lc", nil)
			recordCycleError(count, "Suspend(holder)", err, false)
			leave()
			<-fifoChecked
			leave = enter(1)
			defer leave()
			err = s.CancelWaitingRun(ctx, scope, head.ID, thread.ID)
			recordCycleError(count, "CancelWaitingRun(holder)", err, true)
		})

		// Two pollers: claim whatever the deployment offers, journal a NON-terminal
		// batch through the same fence, then finish the run. Non-terminal on purpose,
		// so this file means the same thing on trees from before and after the
		// terminal-explanation contract, and it asks nothing of that contract here.
		for _, name := range []string{"w-poll-a", "w-poll-b"} {
			worker := name
			spawn(func() {
				for attempt := 0; attempt < 5; attempt++ {
					leave := enter(0)
					lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, worker, 5*time.Millisecond)
					count.calls.Add(1)
					if err != nil {
						recordCycleError(count, "ClaimRun("+worker+")", err, false)
						leave()
						return
					}
					if lease == nil {
						count.claimNil.Add(1)
						leave()
						time.Sleep(time.Millisecond)
						continue
					}
					leave()

					leave = enter(1)
					_, err = s.PutBatch(ctx, scope,
						Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch},
						BatchRequest{Key: "k-lc-" + contracts.NewID(), RunStatus: &RunStatusOp{Status: RunRunning}})
					recordCycleError(count, "PutBatch("+worker+")", err, false)
					leave()
					<-fifoChecked
					leave = enter(1)
					// ReleaseWithReason: a terminal transition owes a reason in this
					// contract, on both trees, and this closure is not the place to
					// argue with it.
					err = s.ReleaseWithReason(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, RunDone, "model_finished")
					recordCycleError(count, "Release("+worker+")", err, false)
					leave()
					return
				}
			})
		}

		// Late holder: claims, lets its own lease lapse, then goes to the run row
		// anyway. Its thread is a legitimate candidate for the pollers by then, so
		// the claim side can be holding the run this worker is about to want under
		// the thread lock. STALE_EPOCH afterwards is honest and expected; an abort is
		// not, and on the two-order tree this is where the abort comes from.
		spawn(func() {
			leave := enter(0)
			lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "w-late", head.ID, 5*time.Millisecond)
			recordCycleError(count, "ClaimRunFor(late)", err, false)
			if err != nil {
				leave()
				return
			}
			if lease == nil {
				count.claimNil.Add(1)
				leave()
				return
			}
			leave()

			time.Sleep(25 * time.Millisecond) // the lease this worker held has expired

			leave = enter(1)
			_, err = s.Heartbeat(ctx, scope, head.ID, lease.Worker, lease.Epoch, 5*time.Millisecond)
			recordCycleError(count, "Heartbeat(late)", err, false)
			leave()
			<-fifoChecked
			leave = enter(1)
			defer leave()
			err = s.ReleaseWithReason(ctx, scope, head.ID, lease.Worker, lease.Epoch, RunFailed, "error")
			recordCycleError(count, "Release(late)", err, false)
		})

		// Canceller racing the holder's suspend: honest refusals are decisions.
		spawn(func() {
			<-fifoChecked
			for attempt := 0; attempt < 2; attempt++ {
				leave := enter(1)
				err := s.CancelWaitingRun(ctx, scope, head.ID, thread.ID)
				recordCycleError(count, "CancelWaitingRun(canceller)", err, true)
				leave()
				time.Sleep(time.Millisecond)
			}
		})

		// FIFO watchdog: the second run of a thread must not be offered while the
		// head of that thread is unfinished. A lease here is a queue-order bug, not a
		// contention outcome.
		spawn(func() {
			defer close(fifoChecked)
			leave := enter(0)
			defer leave()
			current, err := s.GetRun(ctx, scope, head.ID)
			if err != nil {
				recordCycleError(count, "GetRun(FIFO precondition)", err, false)
				return
			}
			if current.Status == RunDone || current.Status == RunFailed || current.Status == RunCanceled {
				count.other.Add(1)
				count.sample("FIFO watchdog precondition violated: head already terminal")
				return
			}
			count.fifoProbes.Add(1)
			lease, err := s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "w-second", second.ID, 20*time.Millisecond)
			recordCycleError(count, "ClaimRunFor(second)", err, false)
			if err != nil {
				return
			}
			if lease == nil {
				count.claimNil.Add(1)
				return
			}
			count.secondOffered.Add(1)
			recordCycleError(count, "Release(second)",
				s.ReleaseWithReason(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, RunCanceled, "error"), false)
		})

		wg.Wait()
	}
	return count
}

// report writes the receipt lines: one number per state, then the samples.
func (c *cycleTally) report(t *testing.T, rounds int) {
	t.Helper()
	t.Logf("lease-cycle receipt: rounds=%d workers_per_round=%d calls=%d contended_overlaps=%d",
		rounds, cycleWorkersPerRound, c.calls.Load(), c.contended.Load())
	t.Logf("lease-cycle counters: 40P01=%d 40001=%d CONFLICT=%d UNAVAILABLE=%d STALE_EPOCH=%d NOT_FOUND=%d FORBIDDEN=%d other=%d cancel_decisions=%d claim_nil=%d second_run_offered=%d",
		c.deadlock.Load(), c.serialization.Load(), c.conflict.Load(), c.unavailable.Load(),
		c.staleEpoch.Load(), c.notFound.Load(), c.forbidden.Load(), c.other.Load(),
		c.cancelDecision.Load(), c.claimNil.Load(), c.secondOffered.Load())
	t.Logf("lease-cycle FIFO preconditions: unfinished_heads=%d", c.fifoProbes.Load())
	for _, line := range c.sampleList() {
		t.Logf("lease-cycle sample: %s", line)
	}
}

// A second-run lease is a failure only while the first run is unfinished. The
// original concurrent watchdog did not preserve that condition. Pin both sides
// of the actual FIFO boundary independently of the statistical deadlock load.
func TestLeaseCycleFIFOBoundaryAfterTerminal(t *testing.T) {
	e := cpFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope := contracts.Scope{Tenant: contracts.NewID(), Deployment: "fifo", Subject: "alice"}
	thread, err := e.s.CreateThread(ctx, scope, "fifo-context-"+contracts.NewID(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	head, err := e.s.CreateRun(ctx, scope, thread.ID, "first-task", "first", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.s.CreateRun(ctx, scope, thread.ID, "second-task", "second", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := e.s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "head-worker", head.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("head claim: %v", err)
	}
	if next, err := e.s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "second-worker", second.ID, time.Minute); err != nil || next != nil {
		t.Fatalf("unfinished head did not block the second run: lease=%v err=%v", next, err)
	}
	if err = e.s.ReleaseWithReason(ctx, scope, head.ID, lease.Worker, lease.Epoch, RunDone, "model_finished"); err != nil {
		t.Fatal(err)
	}
	next, err := e.s.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "second-worker", second.ID, time.Minute)
	if err != nil || next == nil || next.Run.ID != second.ID {
		t.Fatalf("terminal head did not permit the second run: lease=%v err=%v", next, err)
	}
	if err = e.s.ReleaseWithReason(ctx, scope, second.ID, next.Worker, next.Epoch, RunDone, "model_finished"); err != nil {
		t.Fatal(err)
	}
}

// TestLeaseCycleTwoWorkersLeavesNoDeadlockOrFakeConflict is the closure leg. On the
// pre-fix tree it is red: the claim takes the head run then the thread, the writers
// take the thread then the run, and PostgreSQL aborts one side - which reached a
// client wearing a CONFLICT. On the fixed tree every forbidden counter stays at zero
// while the overlap detector says the race really happened.
func TestLeaseCycleTwoWorkersLeavesNoDeadlockOrFakeConflict(t *testing.T) {
	// The fixture exists for one reason: it provisions and migrates the per-run
	// database this package isolates (isolation_test.go). The load then runs on a
	// pool of its own, because five workers must never queue behind one
	// connection and call that contention.
	_ = cpFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	cfg := laneConfig(t)
	cfg.MaxConns = 24 // 5 workers must never serialise on the pool
	pool, err := newCyclePool(t, ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}

	rounds := cycleRoundsOrDefault(t, 40)
	count := runLeaseCycleLoad(t, s, ctx, rounds)
	count.report(t, rounds)

	// Anti-vacuity, before the counters: with no measured overlap this test would
	// pass on any tree, including the broken one.
	if n := count.contended.Load(); n < int64(rounds) {
		t.Fatalf("the claim side and the lease-writer side overlapped only %d time(s) in %d rounds, "+
			"so no cycle was possible and nothing was proved", n, rounds)
	}
	if n := count.secondOffered.Load(); n != 0 {
		t.Errorf("the second run of a thread was offered %d time(s) while its head was unfinished: "+
			"that is queue order, not contention", n)
	}
	if n := count.fifoProbes.Load(); n != int64(rounds) {
		t.Errorf("FIFO watchdog verified %d unfinished heads, want exactly %d", n, rounds)
	}
	if n := count.deadlock.Load(); n != 0 {
		t.Errorf("%d call(s) were aborted by a deadlock (40P01): two lock orders are still present", n)
	}
	if n := count.serialization.Load(); n != 0 {
		t.Errorf("%d call(s) were aborted by a serialization failure (40001) and reached a client", n)
	}
	if n := count.conflict.Load(); n != 0 {
		t.Errorf("%d call(s) reported CONFLICT where the request was not the reason: an aborted "+
			"transaction wearing a decision code is the masking this closure is about", n)
	}
	if n := count.unavailable.Load(); n != 0 {
		t.Errorf("%d call(s) reported UNAVAILABLE: an abort escaped the replay budget, so the cycle "+
			"is bounded and not closed", n)
	}
	if n := count.forbidden.Load(); n != 0 {
		t.Errorf("%d call(s) were refused FORBIDDEN while the caller held the conversation this test created", n)
	}
	if n := count.other.Load(); n != 0 {
		t.Errorf("%d call(s) failed with a state this closure does not account for (see samples)", n)
	}
}
