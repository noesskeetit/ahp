package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// sameJSONValue compares two JSON documents by decoded value, never by raw
// bytes: PostgreSQL jsonb reorders keys and normalises whitespace, so a
// byte-for-byte comparison would only test the serialiser, not the store.
func sameJSONValue(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("conformance: decode stored json %q: %v", a, err)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("conformance: decode expected json %q: %v", b, err)
	}
	ca, err := json.Marshal(va)
	if err != nil {
		t.Fatalf("conformance: re-encode stored json: %v", err)
	}
	cb, err := json.Marshal(vb)
	if err != nil {
		t.Fatalf("conformance: re-encode expected json: %v", err)
	}
	return bytes.Equal(ca, cb)
}

// forceExpiredWaiting parks runID as waiting with a deadline the database
// clock has ALREADY passed. This is the only way to test the forced-expiry
// wake path without wall-clock sleeps: the deadline is written in database
// time, one second in the past.
func forceExpiredWaiting(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID string) {
	t.Helper()
	tag, err := pool.Exec(ctx, `
		UPDATE ahp_state.runs
		   SET status = 'waiting', wait_for = 'sig-' || substr(md5(random()::text), 1, 8),
		       deadline = clock_timestamp() - interval '1 second',
		       updated_at = clock_timestamp()
		 WHERE id = $1`, runID)
	if err != nil {
		t.Fatalf("conformance: force expired waiting on run %s: %v", runID, err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("conformance: forcing waiting state touched %d rows for run %s, want 1",
			tag.RowsAffected(), runID)
	}
}

// TestConcurrentClaimsGrantExactlyOneLease hammers one thread with 100
// concurrent ClaimRun calls and pins the single-lease invariant: exactly one
// worker walks away with a lease, and it covers the FIFO head.
func TestConcurrentClaimsGrantExactlyOneLease(t *testing.T) {
	ctx, _, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	head := createRun(t, ctx, store, scope, thread.ID, "task-hot", jsonPayload(t, "hot"))
	createRun(t, ctx, store, scope, thread.ID, "task-next", jsonPayload(t, "next"))

	const workers = 100
	var (
		wg       sync.WaitGroup
		start    = make(chan struct{})
		leases   = make(chan *state.Lease, workers)
		errs     = make(chan error, workers)
		claimErr atomic.Int64
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			l, err := store.ClaimRun(ctx, scope.Tenant, scope.Deployment,
				"worker-"+contracts.NewID(), time.Minute)
			if err != nil {
				claimErr.Add(1)
				errs <- err
				return
			}
			if l != nil {
				leases <- l
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(leases)

	for err := range errs {
		t.Errorf("conformance: concurrent ClaimRun: %v", err)
	}
	if claimErr.Load() > 0 {
		t.Fatalf("conformance: %d of %d concurrent claims failed", claimErr.Load(), workers)
	}

	granted := 0
	for l := range leases {
		granted++
		if l.Run.ID != head.ID {
			t.Errorf("conformance: concurrent claim granted run %s (seq %d), want head %s (seq %d)",
				l.Run.ID, l.Run.Sequence, head.ID, head.Sequence)
		}
		if l.Thread.ID != thread.ID {
			t.Errorf("conformance: lease is on thread %s, want %s", l.Thread.ID, thread.ID)
		}
		if l.Epoch < 1 {
			t.Errorf("conformance: granted lease carries epoch %d, want >= 1", l.Epoch)
		}
	}
	if granted != 1 {
		t.Fatalf("conformance: %d concurrent claims produced %d leases, want exactly 1", workers, granted)
	}
}

// TestExpiredLeaseReclaimIncrementsEpochFencesStaleWorker proves that an
// expired running lease is reclaimable, that the reclaim raises the epoch, and
// that the old worker is fenced off with STALE_EPOCH on heartbeat, release and
// suspend while the new holder keeps working.
func TestExpiredLeaseReclaimIncrementsEpochFencesStaleWorker(t *testing.T) {
	ctx, pool, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	run := createRun(t, ctx, store, scope, thread.ID, "task-lease", jsonPayload(t, "lease"))

	first := claim(t, ctx, store, scope, "worker-stale", time.Minute, 2*time.Second)
	if first == nil || first.Run.ID != run.ID {
		t.Fatalf("conformance: first claim returned %s, want run %s", leaseOf(first), run.ID)
	}
	if _, err := store.Heartbeat(ctx, scope, run.ID, "worker-stale", first.Epoch, time.Minute); err != nil {
		t.Fatalf("conformance: heartbeat on a live lease: %v", err)
	}

	expireLease(t, ctx, pool, thread.ID, run.ID)

	second := claim(t, ctx, store, scope, "worker-new", time.Minute, 2*time.Second)
	if second == nil || second.Run.ID != run.ID {
		t.Fatalf("conformance: expired lease was not reclaimed: %s", leaseOf(second))
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("conformance: reclaim epoch %d did not exceed the original epoch %d",
			second.Epoch, first.Epoch)
	}

	// The fenced worker can do nothing with its old token.
	if _, err := store.Heartbeat(ctx, scope, run.ID, "worker-stale", first.Epoch, time.Minute); err == nil {
		t.Fatal("conformance: stale heartbeat succeeded")
	} else if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Errorf("conformance: stale heartbeat: want STALE_EPOCH, got %v", err)
	}
	if err := store.Release(ctx, scope, run.ID, "worker-stale", first.Epoch, state.RunDone); err == nil {
		t.Fatal("conformance: stale release succeeded")
	} else if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Errorf("conformance: stale release: want STALE_EPOCH, got %v", err)
	}
	if err := store.Suspend(ctx, scope, run.ID, "worker-stale", first.Epoch, "sig-x", nil); err == nil {
		t.Fatal("conformance: stale suspend succeeded")
	} else if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Errorf("conformance: stale suspend: want STALE_EPOCH, got %v", err)
	}

	// The new holder is unaffected.
	if _, err := store.Heartbeat(ctx, scope, run.ID, "worker-new", second.Epoch, time.Minute); err != nil {
		t.Errorf("conformance: heartbeat on the reclaimed lease: %v", err)
	}
	mustNoErr(t, store.Release(ctx, scope, run.ID, "worker-new", second.Epoch, state.RunDone), "release by new holder")
}

// TestWaitingRunWithoutDeadlineBlocksFifo pins that a waiting run with no
// deadline holds its FIFO position forever: neither the waiting run nor any
// later run of the thread is claimable.
func TestWaitingRunWithoutDeadlineBlocksFifo(t *testing.T) {
	ctx, _, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	first := createRun(t, ctx, store, scope, thread.ID, "task-blocker", jsonPayload(t, "block"))
	createRun(t, ctx, store, scope, thread.ID, "task-behind", jsonPayload(t, "behind"))

	l := claim(t, ctx, store, scope, "worker-a", time.Minute, 2*time.Second)
	if l == nil || l.Run.ID != first.ID {
		t.Fatalf("conformance: expected the head run, got %s", leaseOf(l))
	}
	mustNoErr(t, store.Suspend(ctx, scope, first.ID, l.Worker, l.Epoch, "sig-gate", nil), "Suspend without deadline")

	claimEmpty(t, ctx, store, scope, "worker-b")

	behind, err := store.ListRuns(ctx, scope, thread.ID)
	mustNoErr(t, err, "ListRuns")
	if behind[0].Status != state.RunWaiting || behind[0].Deadline != nil {
		t.Fatalf("conformance: blocker is %+v, want waiting with no deadline", behind[0])
	}
	if behind[1].Status != state.RunPending {
		t.Fatalf("conformance: blocked run jumped the queue: %s", behind[1].Status)
	}
}

// TestExpiredWaitingDeadlineWakesSameRun proves that a waiting run whose
// deadline passed in database time becomes claimable again, that the SAME run
// row is handed out (never a later one), and that the wake clears its wait
// state.
func TestExpiredWaitingDeadlineWakesSameRun(t *testing.T) {
	ctx, pool, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	first := createRun(t, ctx, store, scope, thread.ID, "task-wait", jsonPayload(t, "wait"))
	createRun(t, ctx, store, scope, thread.ID, "task-later", jsonPayload(t, "later"))

	l := claim(t, ctx, store, scope, "worker-a", time.Minute, 2*time.Second)
	mustNoErr(t, store.Suspend(ctx, scope, first.ID, l.Worker, l.Epoch, "sig-gate", nil), "Suspend")

	forceExpiredWaiting(t, ctx, pool, first.ID)

	woken := claim(t, ctx, store, scope, "worker-b", time.Minute, 2*time.Second)
	if woken == nil {
		t.Fatal("conformance: a waiting run with an expired deadline never became claimable")
	}
	if woken.Run.ID != first.ID {
		t.Fatalf("conformance: expired deadline woke run %s, want the same run %s", woken.Run.ID, first.ID)
	}
	if woken.Run.Status != state.RunRunning || woken.Run.WaitFor != "" || woken.Run.Deadline != nil {
		t.Fatalf("conformance: woken run is %+v, want running with wait state cleared", woken.Run)
	}
	if woken.Epoch <= l.Epoch {
		t.Fatalf("conformance: wake claim epoch %d did not exceed the suspend-era epoch %d", woken.Epoch, l.Epoch)
	}
}

// TestDoneReleasesNextQueuedRun proves the FIFO advances: while the head run
// is running nothing else is claimable, and the moment it is released as done
// the next queued run becomes available.
func TestDoneReleasesNextQueuedRun(t *testing.T) {
	ctx, _, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	first := createRun(t, ctx, store, scope, thread.ID, "task-1", jsonPayload(t, "one"))
	second := createRun(t, ctx, store, scope, thread.ID, "task-2", jsonPayload(t, "two"))

	l := claim(t, ctx, store, scope, "worker-a", time.Minute, 2*time.Second)
	if l == nil || l.Run.ID != first.ID {
		t.Fatalf("conformance: first claim returned %s, want %s", leaseOf(l), first.ID)
	}
	claimEmpty(t, ctx, store, scope, "worker-b")

	mustNoErr(t, store.Release(ctx, scope, first.ID, "worker-a", l.Epoch, state.RunDone), "Release done")

	next := claim(t, ctx, store, scope, "worker-b", time.Minute, 2*time.Second)
	if next == nil || next.Run.ID != second.ID {
		t.Fatalf("conformance: after done the next queued run was not available: %s", leaseOf(next))
	}
	if next.Run.Sequence != second.Sequence {
		t.Fatalf("conformance: claimed run has sequence %d, want %d", next.Run.Sequence, second.Sequence)
	}
}

// TestNewStoreOnPoolSeesDurableState pins that state lives in PostgreSQL, not
// in the Store: a brand-new Store over the same pool reads back threads, runs
// and a live lease exactly as the original writer left them.
func TestNewStoreOnPoolSeesDurableState(t *testing.T) {
	ctx, pool, store, scope := testStore(t)
	thread := newThread(t, ctx, store, scope)
	run := createRun(t, ctx, store, scope, thread.ID, "task-restart", jsonPayload(t, "restart"))

	l := claim(t, ctx, store, scope, "worker-a", time.Minute, 2*time.Second)
	if l == nil || l.Run.ID != run.ID {
		t.Fatalf("conformance: claim returned %s, want %s", leaseOf(l), run.ID)
	}

	restarted := state.New(pool)

	if got, err := restarted.GetThread(ctx, scope, thread.ID); err != nil || got.ID != thread.ID {
		t.Fatalf("conformance: restarted store lost the thread: %+v (err %v)", got, err)
	}
	if got, err := restarted.GetRun(ctx, scope, run.ID); err != nil || got.ID != run.ID ||
		got.Status != state.RunRunning {
		t.Fatalf("conformance: restarted store lost the run: %+v (err %v)", got, err)
	}
	runs, err := restarted.ListRuns(ctx, scope, thread.ID)
	mustNoErr(t, err, "ListRuns on restarted store")
	if len(runs) != 1 || runs[0].Sequence != 1 {
		t.Fatalf("conformance: restarted store lists %v, want one run at sequence 1", runs)
	}
	if got, err := restarted.GetThreadByContext(ctx, scope, thread.ContextID); err != nil || got.ID != thread.ID {
		t.Fatalf("conformance: restarted store lost the context index: %+v (err %v)", got, err)
	}

	// The live lease survives the restart: the same token still heartbeats
	// through the new Store, and nobody else can claim the thread.
	if _, err := restarted.Heartbeat(ctx, scope, run.ID, "worker-a", l.Epoch, time.Minute); err != nil {
		t.Fatalf("conformance: heartbeat through the restarted store: %v", err)
	}
	if other := claim(t, ctx, restarted, scope, "worker-b", time.Minute, 300*time.Millisecond); other != nil {
		t.Fatalf("conformance: a live lease did not survive the restart: %s", leaseOf(other))
	}

	// The payload survives the restart with its full JSON value (jsonb
	// reorders keys, so compare decoded values, not bytes).
	var stored json.RawMessage
	if err := pool.QueryRow(ctx, `SELECT payload FROM ahp_state.runs WHERE id = $1`, run.ID).Scan(&stored); err != nil {
		t.Fatalf("conformance: read payload back: %v", err)
	}
	if !sameJSONValue(t, stored, run.Payload) {
		t.Fatalf("conformance: stored payload %s does not carry the value %s", stored, run.Payload)
	}

	mustNoErr(t, restarted.Release(ctx, scope, run.ID, "worker-a", l.Epoch, state.RunDone),
		"Release through the restarted store")
}
