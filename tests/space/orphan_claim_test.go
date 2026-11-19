package space_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// claimableCore is a Core double that counts executions and reports how many are
// running AT THE SAME TIME, so a test can prove the sweep never runs two
// executions of one run concurrently. block keeps an execution alive until the
// test releases it, which is how a hung/abandoned worker is simulated.
type claimableCore struct {
	mu        sync.Mutex
	calls     int
	concursor int32
	maxConcur int32
	block     chan struct{}

	// parked, when set, is closed the first time an execution parks on `block`. Counters
	// bumped a few instructions apart are not an event: this is how a test waits for
	// "the worker is genuinely stuck inside Core" without sampling.
	parked   chan struct{}
	parkOnce sync.Once
}

// park signals (at most once) that this execution is stuck on `block`.
func (c *claimableCore) park() {
	if c.parked == nil {
		return
	}
	c.parkOnce.Do(func() { close(c.parked) })
}

func (c *claimableCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	cur := atomic.AddInt32(&c.concursor, 1)
	for {
		old := atomic.LoadInt32(&c.maxConcur)
		if cur <= old || atomic.CompareAndSwapInt32(&c.maxConcur, old, cur) {
			break
		}
	}
	defer atomic.AddInt32(&c.concursor, -1)

	if c.block != nil {
		c.park()
		select {
		case <-c.block:
		case <-ctx.Done():
			results <- space.StepResult{StepType: space.StepError, Done: true}
			return ctx.Err()
		}
	}
	results <- space.StepResult{StepType: space.StepDone, Content: json.RawMessage(`{"text":"done"}`), Done: true}
	return nil
}

// active reports whether an execution is still inside Core.
func atomicActive(c *claimableCore) bool { return atomic.LoadInt32(&c.concursor) > 0 }

func (c *claimableCore) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func runStatus(t *testing.T, ms *mockState, runID string) string {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	r, ok := ms.runs[runID]
	if !ok {
		t.Fatalf("run %s is not in durable State at all", runID)
	}
	return r.Status
}

func waitForStatus(t *testing.T, ms *mockState, runID, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if st := runStatus(t, ms, runID); st == want {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runStatus(t, ms, runID)
}

func acceptTurn(t *testing.T, c *space.Coordinator, ctxID, key string) (turnID, threadID string) {
	t.Helper()
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}
	acc, err := c.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       key,
		ContextID: ctxID,
		Input:     json.RawMessage(`{"role":"user","content":"sweep me"}`),
		MaxSteps:  2,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	return acc.TurnID, acc.ThreadID
}

// TestOrphanSweepDispatchesRunWithoutClient pins the DEFECT measured on the composed
// stand: a run that was accepted while no worker was alive stayed `pending` forever
// (40 polls, FAIL_NO_ARTIFACT) because ClaimRun was reachable only from DispatchTurn.
// Per the PDF the lease and the orphan pickup are State primitives that a runner uses
// on its own ("другая реплика подбирает задачу"), so recovery must not depend on a
// client ever calling DispatchTurn again.
func TestOrphanSweepDispatchesRunWithoutClient(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	core := &claimableCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: core, WorkerID: "sweep-1"})

	runID, _ := acceptTurn(t, coord, "ctx-sweep-alone", "sweep-alone-"+contracts.NewID())
	if st := runStatus(t, ms, runID); st != "pending" {
		t.Fatalf("freshly accepted run status = %q, want pending", st)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coord.StartOrphanClaimLoop(ctx, 10*time.Millisecond)

	if got := waitForStatus(t, ms, runID, "done", 5*time.Second); got != "done" {
		t.Fatalf("run status after sweep = %q, want done (no client dispatched it)", got)
	}
	if n := core.callCount(); n != 1 {
		t.Fatalf("Core executions = %d, want exactly 1", n)
	}
	if m := atomic.LoadInt32(&core.maxConcur); m != 1 {
		t.Fatalf("max concurrent Core executions = %d, want 1", m)
	}
}

// TestOrphanSweepDisabledKeepsRunPending is the negative control for the test above:
// with the sweep interval non-positive (the previous behaviour) the very same setup
// must NOT advance the run. Without it the first test could pass for an unrelated
// reason, e.g. an accidental dispatch inside AcceptTurn.
func TestOrphanSweepDisabledKeepsRunPending(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)

	core := &claimableCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: core, WorkerID: "sweep-off"})

	runID, _ := acceptTurn(t, coord, "ctx-sweep-off", "sweep-off-"+contracts.NewID())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	coord.StartOrphanClaimLoop(ctx, 0)
	coord.StartOrphanClaimLoop(ctx, -time.Second)

	time.Sleep(300 * time.Millisecond)
	if st := runStatus(t, ms, runID); st != "pending" {
		t.Fatalf("run status with sweep disabled = %q, want pending", st)
	}
	if n := core.callCount(); n != 0 {
		t.Fatalf("Core executions = %d with sweep disabled, want 0", n)
	}
}

// TestSecondWorkerSweepCompletesAbandonedRun is the durable handoff leg: worker A takes
// the run and hangs, the lease is lost, and a DIFFERENT worker completes it from its own
// sweep with no client and no restart of the whole stack.
//
// The handoff is sequenced by EVENTS and by exactly one claim per owner, never by a
// wall-clock sleep and a context.Cancel that only *eventually* stops a sweeper. The
// previous version started worker A's sweep loop and "killed" it with cancel before
// handing the run over, but cancel does not withdraw a tick that is already in flight:
// that tick still calls ClaimRun, and this offline double - unlike the real server, whose
// claim is one database transaction - commits the claim even after the client gave up on
// the response. Worker A therefore often became a SECOND live owner of the very run the
// test was handing over: when its late claim won the race it kept the hung Core, worker B
// never got the run, and the row stayed `running` until the test timed out (~20% of
// -count=30 runs, measured on the composed pin's clean tree).
//
// Here worker A claims exactly once, synchronously, through DispatchTurn - the same claim
// path and the same lease TTL the sweep uses - so the run has one owner at a time, and the
// only background sweeper in the test is the worker whose behaviour is under test. Nothing
// about the fence is relaxed: the point is proved twice, because worker A is woken up
// AFTER the handoff and must still be refused by worker B's higher epoch.
func TestSecondWorkerSweepCompletesAbandonedRun(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "user-ada"}

	// Worker A hangs INSIDE Core: the abandoned process of the PDF's "упал после отправки
	// вызова". It is never cancelled, so it is a live process with a dead lease - the only
	// shape of "abandoned" that State's epoch rule is about.
	hung := &claimableCore{block: make(chan struct{}), parked: make(chan struct{})}
	var wakeA sync.Once
	wakeWorkerA := func() { wakeA.Do(func() { close(hung.block) }) }
	t.Cleanup(wakeWorkerA)

	workerA := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: hung, WorkerID: "worker-a"})
	runID, threadID := acceptTurn(t, workerA, "ctx-handoff", "handoff-"+contracts.NewID())

	if err := workerA.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("worker A DispatchTurn: %v", err)
	}
	// Core is entered on the claim's own goroutine, so the test waits for the event that
	// says worker A is parked inside Core: abandoned, alive, and unable to finish the run.
	select {
	case <-hung.parked:
	case <-time.After(5 * time.Second):
		t.Fatal("worker A never got inside Core - the run is not an abandoned in-flight run")
	}
	if st := runStatus(t, ms, runID); st != "running" {
		t.Fatalf("run status under worker A = %q, want running", st)
	}
	epochA := threadEpoch(t, ms, threadID)
	if epochA <= 0 {
		t.Fatalf("worker A holds no epoch: %d", epochA)
	}

	// The lease lapses. State marks the thread lease expired, so the row of that thread
	// becomes claimable again and its NEXT claim carries a HIGHER epoch; the offline double
	// models that claimable state by dropping the lease and returning the row to the queue.
	ms.mu.Lock()
	ms.runs[runID].Status = "pending"
	delete(ms.leases, runID)
	ms.mu.Unlock()

	// Worker B - another process, no client - picks the run up from its own sweep.
	fresh := &claimableCore{}
	workerB := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: fresh, WorkerID: "worker-b"})
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	workerB.StartOrphanClaimLoop(ctxB, 10*time.Millisecond)

	if got := waitForStatus(t, ms, runID, "done", 5*time.Second); got != "done" {
		t.Fatalf("run status after worker B sweep = %q, want done", got)
	}
	if n := fresh.callCount(); n != 1 {
		t.Fatalf("worker B Core executions = %d, want exactly 1", n)
	}
	if m := atomic.LoadInt32(&fresh.maxConcur); m != 1 {
		t.Fatalf("worker B max concurrent Core executions = %d, want 1", m)
	}
	epochB := threadEpoch(t, ms, threadID)
	if epochB <= epochA {
		t.Fatalf("handoff did not raise the epoch: worker A %d, worker B %d", epochA, epochB)
	}

	// The durable stream is the witness: worker A is still alive, so wake it and let it try
	// to finish the run with its OWN, now superseded, fence. State's epoch rule must refuse
	// every write it makes - the log lines "durable stream hole"/"terminal status event not
	// durable" at the abandoned epoch are that refusal happening, not a lost write. Zero new
	// durable events after the handoff is the proof.
	eventsBefore := streamEventCount(ms, threadID)
	wakeWorkerA()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomicActive(hung) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomicActive(hung) {
		t.Fatal("worker A's Core never left the run")
	}
	// Give any late write of the stale epoch the chance to land before reading it back.
	time.Sleep(100 * time.Millisecond)
	if got := streamEventCount(ms, threadID); got != eventsBefore {
		t.Fatalf("the superseded worker wrote %d durable stream events after the handoff, want 0 (fence weakened)", got-eventsBefore)
	}
	if st := runStatus(t, ms, runID); st != "done" {
		t.Fatalf("run status after the superseded worker woke up = %q, want it to stay done", st)
	}
	if n := hung.callCount(); n != 1 {
		t.Fatalf("worker A Core executions = %d after the handoff, want it to have run once", n)
	}
}

// threadEpoch reads the durable lease epoch of a thread: every claim raises it, so it is
// the authoritative witness of "who may write this thread now".
func threadEpoch(t *testing.T, ms *mockState, threadID string) int64 {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	th, ok := ms.threads[threadID]
	if !ok {
		t.Fatalf("thread %s is not in durable State", threadID)
	}
	return th.Epoch
}

// streamEventCount counts the durable thread-stream events, so a test can state exactly
// how much a given writer left behind.
func streamEventCount(ms *mockState, threadID string) int {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	n := 0
	for _, item := range ms.streams {
		if item.ThreadID == threadID {
			n++
		}
	}
	return n
}
