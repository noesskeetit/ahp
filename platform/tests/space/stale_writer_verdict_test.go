package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A run that loses its lease mid-flight must NOT report a verdict.
//
// The mid-run emit path treated EVERY AppendStream refusal as a durable stream hole and
// stamped the turn `failed`. But a STALE_EPOCH is not a hole: it is State fencing this
// worker because the lease was taken over (or expired). A newer owner may still finish the
// run `done`, so a `failed` verdict here puts a false verdict next to State's real one - the
// same split D8/D16 already close for other paths. A fenced writer reports nothing durable
// and leaves the decision to State.
//
// The double already fences appends exactly like real State (STALE_EPOCH once the lease is
// gone), so no new hook is needed: steal the lease and the fenced append arrives on its own.

// leaseLossCore emits one step, pauses, then - after the test takes the lease away - emits
// a second step whose append is refused. It is the choreographed worker the coordinator
// executes while the fence moves under it.
type leaseLossCore struct {
	first   chan struct{} // closed once the first step has been appended successfully
	proceed chan struct{} // closed by the test after the lease is taken away
}

func (c *leaseLossCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	select {
	case results <- space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`"one"`)}:
	case <-ctx.Done():
		return ctx.Err()
	}
	close(c.first)

	select {
	case <-c.proceed:
	case <-ctx.Done():
		results <- space.StepResult{StepType: space.StepError, Done: true}
		return ctx.Err()
	}
	select {
	case results <- space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`"two"`)}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// emit refuses the second append, which cancels Core's context; end like a real Core.
	<-ctx.Done()
	return ctx.Err()
}

// stealLease simulates a take-over: a different worker holds the run's lease at a higher
// epoch, exactly as State's ClaimRun does after a fence. The previous holder's next durable
// write is refused with STALE_EPOCH, changing nothing durable.
func (m *mockState) stealLease(runID, newWorker string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	var target *runRecord
	for _, r := range m.runs {
		if r.ID == runID {
			target = r
			break
		}
	}
	if target == nil {
		return false
	}
	newEpoch := int64(1)
	if l := m.leases[runID]; l != nil {
		newEpoch = l.Epoch + 1
	}
	if th := m.threads[target.ThreadID]; th != nil {
		th.Epoch = newEpoch
	}
	m.leases[runID] = &leaseRecord{RunID: runID, Worker: newWorker, Epoch: newEpoch}
	return true
}

// awaitTurnSettled waits for the turn to leave the running state and returns its status.
func awaitTurnSettled(t *testing.T, coord *space.Coordinator, turnID string) space.TurnStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if turn, ok := coord.TurnSnapshot(turnID); ok && (turn.Status.IsTerminal() || turn.Status == space.TurnUnknown) {
			return turn.Status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("turn never left the running state")
	return ""
}

func TestStaleWriterDoesNotReportFailed(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	core := &leaseLossCore{first: make(chan struct{}), proceed: make(chan struct{})}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        core,
		WorkerID:    "old-owner",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "stale-writer-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"go"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	// The first step appends fine - the lease is still held here.
	select {
	case <-core.first:
	case <-time.After(5 * time.Second):
		t.Fatal("core never appended its first step")
	}
	turn, ok := coord.TurnSnapshot(accept.TurnID)
	if !ok {
		t.Fatal("turn not tracked after dispatch")
	}
	if turn.RunID == "" {
		t.Fatal("turn has no run id to fence")
	}
	if !ms.stealLease(turn.RunID, "new-owner") {
		t.Fatal("could not take over the lease in the double")
	}
	// Now let Core emit a second step: its append is refused with STALE_EPOCH.
	close(core.proceed)

	if got := awaitTurnSettled(t, coord, accept.TurnID); got != space.TurnUnknown {
		t.Fatalf("a fenced writer reported %q, want %q: it must not declare a verdict State owns", got, space.TurnUnknown)
	}
	if got := runStatus(t, ms, turn.RunID); got == "failed" {
		t.Fatalf("the fenced writer closed the run %q; it must leave the run row to the new owner", got)
	}
}

// Control for the fix above: an append failure that is NOT a fence - a transport death with
// no contracts code - is a genuine durable stream hole and still ends the turn `failed`.
// Without this the fenced-writer fix could silently swallow real durability failures.
func TestDurableHoleStillReportsFailed(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	core := &leaseLossCore{first: make(chan struct{}), proceed: make(chan struct{})}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        core,
		WorkerID:    "the-owner",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "durable-hole-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"go"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	select {
	case <-core.first:
	case <-time.After(5 * time.Second):
		t.Fatal("core never appended its first step")
	}

	// The lease stays held; the NEXT append dies in transport (no code) - a real hole.
	ms.armFailedAppend(1, func(key, kind string) bool { return kind == string(space.StepModel) })
	close(core.proceed)

	if got := awaitTurnSettled(t, coord, accept.TurnID); got != space.TurnFailed {
		t.Fatalf("a genuine durable hole reported %q, want %q", got, space.TurnFailed)
	}
}
