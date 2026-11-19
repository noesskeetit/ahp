package space_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestAcceptTurn(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	req := space.AcceptTurnRequest{
		Key:       "test-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"hello world"`),
		MaxSteps:  8,
	}

	resp, err := coord.AcceptTurn(context.Background(), "token", scope, req)
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if resp.TurnID == "" {
		t.Fatal("expected non-empty TurnID")
	}
	if resp.ThreadID == "" {
		t.Fatal("expected non-empty ThreadID")
	}
	if resp.Status != space.TurnPending {
		t.Fatalf("expected pending status, got %s", resp.Status)
	}
}

func TestDispatchAndComplete(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	coreDone := make(chan struct{})
	mock := &stubCore{done: coreDone}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "dispatch-key-" + contracts.NewID(),
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
	case <-coreDone:
	case <-time.After(5 * time.Second):
		t.Fatal("core did not complete within 5s")
	}

	// Give executeTurn goroutine time to finish releasing
	time.Sleep(200 * time.Millisecond)

	turn := getTurnHelper(coord, accept.TurnID)
	if turn == nil {
		t.Fatal("turn not found after dispatch")
	}
	if turn.Status != space.TurnDone {
		t.Fatalf("expected done, got %s (steps=%d)", turn.Status, turn.Steps)
	}

	// The completed turn must have written its steps to the thread stream so
	// readers can follow execution progress.
	events, err := sc.ReadStream(context.Background(), "token", accept.ThreadID, 0, 100)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("expected stream events for the completed turn")
	}
	var sawModel, sawStatus bool
	for _, ev := range events {
		if ev.ThreadID != accept.ThreadID {
			t.Fatalf("stream event bound to %q, want thread %q", ev.ThreadID, accept.ThreadID)
		}
		switch ev.Kind {
		case string(space.StepModel):
			sawModel = true
		case "status":
			sawStatus = true
		}
	}
	if !sawModel || !sawStatus {
		t.Fatalf("stream missing markers: model=%v status=%v", sawModel, sawStatus)
	}
}

func TestBoundedSteps(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{stepsBeforeDone: 100} // will exceed MaxSteps of 3
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	_, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "bounded-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"loop"`),
		MaxSteps:  3,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	time.Sleep(1 * time.Second)

	ms.mu.Lock()
	// Check that the run was released (no active lease)
	activeLeases := len(ms.leases)
	ms.mu.Unlock()
	if activeLeases != 0 {
		t.Fatalf("expected 0 active leases after turn completion, got %d", activeLeases)
	}
}

func TestCancelTurn(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{blockForever: true}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "cancel-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"cancel me"`),
		MaxSteps:  64,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}

	// Wait for core to start
	time.Sleep(200 * time.Millisecond)

	if err := coord.CancelTurn(context.Background(), accept.TurnID, "caller-state-token"); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	turn := getTurnHelper(coord, accept.TurnID)
	if turn == nil {
		t.Fatal("turn not found")
	}
	if turn.Status != space.TurnCanceled {
		t.Fatalf("expected canceled, got %s", turn.Status)
	}
}

func TestHITLFlow(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	// The run must be genuinely executing for a HITL gate to interrupt it, and the gate's
	// Suspend is only honoured by State for the lease holder - so this flow claims the run
	// first and presents the fence State issued, never a hand-written one.
	mock := &stubCore{blockForever: true}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "hitl-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"needs approval"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, coord, accept.TurnID, "test-worker")
	t.Cleanup(func() {
		// Do not leave a Core parked forever when the assertions fail early.
		_ = coord.CancelTurn(context.Background(), accept.TurnID, "token")
	})

	// Submit HITL
	hitlReq := space.HITLSubmitRequest{
		TurnID: accept.TurnID,
		Kind:   "approval",
		Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}
	deadline := time.Now().Add(5 * time.Minute)
	hitlReq.Deadline = &deadline

	hitl, err := coord.SubmitHITL(context.Background(), "token", hitlReq, accept.TurnID, "test-worker", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	if hitl.ID == "" {
		t.Fatal("expected non-empty HITL ID")
	}
	if hitl.Status != space.HITLPending {
		t.Fatalf("expected pending, got %s", hitl.Status)
	}

	// Check that the turn is awaiting
	turn := getTurnHelper(coord, accept.TurnID)
	if turn != nil && turn.Status != space.TurnAwaiting {
		t.Fatalf("expected awaiting_hitl, got %s", turn.Status)
	}

	// Verify inbox recorded the HITL request
	ms.mu.Lock()
	found := false
	for _, item := range ms.inbox {
		if item.Kind == "hitl_request" {
			found = true
			break
		}
	}
	ms.mu.Unlock()
	if !found {
		t.Fatal("HITL request not found in inbox")
	}
}

func TestSteering(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "steer-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"steer"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Need the turn to be in running state for steering
	// Force status to running for test
	setTurnStatus(coord, accept.TurnID, space.TurnRunning)

	err = coord.Steer(context.Background(), "token", space.SteerRequest{
		TurnID:      accept.TurnID,
		Instruction: "use a different approach",
	})
	if err != nil {
		t.Fatalf("Steer: %v", err)
	}

	// Verify inbox has the steering event
	ms.mu.Lock()
	found := false
	for _, item := range ms.inbox {
		if item.Kind == "steering" {
			found = true
			break
		}
	}
	ms.mu.Unlock()
	if !found {
		t.Fatal("steering event not found in inbox")
	}
}

func TestOutboxEmit(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "outbox-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"emit"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	entry, err := coord.EmitOutbox(context.Background(), "token", space.OutboxEmitRequest{
		TurnID:  accept.TurnID,
		Key:     "delivery-1",
		Kind:    "notification",
		Payload: json.RawMessage(`{"msg":"done"}`),
	}, accept.ThreadID, "w1", 1)
	if err != nil {
		t.Fatalf("EmitOutbox: %v", err)
	}
	if entry.ID == "" {
		t.Fatal("expected non-empty entry ID")
	}

	ms.mu.Lock()
	outboxFound := false
	for _, item := range ms.inbox {
		if item.Kind == "outbox" && item.Key == "outbox:delivery-1" {
			outboxFound = true
			break
		}
	}
	ms.mu.Unlock()
	if !outboxFound {
		t.Fatal("outbox entry not found in inbox")
	}
}

func TestRecovery(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	// Create turns in different states
	a1, _ := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "rec-1-" + contracts.NewID(), ContextID: "ctx-1-" + contracts.NewID(),
		Input: json.RawMessage(`"a"`),
	})
	a2, _ := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "rec-2-" + contracts.NewID(), ContextID: "ctx-2-" + contracts.NewID(),
		Input: json.RawMessage(`"b"`),
	})

	// Simulate: turn 2 was running when we crashed
	setTurnStatus(coord, a2.TurnID, space.TurnRunning)

	result, err := coord.Recover(context.Background(), "token")
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	// Pending turns are recoverable
	found := false
	for _, t := range result.Recovered {
		if t.ID == a1.TurnID {
			found = true
		}
	}
	if !found {
		t.Fatal("expected pending turn to be recovered")
	}

	// Running turns become unknown
	foundUnknown := false
	for _, t := range result.Unknown {
		if t.ID == a2.TurnID {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatal("expected running turn to be marked unknown")
	}
}

func TestCheckpointSave(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	// The snapshot writer is the process that HOLDS the run's lease, so this test
	// must actually claim and execute the run before it may write one. A Core that
	// blocks keeps the lease live for the duration of the assertion.
	mock := &stubCore{blockForever: true}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "ckpt-key-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"checkpoint me"`),
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// Before any lease exists there is no writer, and the refusal carries State's own
	// fence code instead of an internal error.
	if _, err := coord.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID: accept.TurnID, Sequence: 1,
	}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("SaveCheckpoint before the lease is held: code=%q err=%v, want STALE_EPOCH", contracts.Code(err), err)
	}

	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	waitLeaseHeld(t, coord, accept.TurnID, "test-worker")

	resp, err := coord.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID:   accept.TurnID,
		Sequence: 1,
		State:    json.RawMessage(`{"step":1}`),
		Messages: json.RawMessage(`[{"role":"user","content":"hi"}]`),
	})
	if err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if resp.Checkpoint.ID == "" {
		t.Fatal("expected non-empty checkpoint ID")
	}
	if resp.Checkpoint.Sequence != 1 {
		t.Fatalf("expected sequence 1, got %d", resp.Checkpoint.Sequence)
	}

	if err := coord.CancelTurn(context.Background(), accept.TurnID, "token"); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
}

func TestIdempotentAcceptTurn(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)
	mock := &stubCore{}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        mock,
		WorkerID:    "test-worker",
	})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctxID := "ctx-idem-" + contracts.NewID()
	key := "idem-key-" + contracts.NewID()

	// Same context_id = same thread (mock behavior)
	r1, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: key, ContextID: ctxID, Input: json.RawMessage(`"first"`),
	})
	if err != nil {
		t.Fatalf("first AcceptTurn: %v", err)
	}

	r2, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: key, ContextID: ctxID, Input: json.RawMessage(`"second"`),
	})
	if err != nil {
		t.Fatalf("second AcceptTurn: %v", err)
	}

	// Same thread because same context_id
	if r1.ThreadID != r2.ThreadID {
		t.Fatalf("expected same thread, got %s and %s", r1.ThreadID, r2.ThreadID)
	}
}

// --- helpers ---

// stubCore implements space.CoreRunner for testing.
type stubCore struct {
	done            chan struct{}
	stepsBeforeDone int
	blockForever    bool
	capture         chan<- *space.CoreRequest
	mu              sync.Mutex
	calls           int
}

func (s *stubCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	if s.capture != nil {
		reqCopy := *req
		select {
		case s.capture <- &reqCopy:
		default:
		}
	}

	if s.blockForever {
		<-ctx.Done()
		results <- space.StepResult{StepType: space.StepError, Done: true}
		return ctx.Err()
	}

	steps := s.stepsBeforeDone
	if steps == 0 {
		steps = 1
	}

	for i := 0; i < steps; i++ {
		select {
		case <-ctx.Done():
			results <- space.StepResult{StepType: space.StepError, Done: true}
			return ctx.Err()
		default:
		}
		results <- space.StepResult{
			StepType: space.StepModel,
			Content:  json.RawMessage(`"thinking"`),
		}
	}
	results <- space.StepResult{StepType: space.StepDone, Done: true}
	if s.done != nil {
		close(s.done)
	}
	return nil
}

func getTurnHelper(coord *space.Coordinator, turnID string) *space.Turn {
	t, ok := coord.TurnSnapshot(turnID)
	if !ok {
		return nil
	}
	return &t
}

// setTurnStatus rehydrates a turn's status through the coordinator's exported
// rehydration entry point — the same path a restart uses to reload persisted
// turns from State before running Recover classification.
func setTurnStatus(coord *space.Coordinator, turnID string, status space.TurnStatus) {
	coord.RehydrateTurn(space.Turn{ID: turnID, Status: status})
}

// TestColdDispatchResumesFromCheckpoint exercises the restart/resume path: a
// snapshot is persisted by the coordinator process that OWNS the run's lease, that
// process then stops owning it, and a second coordinator (fresh in-memory state)
// claims the run and must hand the persisted message history to Core instead of
// replaying the turn.
func TestColdDispatchResumesFromCheckpoint(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()

	sc := space.NewStateClient(srv.URL, nil)

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	coord1 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{blockForever: true}, WorkerID: "w1",
	})
	accept, err := coord1.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key:       "resume-" + contracts.NewID(),
		ContextID: "ctx-" + contracts.NewID(),
		Input:     json.RawMessage(`"original input"`),
		MaxSteps:  4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}

	// The snapshot is written by the lease owner, so process 1 must claim and execute
	// the run before a snapshot of it is legal.
	if err := coord1.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	waitLeaseHeld(t, coord1, accept.TurnID, "w1")

	cpMsgs := json.RawMessage(`[{"role":"user","content":"original input"},{"role":"assistant","content":"partial"}]`)
	if _, err := coord1.SaveCheckpoint(context.Background(), "token", space.CheckpointRequest{
		TurnID:   accept.TurnID,
		Sequence: 1,
		Messages: cpMsgs,
	}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	// Process 1 loses the run: it stops executing, and the durable outcome a takeover
	// worker sees is a run back in the queue with no lease.
	if err := coord1.CancelTurn(context.Background(), accept.TurnID, "token"); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}
	ms.crashRun(accept.TurnID)

	// Coordinator #2 is the post-restart process: empty in-memory turns.
	captured := make(chan *space.CoreRequest, 1)
	coord2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc,
		Core:        &stubCore{capture: captured},
		WorkerID:    "w2",
	})
	if err := coord2.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("cold DispatchTurn: %v", err)
	}

	var req *space.CoreRequest
	select {
	case req = <-captured:
	case <-time.After(5 * time.Second):
		t.Fatal("core did not run on cold dispatch")
	}

	if req.Checkpoint == nil {
		t.Fatal("expected resume to attach a checkpoint to the core request")
	}
	var got, want any
	if err := json.Unmarshal(req.Messages, &got); err != nil {
		t.Fatalf("unmarshal resumed messages: %v", err)
	}
	if err := json.Unmarshal(cpMsgs, &want); err != nil {
		t.Fatalf("unmarshal expected messages: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resumed messages mismatch:\n got %s\nwant %s", req.Messages, cpMsgs)
	}
}
