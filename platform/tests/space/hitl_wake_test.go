package space_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_wake_test.go — an answer that arrives BEFORE the run is parked wakes nothing, and the run is
// parked afterwards anyway: a durable "answered" sitting next to a run nobody will ever release.
//
// The order in SubmitHITL is: record the gate (Enqueue key "hitl:<id>", kind hitl_request), then park
// the run (Suspend with wait_for="hitl_response"). State wakes a run only while INSERTING an inbox
// event whose kind is what that run waits for (internal/state/events.go Enqueue), and Suspend checks
// nothing (internal/state/store.go Suspend parks unconditionally). So an answer that lands between
// those two calls is inserted while the run is still running - it wakes nothing - and the park that
// follows then waits for an event that has already been and gone. With no deadline on the gate the run
// is parked forever, with no error anywhere and its answer unacknowledged in the thread's inbox.
//
// This is not a theoretical window: the gate's own durable record IS what publishes its id, so anyone
// who approves by watching the thread's inbox - a policy approver, an approval queue that polls
// threads rather than the stream - answers inside exactly that window. The double below plays that
// responder honestly: the answer is stored through the same key/kind/payload shape RespondHITL uses
// and the server's own wake rule is applied to it (mock_state_test.go, both rules measured against
// the real server on the wire in the previous commit).
//
// What the fix may NOT do is invent a decision: it may only re-signal a decision the durable record
// positively holds, which is what the second test guards from the other side.

// gateApprover builds a live EXECUTING turn and a coordinator, and arms the double so that the gate's
// answer becomes durable the instant the gate itself becomes durable - before the Enqueue that created
// the gate has returned to Space.
func gateApprover(t *testing.T, approve bool) (*space.Coordinator, *mockState, string, string, int64) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)

	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        &stubCore{blockForever: true},
		WorkerID:    "w-wake",
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	acc, err := coord.AcceptTurn(context.Background(), "tok-s1", scope, space.AcceptTurnRequest{
		Key: "wake-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"needs approval"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "tok-s1", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, coord, acc.TurnID, "w-wake")
	t.Cleanup(func() { _ = coord.CancelTurn(context.Background(), acc.TurnID, "tok-s1") })
	// Armed only now: the run is executing and holds its lease, which is the state SubmitHITL is
	// called in, and the answer must arrive strictly after the gate and strictly before the park.
	ms.mu.Lock()
	ms.autoAnswerGate = func(string, string) *autoAnswer {
		return &autoAnswer{approved: approve, response: json.RawMessage(`{"note":"answered from the inbox"}`)}
	}
	ms.mu.Unlock()
	return coord, ms, acc.TurnID, acc.ThreadID, held.LeaseEpoch
}

// durableGateEvents counts the thread's inbox events of kind hitl_response, split by whether they are
// the gate's own answer key or some other key carrying the same kind.
func durableGateEvents(ms *mockState, hitlID string) (answers, others []string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, it := range ms.inbox {
		if it.Kind != "hitl_response" {
			continue
		}
		switch it.Key {
		case "hitl_response:" + hitlID:
			answers = append(answers, string(it.Payload))
		default:
			others = append(others, it.Key)
		}
	}
	return answers, others
}

// TestAnAnswerRecordedBeforeTheParkMustNotLeaveTheRunParked is the defect: a gate that is already
// answered may not leave its run parked, because nothing will ever wake it.
func TestAnAnswerRecordedBeforeTheParkMustNotLeaveTheRunParked(t *testing.T) {
	coord, ms, turnID, threadID, epoch := gateApprover(t, true)

	gate, err := coord.SubmitHITL(context.Background(), "tok-s1", space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, turnID, "w-wake", epoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	answers, others := durableGateEvents(ms, gate.ID)
	if len(answers) != 1 {
		t.Fatalf("fixture: the thread holds %d answer events for the gate, want exactly 1 (%v)", len(answers), answers)
	}
	if status, waitFor := durableRun(ms, turnID); status != "pending" {
		t.Fatalf("the gate was answered %d event(s) before the run was parked and the run is durably (%q, wait_for=%q) "+
			"with %d further hitl_response events in the inbox (%v): State wakes a run only while INSERTING an event, "+
			"so this run is parked on a signal that has already been and gone, and nothing releases it. The answer is "+
			"sitting unacknowledged in the same inbox.", len(answers), status, waitFor, len(others), others)
	}
	// A run that was released also has to be honest about what it was released by: the gate the caller
	// is handed may not claim to be pending when the thread's record already holds a decision.
	if gate.Status != space.HITLApproved {
		t.Fatalf("SubmitHITL handed the caller status %q for a gate whose decision is already durable (want %q)",
			gate.Status, space.HITLApproved)
	}
	// And the release must not have been paid for with a fabricated decision: exactly one answer event,
	// under the gate's own key, still saying what the approver said.
	if got, _ := durableGateEvents(ms, gate.ID); len(got) != 1 {
		t.Fatalf("the run was released by writing a second decision for the gate: %v", got)
	}
	read, err := coord.GetHITL(context.Background(), "tok-s1", threadID, gate.ID)
	if err != nil {
		t.Fatalf("GetHITL after the release: %v", err)
	}
	if read.Status != space.HITLApproved {
		t.Fatalf("the durable gate read says %q after the release, want %q", read.Status, space.HITLApproved)
	}
}

// TestAnUnansweredGateStillParksAndSendsNoSecondSignal is the control that keeps the remedy honest:
// with nothing in the record to point at, SubmitHITL must park the run exactly as it always did and
// must not manufacture a wake. Re-signalling without positive evidence would be the same invention as
// answering a gate nobody opened.
func TestAnUnansweredGateStillParksAndSendsNoSecondSignal(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil),
		Core:        &stubCore{blockForever: true},
		WorkerID:    "w-nowake",
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	acc, err := coord.AcceptTurn(context.Background(), "tok-s1", scope, space.AcceptTurnRequest{
		Key: "nowake-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"needs approval"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "tok-s1", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, coord, acc.TurnID, "w-nowake")
	t.Cleanup(func() { _ = coord.CancelTurn(context.Background(), acc.TurnID, "w-nowake") })

	gate, err := coord.SubmitHITL(context.Background(), "tok-s1", space.HITLSubmitRequest{
		TurnID: acc.TurnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, acc.TurnID, "w-nowake", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	if status, waitFor := durableRun(ms, acc.TurnID); status != "waiting" || waitFor != "hitl_response" {
		t.Fatalf("an unanswered gate stopped parking the run: durable state (%q, wait_for=%q), want parked on the gate",
			status, waitFor)
	}
	if answers, others := durableGateEvents(ms, gate.ID); len(answers) != 0 || len(others) != 0 {
		t.Fatalf("a gate nobody answered produced wake traffic: answers=%v others=%v", answers, others)
	}
	if gate.Status != space.HITLPending {
		t.Fatalf("an unanswered gate reads %q to its creator, want %q", gate.Status, space.HITLPending)
	}
	// The ordinary path still works after all of this: the answer that arrives later, after the park,
	// releases the run - which is the case the platform already handles and the fix must not disturb.
	if err := coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: gate.ID, Approved: true, Response: json.RawMessage(`{"note":"later"}`),
	}, acc.ThreadID); err != nil {
		t.Fatalf("RespondHITL after the park: %v", err)
	}
	if status, _ := durableRun(ms, acc.TurnID); status != "pending" {
		t.Fatalf("answering after the park no longer releases the run: durable status %q", status)
	}
}
