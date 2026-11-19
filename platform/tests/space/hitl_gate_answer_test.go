package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_gate_answer_test.go — a gate that has been answered has to STAY answered, and be readable.
//
// SubmitHITL parks the run durably (status waiting, wait_for="hitl_response") and records the gate in
// the thread's inbox as key "hitl:<id>", kind hitl_request. RespondHITL records the decision as key
// "hitl_response:<id>", kind hitl_response. Both facts are therefore durable, and the durable record
// is the only place they exist: the coordinator keeps no gate table at all.
//
// Two things follow, and both are broken.
//
//  1. A second answer to the same gate is never refused BY SPACE. State keys the response event by the
//     gate, so a DIFFERENT decision under one key is a key reuse with a different digest and State
//     refuses it - yet Space never looks at the gate, so whatever comes back is not an answer about the
//     gate. On the real server State's KEY_REUSE_MISMATCH arrives flattened by the transport to
//     INVALID_ARGUMENT ("key reused with a different body"), which cannot say what the gate decided;
//     and the double this package ran against before this change returned the stored event like an
//     idempotent write, so Space answered {"ok":true} to a decision the durable record contradicts.
//     Both shapes are measured - untagged here, on the wire in hitl_gate_realstate_test.go - and they
//     mean the same thing to whoever holds the approval card: nothing reliable about the gate.
//  2. Nothing can read a gate. There is no read face for HITL, so after any restart an approval card
//     cannot ask whether its gate is still pending or what was decided, even though the thread's inbox
//     says exactly that. This is the same split this lane closed for the terminal frame, the failure
//     cause, the cancel and the parked status: the durable plane knows, the projection says nothing.
//
// What this file does NOT do is invent a gate Space was never told about: the read is built only from
// the two durable events, and the answer instant is NOT reported, because the published inbox contract
// carries no created_at (the same gap turn_history.go refuses to paper over for a turn).

// gateFixture is one parked HITL gate plus the two readers that matter: the runner that still holds
// the turn, and a browser face over a coordinator with an EMPTY memory (the restart).
type gateFixture struct {
	coord      *space.Coordinator
	ms         *mockState
	live, cold *httptest.Server
	turnID     string
	threadID   string
	hitlID     string
}

// parkedGate builds a live EXECUTING turn parked on its own HITL gate through the real code path
// (AcceptTurn -> DispatchTurn -> the executor's SubmitHITL under the fence State issued). Both faces
// sit over the SAME durable State; the cold one is a brand-new Coordinator, which is the only form a
// process restart takes in a test.
func parkedGate(t *testing.T) *gateFixture {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)

	newFace := func(core space.CoreRunner, worker string) (*space.Coordinator, *httptest.Server) {
		c := space.NewCoordinator(space.CoordinatorConfig{
			StateClient: space.NewStateClient(srv.URL, nil), Core: core, WorkerID: worker})
		h := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: c, Auth: authPerSubject}))
		t.Cleanup(h.Close)
		return c, h
	}

	coord, hs := newFace(&stubCore{blockForever: true}, "w-gate")
	_, coldHS := newFace(&stubCore{}, "w-gate-cold")

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	acc, err := coord.AcceptTurn(context.Background(), "tok-s1", scope, space.AcceptTurnRequest{
		Key: "gate-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"needs approval"`), MaxSteps: 4,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "tok-s1", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	held := waitLeaseHeld(t, coord, acc.TurnID, "w-gate")
	t.Cleanup(func() {
		// Do not leave a parked Core and a live lease behind when an assertion fails early.
		_ = coord.CancelTurn(context.Background(), acc.TurnID, "tok-s1")
	})
	hitl, err := coord.SubmitHITL(context.Background(), "tok-s1", space.HITLSubmitRequest{
		TurnID: acc.TurnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, acc.TurnID, "w-gate", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	if hitl.Status != space.HITLPending {
		t.Fatalf("a freshly submitted gate reads %q, want %q", hitl.Status, space.HITLPending)
	}
	// The durable row must say what a parked run says, or the gate below is not the thing under test.
	ms.mu.Lock()
	status, waitFor := ms.runs[acc.TurnID].Status, ms.runs[acc.TurnID].WaitFor
	ms.mu.Unlock()
	if status != "waiting" || waitFor != "hitl_response" {
		t.Fatalf("the run is not durably parked on this gate: (%q, wait_for=%q)", status, waitFor)
	}
	return &gateFixture{coord: coord, ms: ms, live: hs, cold: coldHS,
		turnID: acc.TurnID, threadID: acc.ThreadID, hitlID: hitl.ID}
}

// durableResponseEvents counts the decisions this thread's durable inbox holds for one gate.
func durableResponseEvents(ms *mockState, hitlID string) []string {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	var payloads []string
	for _, it := range ms.inbox {
		if it.Key == "hitl_response:"+hitlID {
			payloads = append(payloads, string(it.Payload))
		}
	}
	return payloads
}

// TestLateAnswerCannotReDecideAHITLGate is defect 1: the first decision is the run's, and a late answer
// with the opposite decision must be refused AS A GATE DECISION, naming what the gate already holds -
// not answered on the caller's behalf by a storage-layer key rejection.
func TestLateAnswerCannotReDecideAHITLGate(t *testing.T) {
	g := parkedGate(t)
	coord, ms, threadID, hitlID := g.coord, g.ms, g.threadID, g.hitlID

	if err := coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}, threadID); err != nil {
		t.Fatalf("answering a fresh gate: %v", err)
	}

	err := coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: hitlID, Approved: false, Response: json.RawMessage(`{"note":"scratch that"}`),
	}, threadID)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("a late answer with the opposite decision got no gate-level refusal (err=%v): Space answers on "+
			"State's behalf with a storage-layer verdict (INVALID_ARGUMENT, from State's key-reuse digest check) "+
			"instead of naming the decision this gate holds, so the caller cannot tell whether the gate says "+
			"approved or rejected - and on a plane that does not reject the second write it says {\"ok\":true}. "+
			"One gate cannot hold two decisions.", err)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "approved") {
		t.Fatalf("the refusal does not name the decision it refused: %v", err)
	}
	if got := durableResponseEvents(ms, hitlID); len(got) != 1 {
		t.Fatalf("the refused second answer still wrote to the durable inbox (%d events: %v)", len(got), got)
	}
}

// TestRepeatHITLAnswerWithTheSameDecisionIsIdempotent is the control that keeps the refusal honest:
// one human decision arriving twice (a retried click, a retried request) is ONE decision, stays
// accepted, and stays one durable event.
func TestRepeatHITLAnswerWithTheSameDecisionIsIdempotent(t *testing.T) {
	g := parkedGate(t)
	coord, ms, threadID, hitlID := g.coord, g.ms, g.threadID, g.hitlID
	answer := space.HITLRespondRequest{
		HITLID: hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}
	if err := coord.RespondHITL(context.Background(), "tok-s1", answer, threadID); err != nil {
		t.Fatalf("first answer: %v", err)
	}
	if err := coord.RespondHITL(context.Background(), "tok-s1", answer, threadID); err != nil {
		t.Fatalf("repeat of the SAME decision = %v, want an idempotent success (Code=%v)", err, contracts.Code(err))
	}
	if got := durableResponseEvents(ms, hitlID); len(got) != 1 {
		t.Fatalf("one decision recorded twice in the durable inbox: %v", got)
	}
}

// TestAnsweredHITLGateIsReadableAfterTheRunnerForgets is defect 2 through the browser face: the reader
// that no longer remembers the gate must be able to ask what it says, because the durable inbox knows.
func TestAnsweredHITLGateIsReadableAfterTheRunnerForgets(t *testing.T) {
	g := parkedGate(t)
	coord, hs, threadID, hitlID := g.coord, g.cold, g.threadID, g.hitlID
	if err := coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}, threadID); err != nil {
		t.Fatalf("RespondHITL: %v", err)
	}
	// hs IS the restart: a coordinator with an empty memory over the same durable State.
	code, body := postSpaceAs(t, hs.URL, "GetHITL", map[string]any{"thread_id": threadID, "hitl_id": hitlID}, "s1")
	if code != http.StatusOK {
		t.Fatalf("GetHITL on a cold reader = %d, want 200 (body=%s). An approval UI cannot ask whether its gate "+
			"was answered, even though the decision is durable in the thread's inbox.", code, body)
	}
	got := decodeMap(t, body)
	if got["status"] != string(space.HITLApproved) {
		t.Fatalf("answered gate reads %v, want %q (body=%s)", got["status"], space.HITLApproved, body)
	}
	// The published gate shape names the gate `id` (that is what SubmitHITL answers with), so the read
	// must answer in the same shape rather than inventing a second name for the same thing.
	if got["id"] != hitlID || got["turn_id"] == "" {
		t.Fatalf("gate read lost its identity: %s", body)
	}
	if resp, ok := got["response"].(map[string]any); !ok || resp["note"] != "go ahead" {
		t.Fatalf("the recorded decision response is missing from the gate read: %s", body)
	}
}

// TestUnansweredHITLGateReadsAsPending pins the other half of the same read: a gate nobody answered
// reads pending, with the prompt it asked for. Without this the read could only ever describe
// settled gates, which is no help to the card that is waiting.
func TestUnansweredHITLGateReadsAsPending(t *testing.T) {
	g := parkedGate(t)
	hs, turnID, threadID, hitlID := g.cold, g.turnID, g.threadID, g.hitlID
	code, body := postSpaceAs(t, hs.URL, "GetHITL", map[string]any{"thread_id": threadID, "hitl_id": hitlID}, "s1")
	if code != http.StatusOK {
		t.Fatalf("GetHITL on an unanswered gate = %d, want 200 (body=%s)", code, body)
	}
	got := decodeMap(t, body)
	if got["status"] != string(space.HITLPending) {
		t.Fatalf("unanswered gate reads %v, want %q (body=%s)", got["status"], space.HITLPending, body)
	}
	if got["turn_id"] != turnID {
		t.Fatalf("gate read names turn %v, want %s (body=%s)", got["turn_id"], turnID, body)
	}

	// And a gate that was never submitted is not readable either - Space does not invent one.
	missing, mbody := postSpaceAs(t, hs.URL, "GetHITL",
		map[string]any{"thread_id": threadID, "hitl_id": "hitl-never-submitted"}, "s1")
	if missing == http.StatusOK {
		t.Fatalf("GetHITL invented a gate nobody submitted: %s", mbody)
	}
}

// durableRun reads the run row the way State holds it.
func durableRun(ms *mockState, runID string) (status, waitFor string) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return ms.runs[runID].Status, ms.runs[runID].WaitFor
}

// TestAnsweringAFreshGateReleasesTheRunParkedOnIt is the control that keeps the new refusal honest:
// the ordinary answer to an ordinary gate must still do what it has always done, which is let the run
// go. A guard that only ever says no would be safer code and a broken product.
func TestAnsweringAFreshGateReleasesTheRunParkedOnIt(t *testing.T) {
	g := parkedGate(t)
	if status, waitFor := durableRun(g.ms, g.turnID); status != "waiting" || waitFor != "hitl_response" {
		t.Fatalf("fixture: run is (%q, wait_for=%q), want parked on the gate", status, waitFor)
	}
	if err := g.coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: g.hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}, g.threadID); err != nil {
		t.Fatalf("RespondHITL on a fresh gate: %v", err)
	}
	if status, _ := durableRun(g.ms, g.turnID); status != "pending" {
		t.Fatalf("the answer was accepted but the parked run was not released: durable status %q. The whole point of "+
			"the gate is that the answer resumes the run.", status)
	}
}

// TestAForgedGateAnswerReleasesTheRunParkedOnARealGate pins a KNOWN GAP, and its passing is the
// evidence rather than an endorsement.
//
// State's wake matches on the event KIND against the run's own wait_for (internal/state/events.go), and
// every HITL answer in Space carries the same kind. So an answer naming a gate nobody submitted is
// accepted and releases a run parked on a DIFFERENT, still-unanswered gate: the run resumes as if a
// human had decided, on a decision that was never made. Space cannot close this inside its own zone -
// a fabricated id leaves no durable record to contradict, and the published ReadInbox hides acknowledged
// events, so "no such gate" is not something Space can prove. The remedies are a contract decision:
// park each run on its own gate (per-gate wait target, which changes the event kind another runtime
// filters on), or let State answer an acked-inclusive inbox read. Asked on the forum; until then this
// test states the hole in the only language that cannot drift.
func TestAForgedGateAnswerReleasesTheRunParkedOnARealGate(t *testing.T) {
	g := parkedGate(t)
	forged := "hitl-forged-" + contracts.NewID()

	if err := g.coord.RespondHITL(context.Background(), "tok-s1", space.HITLRespondRequest{
		HITLID: forged, Approved: true, Response: json.RawMessage(`{"note":"nobody asked"}`),
	}, g.threadID); err != nil {
		t.Fatalf("a forged gate id was refused (%v): the gap below is closed, so this pin must be inverted to "+
			"require the refusal - see the comment above before changing anything.", err)
	}
	status, waitFor := durableRun(g.ms, g.turnID)
	if status != "pending" || waitFor != "" {
		t.Fatalf("gap pin drifted: the forged answer did not release the run parked on the real gate (%q, wait_for=%q)",
			status, waitFor)
	}
}
