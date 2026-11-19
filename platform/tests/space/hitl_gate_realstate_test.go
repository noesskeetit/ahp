//go:build state_integration

package space_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_gate_realstate_test.go — the gate's answer, measured on the REAL State over PostgreSQL.
//
// The double in hitl_gate_answer_test.go now mirrors two rules it used to ignore (the inbox wake on
// wait_for, and the canonical {kind,payload} digest behind an existing key), but a double that has been
// brought up to code is still a double. Two claims here need the real server because each one is a
// server behaviour the Space-side fix depends on:
//
//   - the answer really does release the parked run (internal/state/events.go wakes on the event kind),
//     so the new refusal cannot be a guard that quietly stops approvals from working;
//   - a second, DIFFERENT decision under the gate's key is refused by State itself (ErrKeyReuse), which
//     is precisely the refusal Space used to answer over with {"ok":true}. Space must refuse first, name
//     the decision it refused, and leave exactly one durable answer behind.
//
// And the gap pin is measured here too, because the gap is a property of the server's wake rule: an
// answer naming a gate nobody submitted still releases a run parked on a real, unanswered gate.
//
// This file deliberately compiles BEFORE the fix as well as after it, so the real-wire RED leg runs
// exactly this file. What cannot compile pre-fix - reading a gate through an operation that does not
// exist yet - lives in hitl_gate_read_realstate_test.go, and the RED leg sets that file aside rather
// than reporting a build failure as a defect.
func parkedGateRealState(t *testing.T) (*space.Coordinator, *cEnv, string, string, string, string) {
	t.Helper()
	e := cNewEnv(t)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, Core: &stubCore{blockForever: true}, WorkerID: "w-gatereal"})
	hs := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator: coord,
		Auth:        space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
	}))
	t.Cleanup(hs.Close)

	key := "gatereal-" + contracts.NewID()
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": key, "context_id": "ctx-" + key,
		"input": map[string]any{"role": "user", "content": "needs approval"}, "max_steps": 4,
	}, "ada")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(ada) = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn returned no ids: %s", body)
	}
	t.Cleanup(func() {
		_, _ = postSpaceAs(t, hs.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	})
	if c, b := postSpaceAs(t, hs.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coord, turnID, "w-gatereal")

	ownerTok, _, err := realTaskExchanger{e}.Exchange(e.ctx, space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}
	gate, err := coord.SubmitHITL(e.ctx, ownerTok, space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, turnID, "w-gatereal", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL(exec, epoch %d): %v", held.LeaseEpoch, err)
	}
	status, run := e.raw("GetRun", ownerTok, []byte(`{"run_id":"`+turnID+`"}`))
	if status != http.StatusOK || run["status"] != "waiting" || run["wait_for"] != "hitl_response" {
		t.Fatalf("the run is not durably parked on this gate: status=%d run=%v", status, run)
	}
	return coord, e, ownerTok, turnID, threadID, gate.ID
}

// countGateResponses reads the thread's durable inbox and counts the answers recorded for one gate.
func countGateResponses(t *testing.T, e *cEnv, token, threadID, hitlID string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"thread_id": threadID, "after": 0, "limit": 500})
	status, out := e.raw("ReadInbox", token, body)
	if status != http.StatusOK {
		t.Fatalf("ReadInbox = %d (body=%v)", status, out)
	}
	events, _ := out["events"].([]any)
	want := "hitl_response:" + hitlID
	n := 0
	for _, ev := range events {
		m, _ := ev.(map[string]any)
		if m["key"] == want {
			n++
		}
	}
	return n
}

// TestHITLGateAnswerKeepsItsDecisionOnRealState is the real-wire half of the fix.
func TestHITLGateAnswerKeepsItsDecisionOnRealState(t *testing.T) {
	coord, e, ownerTok, turnID, threadID, hitlID := parkedGateRealState(t)

	if err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}, threadID); err != nil {
		t.Fatalf("RespondHITL on a fresh gate: %v", err)
	}
	// The answer must really release the run, or the new guard is a gate that never opens.
	if status, run := e.raw("GetRun", ownerTok, []byte(`{"run_id":"`+turnID+`"}`)); status != http.StatusOK || run["status"] != "pending" {
		t.Fatalf("the accepted answer did not release the parked run: status=%d run=%v", status, run)
	}

	// A second, opposite decision is refused by Space, naming the decision it refused, and it leaves no
	// second answer behind. On the real server this is the refusal Space used to talk over.
	err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: hitlID, Approved: false, Response: json.RawMessage(`{"note":"scratch that"}`),
	}, threadID)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("a second decision was accepted on the real State (err=%v)", err)
	}
	if got := countGateResponses(t, e, ownerTok, threadID, hitlID); got != 1 {
		t.Fatalf("the durable inbox holds %d answers for one gate after a refused re-decision", got)
	}
	// The repeat that IS the same decision stays accepted and still writes nothing new.
	if err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: hitlID, Approved: true, Response: json.RawMessage(`{"note":"go ahead"}`),
	}, threadID); err != nil {
		t.Fatalf("repeat of the identical decision: %v", err)
	}
	if got := countGateResponses(t, e, ownerTok, threadID, hitlID); got != 1 {
		t.Fatalf("an idempotent repeat wrote a second durable answer (%d)", got)
	}

	// What the gate is READABLE by after a restart is the sibling file's claim: it needs the read
	// operation itself, so it cannot be expressed in a tree that lacks it.
}

// TestAForgedGateAnswerReleasesTheParkedRunOnRealState pins the open contract gap on the real server.
// Its PASSING is the evidence: an answer naming a gate nobody submitted is accepted and wakes the run
// parked on a real, unanswered gate, because the wake matches on the event kind. See docs/space.md.
func TestAForgedGateAnswerReleasesTheParkedRunOnRealState(t *testing.T) {
	coord, e, ownerTok, turnID, threadID, _ := parkedGateRealState(t)

	if err := coord.RespondHITL(e.ctx, ownerTok, space.HITLRespondRequest{
		HITLID: "hitl-never-submitted-" + contracts.NewID(), Approved: true,
		Response: json.RawMessage(`{"note":"nobody asked"}`),
	}, threadID); err != nil {
		t.Fatalf("a forged gate id was refused (%v): the gap is closed, so invert this pin deliberately - "+
			"docs/space.md and the forum ask describe when that is correct.", err)
	}
	status, run := e.raw("GetRun", ownerTok, []byte(`{"run_id":"`+turnID+`"}`))
	if status != http.StatusOK || run["status"] != "pending" {
		t.Fatalf("gap pin drifted: the forged answer did not release the parked run (status=%d run=%v)", status, run)
	}
}
