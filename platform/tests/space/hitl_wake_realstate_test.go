//go:build state_integration

package space_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_wake_realstate_test.go — the lost wake, measured on the real server.
//
// State checks already-arrived matching inbox events during Suspend. The first
// test proves this on real PostgreSQL, independently of Space's older re-signal
// repair. An answer that beats the park must not leave a run waiting forever.
//
// The second test drives the product on a real interleaving. The durable plane is not faked in any way:
// a thin front forwards every one of Space's calls to the real State and, once the server has accepted
// the gate event, answers that gate through the SAME server before Space's own call returns. The only
// thing arranged is how fast the approver is - which is the whole defect, because SubmitHITL parks
// after that call returns.

// rawGateAndAnswer stores a gate and its answer on the real server exactly as SubmitHITL and
// RespondHITL write them, and returns nothing: this helper is the responder's side of the interleaving.
func rawGateAndAnswer(t *testing.T, e *cEnv, tok, threadID, gateID string, approved bool) {
	t.Helper()
	gateBody, _ := json.Marshal(map[string]any{
		"thread_id": threadID, "key": "hitl:" + gateID, "kind": "hitl_request",
		"payload": map[string]any{"id": gateID, "turn_id": "", "kind": "approval", "status": "pending"},
	})
	if status, body := e.raw("Enqueue", tok, gateBody); status != http.StatusOK {
		t.Fatalf("Enqueue(gate) = %d (body=%v)", status, body)
	}
	answerBody, _ := json.Marshal(map[string]any{
		"thread_id": threadID, "key": "hitl_response:" + gateID, "kind": "hitl_response",
		"payload": map[string]any{"hitl_id": gateID, "approved": approved, "response": json.RawMessage(`{"note":"answered before the park"}`)},
	})
	if status, body := e.raw("Enqueue", tok, answerBody); status != http.StatusOK {
		t.Fatalf("Enqueue(answer) = %d (body=%v)", status, body)
	}
}

// runStateOnRealState reads the run row off the wire: status and what it is parked on.
func runStateOnRealState(t *testing.T, e *cEnv, tok, runID string) (string, string) {
	t.Helper()
	status, run := e.raw("GetRun", tok, []byte(`{"run_id":"`+runID+`"}`))
	if status != http.StatusOK {
		t.Fatalf("GetRun(%s) = %d (body=%v)", runID, status, run)
	}
	st, _ := run["status"].(string)
	waitFor, _ := run["wait_for"].(string)
	return st, waitFor
}

// unackedAnswers counts the gate's unacknowledged answer events in the thread's durable inbox.
func unackedAnswers(t *testing.T, e *cEnv, tok, threadID, gateID string) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"thread_id": threadID, "after": 0, "limit": 500})
	status, out := e.raw("ReadInbox", tok, body)
	if status != http.StatusOK {
		t.Fatalf("ReadInbox = %d (body=%v)", status, out)
	}
	events, _ := out["events"].([]any)
	want := "hitl_response:" + gateID
	n := 0
	for _, ev := range events {
		m, _ := ev.(map[string]any)
		if m["key"] == want {
			n++
		}
	}
	return n
}

// State itself prevents the lost wake, even without a Coordinator re-signal.
func TestAParkAfterDurableAnswerBecomesPendingOnRealState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "run-wake-real")

	// Setting up a running run is not the claim under test, so it goes through the typed client:
	// CreateRun's task_id must match the presenting token, and ClaimRun is a workload/cert-only
	// route that takes no bearer token and answers with the fence and the run's own task token.
	thread, err := e.client.CreateThread(e.ctx, tok, contracts.Scope{}, "ctx-"+contracts.NewID(), "wake-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(e.ctx, tok, thread.ID, "wake-run-"+contracts.NewID(), json.RawMessage(`{"type":"turn"}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Four minutes: State caps a lease TTL at five, and the lease only has to survive until the
	// Suspend below hands it back.
	claim, err := e.client.ClaimRun(e.ctx, "", "w-wakereal", 4*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim == nil || claim.Lease == nil {
		t.Fatalf("ClaimRun gave no lease: %+v", claim)
	}
	if claim.Lease.Run.ID != run.ID {
		t.Fatalf("ClaimRun handed out a lease for another run: %s (want %s)", claim.Lease.Run.ID, run.ID)
	}
	threadID, runID, epoch := thread.ID, run.ID, claim.Lease.Epoch
	// From here on the run has a live lease, which is the state SubmitHITL finds itself in.
	tok = claim.TaskToken

	gateID := "gate-" + contracts.NewID()
	rawGateAndAnswer(t, e, tok, threadID, gateID, true)

	// Fact 1: the answer is durable and the run was not waiting, so it woke nothing - the run is still
	// the claimed, running run it was before the two inserts.
	if st, waitFor := runStateOnRealState(t, e, tok, runID); st != "running" || waitFor != "" {
		t.Fatalf("the answer inserted while the run was executing disturbed it (status=%q, wait_for=%q), want "+
			`("running", ""): the wake-on-insert claim below is then not the rule this server follows`, st, waitFor)
	}
	// The matching durable answer prevents a lost wake when Suspend releases the lease.
	suspendBody, _ := json.Marshal(map[string]any{
		"run_id": runID, "worker": "w-wakereal", "epoch": epoch, "wait_for": "hitl_response",
	})
	if status, out := e.raw("Suspend", tok, suspendBody); status != http.StatusOK {
		t.Fatalf("Suspend = %d (body=%v)", status, out)
	}
	st, waitFor := runStateOnRealState(t, e, tok, runID)
	if st != "pending" || waitFor != "" {
		t.Fatalf("Suspend lost an already durable answer: (%q, wait_for=%q)", st, waitFor)
	}
	if n := unackedAnswers(t, e, tok, threadID, gateID); n != 1 {
		t.Fatalf("the thread's unacked inbox holds %d answers for the gate, want 1: the durable record "+
			"must remain available for the worker that resumes this run", n)
	}
	// Space's older re-signal remains compatible; it does not duplicate the answer.
	wakeBody, _ := json.Marshal(map[string]any{
		"thread_id": threadID, "key": "hitl_response:" + gateID + ":wake", "kind": "hitl_response",
		"payload": map[string]any{"hitl_id": gateID, "approved": true, "resignal_of": "hitl_response:" + gateID},
	})
	if status, out := e.raw("Enqueue", tok, wakeBody); status != http.StatusOK {
		t.Fatalf("Enqueue(re-signal) = %d (body=%v)", status, out)
	}
	if st, waitFor := runStateOnRealState(t, e, tok, runID); st != "pending" || waitFor != "" {
		t.Fatalf("a compatible re-signal disturbed the runnable turn: (%q, wait_for=%q)", st, waitFor)
	}
	// And the gate's own answer is still exactly one event: the re-signal lives under its own key.
	if n := unackedAnswers(t, e, tok, threadID, gateID); n != 1 {
		t.Fatalf("the re-signal wrote %d answer events for one gate, want 1 - the gate's single decision "+
			"event must stay the only thing a reader of that key finds", n)
	}
	resumed, err := e.client.ClaimRun(e.ctx, "", "resumed-wakereal", time.Minute)
	if err != nil || resumed == nil || resumed.Lease == nil || resumed.Lease.Run.ID != runID || resumed.Lease.Epoch <= epoch {
		t.Fatalf("durable answer did not allow the same run to resume with a fresh epoch: %+v, %v", resumed, err)
	}
}

// TestAnAnswerRecordedBeforeTheParkIsRepairedOnRealState is the product half on a real interleaving.
func TestAnAnswerRecordedBeforeTheParkIsRepairedOnRealState(t *testing.T) {
	e := cNewEnv(t)

	// The fast approver. Everything is forwarded to the real State untouched; after the server has
	// accepted a gate event, the approver answers that gate through the same server, and only then does
	// Space see its own Enqueue return. Nothing about the durable plane is imitated here.
	// The handler runs on a server goroutine while this test's goroutine reads the record, so the
	// record of what the approver answered is shared state and is treated as such.
	var answeredMu sync.Mutex
	var answeredGates []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, e.srv.URL+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if authz := r.Header.Get("Authorization"); authz != "" {
			req.Header.Set("Authorization", authz)
		}
		resp, err := e.srv.Client().Do(req)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		upstream, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var in struct {
			ThreadID string          `json:"thread_id"`
			Key      string          `json:"key"`
			Kind     string          `json:"kind"`
			Payload  json.RawMessage `json:"payload"`
		}
		if resp.StatusCode == http.StatusOK && json.Unmarshal(body, &in) == nil && in.Kind == "hitl_request" {
			var gate struct {
				ID string `json:"id"`
			}
			if json.Unmarshal(in.Payload, &gate) == nil && gate.ID != "" {
				answerBody, _ := json.Marshal(map[string]any{
					"thread_id": in.ThreadID, "key": "hitl_response:" + gate.ID, "kind": "hitl_response",
					"payload": map[string]any{"hitl_id": gate.ID, "approved": true,
						"response": json.RawMessage(`{"note":"answered before the park"}`)},
				})
				aReq, _ := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/state/Enqueue", bytes.NewReader(answerBody))
				aReq.Header.Set("Content-Type", "application/json")
				if authz := r.Header.Get("Authorization"); authz != "" {
					aReq.Header.Set("Authorization", authz)
				}
				aResp, err := e.srv.Client().Do(aReq)
				if err == nil {
					aResp.Body.Close()
					if aResp.StatusCode == http.StatusOK {
						answeredMu.Lock()
						answeredGates = append(answeredGates, gate.ID)
						answeredMu.Unlock()
					}
				}
			}
		}
		for k, v := range resp.Header {
			for _, one := range v {
				w.Header().Add(k, one)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(upstream)
	}))
	t.Cleanup(proxy.Close)

	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(proxy.URL, nil),
		Core:        &stubCore{blockForever: true},
		WorkerID:    "w-wakereal",
	})
	hs := httptest.NewServer(space.NewHandler(space.HandlerConfig{
		Coordinator: coord,
		Auth:        space.ExchangeMiddleware(realTaskExchanger{e}, authByHeader(e.tenant, e.deployment)),
	}))
	t.Cleanup(hs.Close)

	key := "wakeproxy-" + contracts.NewID()
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
	t.Cleanup(func() {
		_, _ = postSpaceAs(t, hs.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "ada")
	})
	if c, b := postSpaceAs(t, hs.URL, "DispatchTurn", nil, "ada"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}
	held := waitLeaseHeld(t, coord, turnID, "w-wakereal")
	ownerTok, _, err := realTaskExchanger{e}.Exchange(e.ctx, space.HumanPrincipal{Subject: "ada"}, "")
	if err != nil {
		t.Fatalf("mint owner task token: %v", err)
	}

	gate, err := coord.SubmitHITL(e.ctx, ownerTok, space.HITLSubmitRequest{
		TurnID: turnID, Kind: "approval", Prompt: json.RawMessage(`{"question":"Proceed?"}`),
	}, turnID, "w-wakereal", held.LeaseEpoch)
	if err != nil {
		t.Fatalf("SubmitHITL: %v", err)
	}
	answeredMu.Lock()
	answered := append([]string(nil), answeredGates...)
	answeredMu.Unlock()
	if len(answered) != 1 || answered[0] != gate.ID {
		t.Fatalf("the approver answered %v, want exactly the gate under test (%s) - the interleaving below "+
			"is not the one the test claims to measure", answered, gate.ID)
	}

	// The defect: an answer that became durable before the park woke nothing, and the park landed anyway.
	st, waitFor := runStateOnRealState(t, e, ownerTok, turnID)
	if st != "pending" || waitFor != "" {
		t.Fatalf("the gate was durably answered before the run was parked and the run is still (%q, wait_for=%q): "+
			"State wakes only on insertion, so nothing will ever release it while its answer sits unacked in the "+
			"same inbox.", st, waitFor)
	}
	// Released, but not by inventing anything: the gate keeps exactly one decision event.
	if n := unackedAnswers(t, e, ownerTok, threadID, gate.ID); n != 1 {
		t.Fatalf("the run was released with %d answer events for the gate, want exactly 1", n)
	}
	// And the caller is told what the gate actually is, rather than being invited to wait for a decision
	// that had already been taken.
	if gate.Status != space.HITLApproved {
		t.Fatalf("SubmitHITL returned status %q for a gate whose answer was already durable (want %q)",
			gate.Status, space.HITLApproved)
	}
	cold := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: &stubCore{}, WorkerID: "w-wakereal-cold"})
	read, err := cold.GetHITL(e.ctx, ownerTok, threadID, gate.ID)
	if err != nil {
		t.Fatalf("GetHITL after the repair: %v", err)
	}
	if read.Status != space.HITLApproved {
		t.Fatalf("the durable gate read says %q after the repair, want %q", read.Status, space.HITLApproved)
	}
}
