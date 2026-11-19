//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Real PostgreSQL, State API, mTLS and fenced writes; the native runtime and
// Identity delegation are controlled seams. This is not an LLM or deployed proof.
type agentExecutorPeer struct {
	mu                   sync.Mutex
	linked               bool
	sends, reads         int
	readErr              error
	human                bool
	answered             bool
	continuation         string
	staleContinuationACK bool
}

func (p *agentExecutorPeer) Send(_ context.Context, token, installation string, input json.RawMessage, key string) (*space.AgentTaskSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sends++
	if key != "" {
		if !p.human || installation != "installed" || !strings.Contains(string(input), "approved") {
			return nil, contracts.NewError(contracts.ErrInvalid, "wrong native continuation")
		}
		p.answered, p.continuation = true, key
		if p.staleContinuationACK {
			if p.sends > 3 {
				return nil, contracts.NewError(contracts.ErrInvalid, "continuation repeatedly resent instead of observed")
			}
			return executorQuestion(), nil
		}
		return executorTask("completed"), nil
	}
	if installation != "installed" || token == "" || string(input) != `"hello native"` || key != "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "wrong native dispatch")
	}
	p.linked = true
	return executorTask("working"), nil
}
func (p *agentExecutorPeer) Read(context.Context, string, string) (*space.AgentTaskSnapshot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	if p.readErr != nil {
		return nil, p.readErr
	}
	if !p.linked {
		return nil, contracts.NewReasoned(contracts.ErrNotFound, "AGENT_TASK_UNCONFIRMED", "not confirmed")
	}
	if p.human && !p.answered {
		return executorQuestion(), nil
	}
	return executorTask("completed"), nil
}

func TestAddressedAgentHITLParksAndNewCoordinatorResumesSameTask_RealState(t *testing.T) {
	for _, stale := range []bool{false, true} {
		t.Run(map[bool]string{false: "immediate-result", true: "continuation-ack-retains-old-question"}[stale], func(t *testing.T) {
			runAddressedAgentHITL(t, stale)
		})
	}
}

func executorQuestion() *space.AgentTaskSnapshot {
	task := executorTask("input-required")
	task.Status.Message = json.RawMessage(`{"kind":"message","role":"agent","messageId":"question-1","parts":[{"kind":"text","text":"Proceed?"}]}`)
	return task
}

func runAddressedAgentHITL(t *testing.T, stale bool) {
	e := cNewEnv(t)
	peer := &agentExecutorPeer{human: true, staleContinuationACK: stale}
	core := &agentExecutorCore{}
	newCoordinator := func() *space.Coordinator {
		return space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core, WorkerID: "native-hitl", AgentPinner: &agentAcceptancePin{e: e}, AgentControl: peer, RequireRunGrant: true, RunGrantIssuer: agentAcceptanceIssuer{e}, TurnDispatchTimeout: -1})
	}
	c := newCoordinator()
	parent := e.signTask(t, "parent")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	accepted, err := c.AcceptTurn(space.WithAuthorityTask(e.ctx, "parent"), parent, scope, space.AcceptTurnRequest{Key: "human", ContextID: "ctx-1", Input: json.RawMessage(`"hello native"`), AgentInstallationID: "installed"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.DispatchTurn(e.ctx, parent, scope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	status := ""
	for time.Now().Before(deadline) {
		r, err := e.client.GetRun(e.ctx, parent, accepted.TurnID)
		if err != nil {
			t.Fatal(err)
		}
		status = r.Status
		if status == "waiting" || status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "waiting" {
		t.Fatalf("native input-required was not durably parked: %s", status)
	}
	inbox, err := e.client.ReadInbox(e.ctx, parent, accepted.ThreadID, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	gateID := ""
	for _, ev := range inbox {
		if ev.Kind == "hitl_request" {
			var gate space.HITLRequest
			if err = json.Unmarshal(ev.Payload, &gate); err != nil {
				t.Fatal(err)
			}
			if gateID != "" {
				t.Fatal("duplicate native gate")
			}
			gateID = gate.ID
		}
	}
	if gateID == "" {
		t.Fatal("no durable native question")
	}
	c = newCoordinator()
	gate, err := c.GetHITL(e.ctx, parent, accepted.ThreadID, gateID)
	if err != nil || gate.Status != space.HITLPending {
		t.Fatal("cold coordinator lost native gate", err)
	}
	if err = c.RespondHITL(e.ctx, parent, space.HITLRespondRequest{HITLID: gateID, Approved: true, Response: json.RawMessage(`"approved"`)}, accepted.ThreadID); err != nil {
		t.Fatal(err)
	}
	if err = c.DispatchTurn(e.ctx, parent, scope); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		r, err := e.client.GetRun(e.ctx, parent, accepted.TurnID)
		if err != nil {
			t.Fatal(err)
		}
		status = r.Status
		if status == "done" || status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if status != "done" {
		t.Fatalf("native continuation did not complete: %s", status)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.sends != 2 || peer.continuation == "" || core.calls.Load() != 0 {
		t.Fatalf("native work restarted instead of continued: sends=%d key=%s core=%d", peer.sends, peer.continuation, core.calls.Load())
	}
}

func TestAddressedAgentHumanWaitStopsHeartbeatBeforeLeaseRelease_RealState(t *testing.T) {
	e := cNewEnv(t)
	var parked atomic.Bool
	var lateHeartbeats atomic.Int32
	parkedSignal := make(chan struct{})
	// The authoritative Suspend commits before its response arrives. Keeping
	// that response in flight across one real heartbeat tick exposes the race.
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/state/Heartbeat" && parked.Load() {
			lateHeartbeats.Add(1)
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, e.srv.URL+r.URL.Path, r.Body)
		if err != nil {
			http.Error(w, "proxy request", 500)
			return
		}
		req.Header = r.Header.Clone()
		response, err := e.srv.Client().Do(req)
		if err != nil {
			http.Error(w, "proxy dependency", 503)
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			http.Error(w, "proxy read", 503)
			return
		}
		if r.URL.Path == "/v1/state/Suspend" && response.StatusCode == 200 {
			parked.Store(true)
			close(parkedSignal)
			timer := time.NewTimer(11 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
	}))
	defer proxy.Close()
	peer := &agentExecutorPeer{human: true}
	c := space.NewCoordinator(space.CoordinatorConfig{StateClient: space.NewStateClient(proxy.URL, proxy.Client()), WorkerID: "native-hb", AgentPinner: &agentAcceptancePin{e: e}, AgentControl: peer, RequireRunGrant: true, RunGrantIssuer: agentAcceptanceIssuer{e}, TurnDispatchTimeout: -1})
	parent := e.signTask(t, "parent")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	accepted, err := c.AcceptTurn(space.WithAuthorityTask(e.ctx, "parent"), parent, scope, space.AcceptTurnRequest{Key: "hb", ContextID: "ctx-1", Input: json.RawMessage(`"hello native"`), AgentInstallationID: "installed"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.DispatchTurn(e.ctx, parent, scope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parkedSignal:
	case <-time.After(4 * time.Second):
		t.Fatal("native run did not park")
	}
	time.Sleep(11200 * time.Millisecond)
	if lateHeartbeats.Load() != 0 {
		t.Fatalf("observer heartbeated a released lease %d time(s), cancelling its own HITL transition", lateHeartbeats.Load())
	}
	r, err := e.client.GetRun(e.ctx, parent, accepted.TurnID)
	if err != nil || r.Status != "waiting" {
		t.Fatal("native wait lost durable parked state", err)
	}
}
func (p *agentExecutorPeer) Cancel(context.Context, string, string) (*space.AgentTaskSnapshot, error) {
	return executorTask("canceled"), nil
}
func executorTask(state string) *space.AgentTaskSnapshot {
	t := &space.AgentTaskSnapshot{ID: "opaque-native", ContextID: "native-conversation"}
	t.Status.State = state
	if state == "completed" {
		t.Artifacts = []json.RawMessage{json.RawMessage(`{"artifactId":"answer","parts":[{"kind":"text","text":"NATIVE-RESULT"}]}`)}
	}
	return t
}

type agentExecutorCore struct{ calls atomic.Int32 }

func (c *agentExecutorCore) Run(context.Context, *space.CoreRequest, chan<- space.StepResult) error {
	c.calls.Add(1)
	return nil
}

func TestAddressedAgentExecutorCompletesAndResumesWithoutDotCore_RealState(t *testing.T) {
	for _, cold := range []bool{false, true} {
		t.Run(map[bool]string{false: "first-dispatch", true: "cold-existing-remote-task"}[cold], func(t *testing.T) {
			e := cNewEnv(t)
			core := &agentExecutorCore{}
			peer := &agentExecutorPeer{linked: cold}
			newCoordinator := func() *space.Coordinator {
				return space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core, WorkerID: "native-observer", AgentPinner: &agentAcceptancePin{e: e}, AgentControl: peer, RequireRunGrant: true, RunGrantIssuer: agentAcceptanceIssuer{e}, TurnDispatchTimeout: -1})
			}
			c := newCoordinator()
			parent := e.signTask(t, "parent")
			scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
			accepted, err := c.AcceptTurn(space.WithAuthorityTask(e.ctx, "parent"), parent, scope, space.AcceptTurnRequest{Key: "native", ContextID: "ctx-1", Input: json.RawMessage(`"hello native"`), AgentInstallationID: "installed", MaxSteps: 8})
			if err != nil {
				t.Fatal(err)
			}
			if cold {
				c = newCoordinator()
			}
			if err = c.DispatchTurn(e.ctx, parent, scope); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(4 * time.Second)
			status := ""
			for time.Now().Before(deadline) {
				r, err := e.client.GetRun(e.ctx, parent, accepted.TurnID)
				if err != nil {
					t.Fatal(err)
				}
				status = r.Status
				if status == "done" || status == "failed" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if status != "done" {
				t.Fatalf("addressed agent did not complete: %s", status)
			}
			if core.calls.Load() != 0 {
				t.Fatal("addressed agent ran Dot Core")
			}
			peer.mu.Lock()
			sends, reads := peer.sends, peer.reads
			peer.mu.Unlock()
			wantSend := 1
			if cold {
				wantSend = 0
			}
			if sends != wantSend || reads == 0 {
				t.Fatalf("replayed native execution: sends=%d reads=%d", sends, reads)
			}
			events, err := e.client.ReadStream(e.ctx, parent, accepted.ThreadID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			models, terminals := 0, 0
			for _, ev := range events {
				if ev.Kind == "model" && strings.Contains(string(ev.Payload), "NATIVE-RESULT") {
					models++
				}
				if ev.Kind == "status" && strings.Contains(string(ev.Payload), `"done"`) {
					terminals++
				}
			}
			if models != 1 || terminals != 1 {
				t.Fatalf("native result/terminal not durable once: %d/%d", models, terminals)
			}
			cps, err := e.client.ListCheckpoints(e.ctx, parent, accepted.ThreadID, space.CheckpointNamespace, 10)
			if err != nil || len(cps) != 0 {
				t.Fatal("Coordinator wrote agent workflow checkpoints", err)
			}
		})
	}
}

func TestAddressedAgentExecutorNeverRestartsKnownMissingTask_RealState(t *testing.T) {
	e := cNewEnv(t)
	peer := &agentExecutorPeer{linked: true, readErr: contracts.NewError(contracts.ErrNotFound, "known remote task lost")}
	c := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, WorkerID: "observer", AgentPinner: &agentAcceptancePin{e: e}, AgentControl: peer, RequireRunGrant: true, RunGrantIssuer: agentAcceptanceIssuer{e}, TurnDispatchTimeout: -1})
	parent := e.signTask(t, "parent")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	accepted, err := c.AcceptTurn(space.WithAuthorityTask(e.ctx, "parent"), parent, scope, space.AcceptTurnRequest{Key: "missing", ContextID: "ctx-1", Input: json.RawMessage(`"hello native"`), AgentInstallationID: "installed"})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.DispatchTurn(e.ctx, parent, scope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		r, err := e.client.GetRun(e.ctx, parent, accepted.TurnID)
		if err != nil {
			t.Fatal(err)
		}
		if r.Status == "failed" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.sends != 0 || peer.reads == 0 {
		t.Fatalf("missing known task was restarted or not inspected: sends=%d reads=%d", peer.sends, peer.reads)
	}
}
