package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type acceptAgentPinner struct {
	target              AgentTargetRef
	token, installation string
	calls               int
}

func (p *acceptAgentPinner) PinAgentTurn(_ context.Context, token, installation string) (*AgentTargetRef, error) {
	p.calls++
	p.token = token
	p.installation = installation
	copy := p.target
	copy.InstallationID = installation
	return &copy, nil
}

type acceptAgentGrant struct{}

func (acceptAgentGrant) Delegate(context.Context, string, string, RunGrantSpec) (string, error) {
	return "child-run-token", nil
}
func (acceptAgentGrant) Cancel(context.Context, string, string) error { return nil }

// A wire fixture for the Coordinator boundary, not a PostgreSQL/LLM proof.
type acceptAgentState struct {
	mu        sync.Mutex
	history   []runResponse
	payload   json.RawMessage
	runWrites int
	binding   json.RawMessage
}

func (s *acceptAgentState) client(t *testing.T) *StateClient {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var in struct {
			Payload json.RawMessage `json:"payload"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/state/GetThread", "/v1/state/CreateThread":
			_, _ = w.Write([]byte(`{"id":"thread","context_id":"conversation"}`))
		case "/v1/state/ListRuns":
			_ = json.NewEncoder(w).Encode(map[string]any{"runs": s.history})
		case "/v1/state/Enqueue":
			if s.binding != nil && string(s.binding) != string(in.Payload) {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrKeyReuse, "binding changed"))
				return
			}
			s.binding = append(json.RawMessage(nil), in.Payload...)
			_, _ = w.Write([]byte(`{"id":"binding","sequence":1}`))
		case "/v1/state/CreateRun":
			s.runWrites++
			s.payload = append(json.RawMessage(nil), in.Payload...)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "run", "thread_id": "thread", "status": "pending", "payload": in.Payload})
		default:
			t.Errorf("unexpected State call %s", r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(server.Close)
	return NewStateClient(server.URL, server.Client())
}

func TestStateClientPreservesKeyReuseReasonWithoutChangingItsClass(t *testing.T) {
	state := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrKeyReuse, "already bound"))
	}))
	defer state.Close()
	_, err := NewStateClient(state.URL, state.Client()).Enqueue(context.Background(), "token", "thread", "key", "executor_binding", json.RawMessage(`{}`))
	if contracts.Code(err) != contracts.ErrInvalid || contracts.ReasonOf(err) != contracts.ErrKeyReuse {
		t.Fatalf("State's structured refusal lost its reason: class=%s reason=%s", contracts.Code(err), contracts.ReasonOf(err))
	}
}

func acceptedAgentRequest(t *testing.T, installation string) AcceptTurnRequest {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"key": "turn-1", "context_id": "conversation", "input": "hello", "agent_installation_id": installation})
	var req AcceptTurnRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestAcceptAgentTurnPinsUnderChildAndStoresExecutor(t *testing.T) {
	state := &acceptAgentState{}
	pinner := &acceptAgentPinner{target: AgentTargetRef{AgentID: "agent", VersionID: "v2", PinID: "pin", StateCompat: "v1", StatePartition: "agent"}}
	c := NewCoordinator(CoordinatorConfig{StateClient: state.client(t), AgentPinner: pinner, AgentControl: &profileClient{}, RunGrantIssuer: acceptAgentGrant{}, RequireRunGrant: true, TurnDispatchTimeout: -1})
	ctx := WithAuthorityTask(context.Background(), "parent")
	result, err := c.AcceptTurn(ctx, "ingress", contracts.Scope{Tenant: "tenant", Deployment: "dot", Subject: "alice"}, acceptedAgentRequest(t, "installed"))
	if err != nil {
		t.Fatal(err)
	}
	if pinner.calls != 1 || pinner.token != "child-run-token" || pinner.installation != "installed" {
		t.Fatal("target was not pinned under this run's child grant")
	}
	var payload turnRunPayload
	if err := json.Unmarshal(state.payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.AgentInstallationID != "installed" || payload.AgentTarget == nil || payload.AgentTarget.VersionID != "v2" || payload.AgentConfig != nil {
		t.Fatalf("lost native executor: %s", state.payload)
	}
	var projected map[string]any
	raw, _ := json.Marshal(result)
	_ = json.Unmarshal(raw, &projected)
	if projected["agent_installation_id"] != "installed" {
		t.Fatal("accept reply lost agent identity")
	}
}

func TestAcceptAgentTurnInheritsInstallationButPinsEachVersion(t *testing.T) {
	for _, incompatible := range []bool{false, true} {
		state := &acceptAgentState{history: []runResponse{{Payload: json.RawMessage(`{"type":"turn","agent_installation_id":"installed","agent_target":{"installation_id":"installed","version_id":"v1","state_compat":"v1","state_partition":"agent"}}`)}}}
		compat := "v1"
		if incompatible {
			compat = "incompatible"
		}
		pinner := &acceptAgentPinner{target: AgentTargetRef{AgentID: "agent", VersionID: "v2", PinID: "next-pin", StateCompat: compat, StatePartition: "agent"}}
		c := NewCoordinator(CoordinatorConfig{StateClient: state.client(t), AgentPinner: pinner, AgentControl: &profileClient{}, RunGrantIssuer: acceptAgentGrant{}, RequireRunGrant: true, TurnDispatchTimeout: -1})
		req := acceptedAgentRequest(t, "")
		req.ThreadID = "thread"
		_, err := c.AcceptTurn(WithAuthorityTask(context.Background(), "parent"), "ingress", contracts.Scope{Tenant: "tenant", Deployment: "dot", Subject: "alice"}, req)
		if incompatible {
			if contracts.Code(err) != contracts.ErrConflict || state.runWrites != 0 {
				t.Fatal("incompatible native state accepted", err)
			}
			continue
		}
		if err != nil || pinner.installation != "installed" || state.runWrites != 1 {
			t.Fatal("cold conversation did not keep its agent", err)
		}
		var payload turnRunPayload
		_ = json.Unmarshal(state.payload, &payload)
		if payload.AgentTarget == nil || payload.AgentTarget.VersionID != "v2" {
			t.Fatal("version frozen for whole conversation instead of each turn")
		}
	}
}

func TestAcceptAgentTurnRequiresConfiguredExecutorAndOwnRunGrant(t *testing.T) {
	for _, cfg := range []CoordinatorConfig{
		{},
		{AgentPinner: &acceptAgentPinner{}, AgentControl: &profileClient{}},
	} {
		state := &acceptAgentState{}
		cfg.StateClient = state.client(t)
		cfg.TurnDispatchTimeout = -1
		c := NewCoordinator(cfg)
		_, err := c.AcceptTurn(context.Background(), "ingress", contracts.Scope{Tenant: "tenant", Deployment: "dot", Subject: "alice"}, acceptedAgentRequest(t, "installed"))
		if err == nil || state.runWrites != 0 {
			t.Fatal("agent turn accepted without configured executor/child authority")
		}
	}
}
