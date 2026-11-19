package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestAgentTurnTargetUsesOwnerPinAndRejectsRedirect(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	calls := 0
	owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/registry/PinAgentTurn" || r.Header.Get("Authorization") != "Bearer own-run" {
			t.Error("wrong owner authority")
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 || body["installation_id"] != "installed" {
			t.Error("caller-controlled runtime identity sent to owner")
		}
		if calls == 2 {
			w.Header().Set("Location", "https://foreign.invalid/credentials")
			w.WriteHeader(307)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"target": map[string]any{"task": "pin", "installation_id": "installed", "agent_id": "agent", "version_id": "version", "digest": digest, "environment": "dev", "config_revision": 7, "manifest": map[string]any{"state": map[string]any{"compat": "v1", "partition": "agent"}}}})
	}))
	defer owner.Close()
	client, err := NewAgentTargetClient(owner.URL, owner.Client())
	if err != nil {
		t.Fatal(err)
	}
	target, err := client.PinAgentTurn(context.Background(), "own-run", "installed")
	if err != nil {
		t.Fatal(err)
	}
	if target.InstallationID != "installed" || target.VersionID != "version" || target.ConfigRevision != 7 || target.StateCompat != "v1" || target.StatePartition != "agent" || target.PinID != "pin" {
		t.Fatalf("invalid target projection: %+v", target)
	}
	if _, err = client.PinAgentTurn(context.Background(), "own-run", "installed"); err == nil {
		t.Fatal("redirect treated as an accepted target")
	}
	if calls != 2 {
		t.Fatalf("redirect retried: %d", calls)
	}
}

func TestRegistryOwnerConstructorsReturnNoClientOnInvalidConfig(t *testing.T) {
	if client, err := NewAgentTargetClient("http://unsafe.invalid", http.DefaultClient); err == nil || client != nil {
		t.Fatal("failed agent constructor returned a usable-looking client")
	}
	if client, err := NewProfileClient("http://unsafe.invalid", http.DefaultClient); err == nil || client != nil {
		t.Fatal("failed Dot constructor returned a usable-looking client")
	}
}

type unexpectedAgentCore struct{ calls atomic.Int64 }

type guardAgentControl struct{ calls atomic.Int64 }

func (g *guardAgentControl) Read(context.Context, string, string) (*AgentTaskSnapshot, error) {
	g.calls.Add(1)
	task := &AgentTaskSnapshot{ID: "native"}
	task.Status.State = "completed"
	return task, nil
}
func (g *guardAgentControl) Send(ctx context.Context, token, installation string, _ json.RawMessage, _ string) (*AgentTaskSnapshot, error) {
	return g.Read(ctx, token, installation)
}
func (g *guardAgentControl) Cancel(ctx context.Context, token, installation string) (*AgentTaskSnapshot, error) {
	return g.Read(ctx, token, installation)
}

func TestColdAddressedAgentCannotUseHostWithoutPerRunGrants(t *testing.T) {
	state := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer state.Close()
	control := &guardAgentControl{}
	c := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(state.URL, state.Client()), AgentControl: control, WorkerID: "worker"})
	turn := &Turn{ID: "run", ThreadID: "thread", Status: TurnRunning, AgentInstallationID: "installed", AgentTarget: &AgentTargetRef{InstallationID: "installed"}}
	lease := &leaseResponse{Run: runResponse{ID: "run", ThreadID: "thread", TaskID: "shared-ingress"}, Thread: threadResponse{ID: "thread"}, Worker: "worker", Epoch: 1}
	c.executeTurn(context.Background(), turn, lease, "shared-authority")
	if control.calls.Load() != 0 || turn.Status != TurnFailed {
		t.Fatalf("cold custom task used ingress authority: calls=%d status=%s", control.calls.Load(), turn.Status)
	}
}

func (c *unexpectedAgentCore) Run(context.Context, *CoreRequest, chan<- StepResult) error {
	c.calls.Add(1)
	return nil
}

func TestUnconfiguredAgentExecutorNeverFallsBackToDotCore(t *testing.T) {
	for _, targetOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "selected-installation", true: "orphan-target"}[targetOnly], func(t *testing.T) {
			// Wire fixture only. It records no real State/LLM acceptance claim.
			state := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{}`))
			}))
			defer state.Close()
			core := &unexpectedAgentCore{}
			c := NewCoordinator(CoordinatorConfig{StateClient: NewStateClient(state.URL, state.Client()), Core: core, WorkerID: "worker"})
			turn := &Turn{ID: "run", ThreadID: "thread", Input: json.RawMessage(`"continue"`), Status: TurnRunning}
			if targetOnly {
				turn.AgentTarget = &AgentTargetRef{InstallationID: "installed"}
			} else {
				turn.AgentInstallationID = "installed"
			}
			lease := &leaseResponse{Run: runResponse{ID: "run", ThreadID: "thread"}, Thread: threadResponse{ID: "thread", ContextID: "conversation"}, Worker: "worker", Epoch: 1}
			c.executeTurn(context.Background(), turn, lease, "task")
			if core.calls.Load() != 0 || turn.Status != TurnFailed || !strings.Contains(turn.FailureReason, "agent executor") {
				t.Fatalf("selected agent silently ran Dot: calls=%d status=%s cause=%s", core.calls.Load(), turn.Status, turn.FailureReason)
			}
		})
	}
}

func TestAgentTurnTargetRefusesWrongOrIncompleteOwnerSnapshot(t *testing.T) {
	for _, tt := range []struct {
		name, body string
		status     int
	}{
		{"wrong installation", `{"target":{"installation_id":"other","agent_id":"agent","version_id":"v","digest":"sha256:` + strings.Repeat("a", 64) + `","task":"pin","environment":"dev"}}`, 200},
		{"missing pin", `{"target":{"installation_id":"installed","agent_id":"agent","version_id":"v"}}`, 200},
		{"unavailable", `{"code":"UNAVAILABLE","reason":"runtime_not_ready"}`, 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer owner.Close()
			client, err := NewAgentTargetClient(owner.URL, owner.Client())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = client.PinAgentTurn(context.Background(), "own-run", "installed"); err == nil {
				t.Fatal("invalid owner snapshot accepted")
			} else if tt.status == 503 && contracts.Code(err) != contracts.ErrUnavailable {
				t.Fatal("owner refusal lost its class", err)
			}
		})
	}
}

func TestConversationExecutorIsImmutableAndInherited(t *testing.T) {
	run := func(installation string) runResponse {
		b, _ := json.Marshal(map[string]any{"type": "turn", "agent_installation_id": installation})
		return runResponse{Payload: b}
	}
	for _, tt := range []struct {
		name            string
		runs            []runResponse
		requested, want string
		conflict        bool
	}{
		{"new custom", nil, "installed", "installed", false},
		{"inherited custom", []runResponse{run("installed")}, "", "installed", false},
		{"same custom", []runResponse{run("installed")}, "installed", "installed", false},
		{"new native", nil, "", "", false},
		{"native cannot become custom", []runResponse{run("")}, "installed", "", true},
		{"custom cannot change target", []runResponse{run("installed")}, "another", "", true},
		{"mixed executor history", []runResponse{run("installed"), run("")}, "", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := conversationExecutorBinding(tt.runs, tt.requested)
			if tt.conflict {
				if contracts.Code(err) != contracts.ErrConflict {
					t.Fatalf("changed executor: %q %v", got, err)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q %v", got, err)
			}
		})
	}
}

func TestColdTurnRetainsSelectedAgentExecutor(t *testing.T) {
	payload := json.RawMessage(`{"type":"turn","input":"continue","agent_installation_id":"installed","agent_target":{"installation_id":"installed","agent_id":"agent","version_id":"version","digest":"sha256:` + strings.Repeat("a", 64) + `","pin_id":"pin","state_compat":"v1","state_partition":"agent"}}`)
	turn := &Turn{}
	restoreTurnFromRunPayload(turn, payload)
	raw, err := json.Marshal(turn)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	if fields["agent_installation_id"] != "installed" || fields["agent_target"] == nil {
		t.Fatal("restart loses the selected runtime", string(raw))
	}
	projected, ok := turnFromRun(runResponse{ID: "run", ThreadID: "thread", Payload: payload, Status: "done"}, contracts.Scope{Tenant: "tenant", Deployment: "dot", Subject: "user"})
	if !ok {
		t.Fatal("durable custom turn missing")
	}
	raw, _ = json.Marshal(projected)
	fields = map[string]any{}
	_ = json.Unmarshal(raw, &fields)
	if fields["agent_installation_id"] != "installed" || fields["agent_target"] == nil {
		t.Fatal("history reload loses the selected runtime", string(raw))
	}
}
