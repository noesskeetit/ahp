package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func callAgentTurnPin(t *testing.T, h *HTTPHandler, scope contracts.Scope, task, conversation string, body any) (int, map[string]any) {
	t.Helper()
	fn, ok := h.routes["PinAgentTurn"]
	if !ok {
		t.Fatal("PinAgentTurn is missing: a Coordinator cannot pin its selected executor")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/registry/PinAgentTurn", bytes.NewReader(raw))
	claims := auth.Claims{Kind: auth.KindTask, Task: task, Context: conversation}
	r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
	w := httptest.NewRecorder()
	fn(w, r)
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return w.Code, result
}

func agentTurnInstallation(t *testing.T, h *HTTPHandler, scope contracts.Scope) (string, string, string) {
	t.Helper()
	suffix := httpSuffix(t)
	agent := "chat-agent-" + suffix
	if _, err := h.store.RegisterAgent(context.Background(), scope.Subject, agent); err != nil {
		t.Fatal(err)
	}
	m := validManifest()
	m.State.Compat = "chat-" + suffix
	// Use the existing real Evals gate, not an unreviewed synthetic admission.
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's eval store shares this owned PostgreSQL pool.
	_, eval := newHTTPFixture(t)
	httpPassVerdict(t, eval, digest)
	v, err := h.store.PutVersion(context.Background(), scope.Subject, agent, digest, m)
	if err != nil {
		t.Fatal(err)
	}
	inst, err := h.store.Install(context.Background(), scope.Tenant, agent, v.ID, "dev", "chat-"+suffix, m.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	return inst.ID, agent, v.ID
}

func TestAgentTurnPinUsesVerifiedTaskAndSurvivesConfigChange(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "chat-pin-" + httpSuffix(t), Deployment: "space", Subject: "owner"}
	installation, agent, version := agentTurnInstallation(t, h, scope)
	ctx := context.Background()
	first, err := h.store.PutConfig(ctx, scope.Tenant, agent, 0, json.RawMessage(`{"instructions":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	code, result := callAgentTurnPin(t, h, scope, "run-one", "conversation", map[string]any{"installation_id": installation})
	if code != 200 {
		t.Fatalf("first pin: %d %v", code, result)
	}
	b, _ := json.Marshal(result["target"])
	var pinned EffectiveConfig
	if json.Unmarshal(b, &pinned) != nil {
		t.Fatal("target shape")
	}
	if pinned.VersionID != version || pinned.ConfigRevision != first.Revision || pinned.Installation != installation || pinned.Task == "" {
		t.Fatalf("wrong pin: %+v", pinned)
	}
	if _, err := h.store.PutConfig(ctx, scope.Tenant, agent, first.Revision, json.RawMessage(`{"instructions":"second"}`)); err != nil {
		t.Fatal(err)
	}
	code, replay := callAgentTurnPin(t, h, scope, "run-one", "conversation", map[string]any{"installation_id": installation})
	if code != 200 {
		t.Fatalf("replay: %d %v", code, replay)
	}
	old, _ := json.Marshal(replay["target"])
	if !bytes.Equal(old, b) {
		t.Fatal("config rollout rewrote the already pinned turn")
	}
	code, next := callAgentTurnPin(t, h, scope, "run-two", "conversation", map[string]any{"installation_id": installation})
	if code != 200 {
		t.Fatalf("next turn: %d %v", code, next)
	}
	newBytes, _ := json.Marshal(next["target"])
	var fresh EffectiveConfig
	_ = json.Unmarshal(newBytes, &fresh)
	if fresh.ConfigRevision == pinned.ConfigRevision || fresh.Task == pinned.Task {
		t.Fatal("a new turn did not receive a fresh pin/config")
	}
}

func TestAgentTurnPinRejectsCallerControlledAuthorityAndForeignInstallation(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "chat-authority-" + httpSuffix(t), Deployment: "space", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	for _, field := range []string{"version_id", "deployment", "endpoint", "task_id", "config"} {
		code, _ := callAgentTurnPin(t, h, scope, "run", "conversation", map[string]any{"installation_id": installation, field: "injected"})
		if code != 400 {
			t.Fatalf("caller-controlled %s accepted: %d", field, code)
		}
	}
	for _, tt := range []struct{ task, conversation string }{{"", "conversation"}, {"run", ""}} {
		code, _ := callAgentTurnPin(t, h, scope, tt.task, tt.conversation, map[string]any{"installation_id": installation})
		if code != 403 {
			t.Fatalf("missing authenticated task/context accepted: %d", code)
		}
	}
	foreign := scope
	foreign.Tenant = "another-" + httpSuffix(t)
	code, _ := callAgentTurnPin(t, h, foreign, "run", "conversation", map[string]any{"installation_id": installation})
	if code != 404 {
		t.Fatalf("foreign installation: %d", code)
	}
}

func TestAgentTurnPinSeparatesSameNamedTasksAcrossDeployments(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "chat-deploy-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	pins := map[string]bool{}
	for _, deployment := range []string{"dot", "dot-second"} {
		scope.Deployment = deployment
		code, out := callAgentTurnPin(t, h, scope, "same-run", "same-context", map[string]any{"installation_id": installation})
		if code != 200 {
			t.Fatalf("%s pin: %d %v", deployment, code, out)
		}
		raw, _ := json.Marshal(out["target"])
		var config EffectiveConfig
		_ = json.Unmarshal(raw, &config)
		if config.Task == "" || pins[config.Task] {
			t.Fatal("cross-deployment task pin collision")
		}
		pins[config.Task] = true
	}
}
