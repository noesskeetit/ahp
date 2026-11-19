package registry_test

import (
	"context"
	"encoding/json"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Actual pinned upstream protocol implementation, actual Registry PG routing.
// Supply-chain and token issuance are explicit fixtures; this is not an LLM,
// workload mTLS, durable native task store, or crash-recovery acceptance.
func TestAgentConversationUsesNativeA2A10AndReturnsWhileAgentIsWorking(t *testing.T) {
	store, tenant := newSyntheticStore(t)
	agent := "native-protocol-" + randSuffix(t)
	m := manifest(t, "native-protocol")
	m.Components[0].Image = "example/agent@sha256:" + strings.Repeat("d", 64)
	v := publish(t, store, agent, m)
	release := make(chan struct{})
	executor := a2asrv.AgentExecutorFunc(func(ctx context.Context, exec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
		return func(yield func(a2a.Event, error) bool) {
			if exec.Message.Role != a2a.MessageRoleUser || exec.Message.Parts[0].Text() != "hello" {
				t.Error("native server did not receive the user's message")
			}
			if !yield(a2a.NewSubmittedTask(exec, exec.Message), nil) {
				return
			}
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
			if !yield(a2a.NewArtifactEvent(exec, a2a.NewTextPart("native result")), nil) {
				return
			}
			yield(a2a.NewStatusUpdateEvent(exec, a2a.TaskStateCompleted, nil), nil)
		}
	})
	wire := a2asrv.NewJSONRPCHandler(a2asrv.NewHandler(executor))
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": v.Digest, "image": m.Components[0].Image})
			return
		}
		if r.Header.Get("Authorization") != "Bearer fixture-target-token" {
			t.Error("wrong runtime credential")
		}
		wire.ServeHTTP(w, r)
	}))
	defer srv.Close()
	defer close(release)
	binding := registry.RuntimeBinding{Tenant: tenant, Deployment: "native-protocol", Environment: "dev", AgentID: agent, Digest: v.Digest, Image: m.Components[0].Image, Attestation: "synthetic protocol-only admission", Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz"}
	if err := store.RegisterRuntime(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := registry.NewRoutedA2AInvokeDispatcher(store, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.SetRuntimeTokenIssuer(&agentWireIssuer{})
	req := registry.InvokeRequest{Scope: contracts.Scope{Tenant: tenant, Deployment: "space", Subject: "owner"}, Tenant: tenant, Environment: "dev", AgentID: agent, VersionID: v.ID, VersionDigest: v.Digest, InstallationID: "installed", TaskID: "ahp-grant", ConversationContext: "native-context"}
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("hello"))
	message.ContextID = req.ConversationContext
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	raw, err := dispatcher.SendAgentMessage(ctx, req, body)
	if err != nil {
		t.Fatalf("native SendMessage must return before the waiting agent finishes: %v", err)
	}
	var task a2a.Task
	if json.Unmarshal(raw, &task) != nil || task.ID == "" || task.ContextID != message.ContextID || task.Status.State != a2a.TaskStateSubmitted {
		t.Fatalf("invalid native task: %s", raw)
	}
	if string(task.ID) == req.TaskID {
		t.Fatal("test did not exercise opaque native task identity")
	}
	req.Observe = true
	if _, err := dispatcher.AgentTask(t.Context(), req, "GetTask", string(task.ID)); err != nil {
		t.Fatal("native GetTask failed", err)
	}
}
