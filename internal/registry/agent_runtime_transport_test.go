package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type agentWireIssuer struct{ calls []registry.InvokeRequest }

func (i *agentWireIssuer) IssueRuntimeToken(_ context.Context, r registry.InvokeRequest, _ registry.RuntimeBinding) (string, error) {
	i.calls = append(i.calls, r)
	return "fixture-target-token", nil
}

func TestAgentConversationTLSRoutingKeepsTaskContextAndAllowsDrainingReads(t *testing.T) {
	// Real PG + TLS wire fixture; no claim of actual mTLS identity or LLM execution.
	store, tenant := newSyntheticStore(t)
	agent := "agent-conversation-" + randSuffix(t)
	m := manifest(t, "conversation-runtime")
	m.Components[0].Image = "example/agent@sha256:" + strings.Repeat("c", 64)
	v := publish(t, store, agent, m)
	var ready atomic.Bool
	ready.Store(true)
	var broken atomic.Bool
	var redirect atomic.Bool
	var requests atomic.Int64
	var sinkCalls atomic.Int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { sinkCalls.Add(1); w.WriteHeader(200) }))
	defer sink.Close()
	var messages []map[string]any
	var methods []string
	var mu sync.Mutex
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			if !ready.Load() {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": v.Digest, "image": m.Components[0].Image})
			return
		}
		requests.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-target-token" {
			t.Error("parent credential or missing target credential sent to runtime")
		}
		if redirect.Load() {
			w.Header().Set("Location", sink.URL)
			w.WriteHeader(307)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		var body struct {
			ID     any            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		methods = append(methods, body.Method)
		if message, ok := body.Params["message"].(map[string]any); ok {
			messages = append(messages, message)
			meta := body.Params["metadata"].(map[string]any)["ahp"].(map[string]any)
			if meta["task_id"] != "child-grant" || meta["version_digest"] != v.Digest {
				t.Error("wrong pinned target metadata")
			}
		}
		if broken.Load() {
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"wrong-reply","result":{"id":"native-task"}}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": body.ID, "result": map[string]any{"id": "native-task", "contextId": "stable-context", "status": map[string]any{"state": "working"}}})
	}))
	defer srv.Close()
	binding := registry.RuntimeBinding{Tenant: tenant, Deployment: "agent-v1", Environment: "dev", AgentID: agent, Digest: v.Digest, Image: m.Components[0].Image, Attestation: "synthetic protocol fixture; not a signed deployment", Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz"}
	if err := store.RegisterRuntime(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	d, err := registry.NewRoutedA2AInvokeDispatcher(store, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	issuer := &agentWireIssuer{}
	d.SetRuntimeTokenIssuer(issuer)
	r := registry.InvokeRequest{Scope: contracts.Scope{Tenant: tenant, Deployment: "space", Subject: "owner"}, Tenant: tenant, Environment: "dev", AgentID: agent, VersionID: v.ID, VersionDigest: v.Digest, InstallationID: "installed", TaskID: "child-grant", Token: "parent-not-for-runtime", ConversationContext: "stable-context"}
	message := json.RawMessage(`{"role":"user","kind":"message","messageId":"stable-message","contextId":"stable-context","parts":[{"kind":"text","text":"hello"}]}`)
	if _, err = d.SendAgentMessage(t.Context(), r, message); err != nil {
		t.Fatal(err)
	}
	ready.Store(false)
	r.Observe = true
	continued := json.RawMessage(`{"role":"user","kind":"message","messageId":"stable-answer","taskId":"native-task","contextId":"stable-context","parts":[{"kind":"text","text":"approved"}]}`)
	if _, err = d.SendAgentMessage(t.Context(), r, continued); err != nil {
		t.Fatal("a draining runtime's existing task cannot continue", err)
	}
	for _, method := range []string{"GetTask", "CancelTask"} {
		if _, err = d.AgentTask(t.Context(), r, method, "native-task"); err != nil {
			t.Fatal("existing task rejected by readiness", method, err)
		}
	}
	mu.Lock()
	if len(messages) != 2 || messages[1]["taskId"] != "native-task" || messages[0]["contextId"] != messages[1]["contextId"] || len(methods) != 4 {
		t.Fatal("wrong task conversation wire", messages, methods)
	}
	mu.Unlock()
	for _, seen := range issuer.calls {
		if seen.ConversationContext != "stable-context" || seen.Token != r.Token {
			t.Fatal("issuer lost parent/context authority")
		}
	}
	before := requests.Load()
	if _, err = d.AgentTask(t.Context(), r, "DeleteEverything", "native-task"); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatal("arbitrary method accepted", err)
	}
	if requests.Load() != before {
		t.Fatal("unsupported method reached runtime")
	}
	broken.Store(true)
	if _, err = d.AgentTask(t.Context(), r, "GetTask", "native-task"); err == nil {
		t.Fatal("reply of another RPC accepted")
	}
	broken.Store(false)
	redirect.Store(true)
	if _, err = d.AgentTask(t.Context(), r, "GetTask", "native-task"); err == nil {
		t.Fatal("redirect accepted")
	}
	if sinkCalls.Load() != 0 {
		t.Fatal("target credential forwarded to another origin")
	}
}
