//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/agentworkload"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// The actual native runtime talks to real State/PostgreSQL over mTLS. The model
// is a named protocol fixture and Identity authorization remains the cEnv seam.
// No Coordinator transcript or Dot Core exists in this test.
func TestNativeAgentOwnConversationSurvivesRuntimeRestart_RealState(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(map[bool]string{false: "restored-history", true: "unreadable-prior-part-refuses-new-model-call"}[unavailable], func(t *testing.T) { testNativeConversationRestart(t, unavailable) })
	}
}

func testNativeConversationRestart(t *testing.T, unavailable bool) {
	e := cNewEnv(t)
	const marker = "NATIVE-HISTORY-4e37b"
	var mu sync.Mutex
	var calls [][]map[string]string
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(e.ca.pem)
	model := httptest.NewUnstartedServer(transport.Task(e.verifier, cAudience, "model", func(context.Context, auth.Claims, string) error { return nil })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]string `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid", 400)
			return
		}
		mu.Lock()
		calls = append(calls, body.Messages)
		number := len(calls)
		mu.Unlock()
		answer := "ACK-NATIVE"
		if number == 2 {
			answer = "HISTORY-MISSING"
			if len(body.Messages) == 4 && body.Messages[1]["role"] == "user" && strings.Contains(body.Messages[1]["content"], marker) && body.Messages[2]["role"] == "assistant" && body.Messages[2]["content"] == "ACK-NATIVE" {
				answer = "HISTORY-OK"
			}
		}
		transport.WriteJSON(w, 200, map[string]any{"id": contracts.NewID(), "status": "COMPLETED", "output": answer})
	})))
	model.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	model.StartTLS()
	defer model.Close()
	var blockParts atomic.Bool
	stateProxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockParts.Load() && r.URL.Path == "/v1/state/GetPart" {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "controlled prior-part outage"))
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, e.srv.URL+r.URL.Path, r.Body)
		if err != nil {
			http.Error(w, "proxy", 500)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := e.srv.Client().Do(req)
		if err != nil {
			http.Error(w, "dependency", 503)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	stateProxy.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	stateProxy.StartTLS()
	defer stateProxy.Close()
	gateway := auth.WorkloadID(contracts.Scope{Tenant: e.tenant, Deployment: "gateway"})
	cfg := agentworkload.Config{Tenant: e.tenant, Deployment: e.deployment, AgentID: "native", Digest: "sha256:" + strings.Repeat("a", 64), Image: "example/native@sha256:" + strings.Repeat("a", 64), Profile: "native-conversation", StateURL: stateProxy.URL, ModelURL: model.URL, Worker: "first-native", GatewayWorkloads: []string{gateway}, Client: e.srv.Client(), Verifier: e.verifier}
	start := func(config agentworkload.Config) (*httptest.Server, *http.Client, func()) {
		t.Helper()
		svc, err := agentworkload.New(config)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := context.WithCancel(e.ctx)
		done := make(chan struct{})
		go func() { defer close(done); svc.Run(ctx) }()
		server := httptest.NewUnstartedServer(svc)
		server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
		server.StartTLS()
		tr := server.Client().Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, gateway)}
		client := &http.Client{Transport: tr, Timeout: 15 * time.Second}
		return server, client, func() { stop(); <-done; server.Close(); tr.CloseIdleConnections() }
	}
	send := func(server *httptest.Server, client *http.Client, task, text string) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "rpc-" + task, "method": "SendMessage", "params": map[string]any{"message": map[string]any{"messageId": "msg-" + task, "contextId": "ctx-1", "parts": []any{map[string]string{"kind": "text", "text": text}}}, "configuration": map[string]bool{"blocking": true}, "metadata": map[string]any{"ahp": map[string]any{"task_id": task, "agent_id": cfg.AgentID, "version_digest": cfg.Digest, "config_revision": 1, "config": map[string]any{"model": "controlled", "max_tokens": 128}}}}})
		req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, server.URL+"/a2a", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+e.signTask(t, task))
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]any
		if response.StatusCode != 200 || json.Unmarshal(raw, &out) != nil || out["error"] != nil {
			t.Fatalf("native task failed: status=%d body=%s", response.StatusCode, raw)
		}
		return out
	}
	first, firstClient, stopFirst := start(cfg)
	send(first, firstClient, "native-one", "Remember "+marker)
	stopFirst()
	blockParts.Store(unavailable)
	cfg.Worker = "second-native"
	second, secondClient, stopSecond := start(cfg)
	defer stopSecond()
	result := send(second, secondClient, "native-two", "What code did I ask you to remember?")
	raw, _ := json.Marshal(result)
	if !unavailable && !bytes.Contains(raw, []byte("HISTORY-OK")) {
		t.Fatalf("new runtime lost its own conversation state: %s", raw)
	}
	if unavailable && result["result"].(map[string]any)["status"].(map[string]any)["state"] != "failed" {
		t.Fatal("unreadable history did not stop the native turn")
	}
	mu.Lock()
	count := len(calls)
	mu.Unlock()
	wantCalls := 2
	if unavailable {
		wantCalls = 1
	}
	if count != wantCalls {
		t.Fatalf("unexpected model executions: %d", count)
	}
	code, thread := e.raw("GetThreadByContext", e.signTask(t, "reader"), []byte(`{"context_id":"ctx-1"}`))
	if code != http.StatusOK {
		t.Fatalf("native thread read failed: %d", code)
	}
	threadID, _ := thread["id"].(string)
	runs, err := e.client.ListRuns(e.ctx, e.signTask(t, "reader"), threadID)
	if err != nil || len(runs) != 2 {
		t.Fatal("native conversation not held in one durable thread", err)
	}
}
