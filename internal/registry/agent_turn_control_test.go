package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// The dispatcher is a protocol fixture. PG pins and link transactions are real;
// these tests never claim an actual agent or inference request ran.
type agentControlFixture struct {
	fakeDispatcher
	requests  []InvokeRequest
	messages  []map[string]any
	reads     []string
	methods   []string
	changedID bool
	readError error
}

func TestAgentTurnStatusReadDoesNotWaitForAnInstallationWriter(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "agent-chat-read-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	f := &agentControlFixture{}
	_ = AttachInvokeDispatcher(h, f)
	if code, _ := callAgentControl(t, h, "SendAgentTurn", scope, "turn", "conversation", map[string]any{"installation_id": installation, "input": "hello"}); code != 200 {
		t.Fatal(code)
	}
	writer, err := h.store.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background())
	if _, err = writer.Exec(t.Context(), `SELECT id FROM ahp_registry.installations WHERE tenant=$1 AND id=$2 FOR UPDATE`, scope.Tenant, installation); err != nil {
		t.Fatal(err)
	}
	done := make(chan int, 1)
	go func() {
		body, _ := json.Marshal(map[string]any{"installation_id": installation})
		r := httptest.NewRequest(http.MethodPost, "/v1/registry/ReadAgentTurn", bytes.NewReader(body))
		r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Task: "turn", Context: "conversation"}}))
		w := httptest.NewRecorder()
		h.routes["ReadAgentTurn"](w, r)
		done <- w.Code
	}()
	select {
	case code := <-done:
		if code != 200 {
			t.Fatalf("pinned read failed: %d", code)
		}
	case <-time.After(1500 * time.Millisecond):
		_ = writer.Rollback(context.Background())
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("status reader failed to stop after its lock was released")
		}
		t.Fatal("status polling takes the installation write lock instead of reading the pinned snapshot")
	}
}

func (f *agentControlFixture) SendAgentMessage(_ context.Context, r InvokeRequest, message json.RawMessage) (json.RawMessage, error) {
	f.requests = append(f.requests, r)
	var m map[string]any
	_ = json.Unmarshal(message, &m)
	f.messages = append(f.messages, m)
	id := "opaque-" + r.TaskID
	if f.changedID {
		id = "changed-" + id
	}
	b, _ := json.Marshal(map[string]any{"id": id, "kind": "task", "status": map[string]any{"state": "input-required"}})
	return b, nil
}
func (f *agentControlFixture) AgentTask(_ context.Context, r InvokeRequest, method, remote string) (json.RawMessage, error) {
	f.requests = append(f.requests, r)
	f.reads = append(f.reads, remote)
	f.methods = append(f.methods, method)
	if f.readError != nil {
		return nil, f.readError
	}
	b, _ := json.Marshal(map[string]any{"id": remote, "kind": "task", "status": map[string]any{"state": "working"}})
	return b, nil
}

func TestAgentTurnMissingLinkIsDistinctFromMissingKnownRemoteTask(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "agent-chat-missing-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	f := &agentControlFixture{}
	_ = AttachInvokeDispatcher(h, f)
	if code, _ := callAgentTurnPin(t, h, scope, "turn", "conversation", map[string]any{"installation_id": installation}); code != 200 {
		t.Fatal(code)
	}
	code, out := callAgentControl(t, h, "ReadAgentTurn", scope, "turn", "conversation", map[string]any{"installation_id": installation})
	if code != 404 || out["reason"] != "AGENT_TASK_UNCONFIRMED" {
		t.Fatalf("unconfirmed dispatch has no distinct machine reason: %d %v", code, out)
	}
	if len(f.methods) != 0 {
		t.Fatal("an unsent task triggered a remote read")
	}
	if code, _ = callAgentControl(t, h, "SendAgentTurn", scope, "turn", "conversation", map[string]any{"installation_id": installation, "input": "hello"}); code != 200 {
		t.Fatal(code)
	}
	f.readError = contracts.NewError(contracts.ErrNotFound, "remote runtime lost a known task")
	code, out = callAgentControl(t, h, "ReadAgentTurn", scope, "turn", "conversation", map[string]any{"installation_id": installation})
	if code != 404 || out["reason"] == "AGENT_TASK_UNCONFIRMED" {
		t.Fatalf("missing known task was relabeled as permission to start again: %d %v", code, out)
	}
}
func callAgentControl(t *testing.T, h *HTTPHandler, op string, scope contracts.Scope, task, conversation string, body any) (int, map[string]any) {
	t.Helper()
	fn, ok := h.routes[op]
	if !ok {
		t.Fatalf("%s missing: no resumable agent conversation transport", op)
	}
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/registry/"+op, bytes.NewReader(b))
	r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Task: task, Context: conversation}}))
	r.Header.Set("Authorization", "Bearer coordinator-secret")
	w := httptest.NewRecorder()
	fn(w, r)
	var out map[string]any
	if json.Unmarshal(w.Body.Bytes(), &out) != nil {
		t.Fatal("response is not JSON")
	}
	return w.Code, out
}

func TestAgentTurnControlPersistsOpaqueTaskAndReplaysTheSameMessage(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "agent-chat-control-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, agent, version := agentTurnInstallation(t, h, scope)
	_, err := h.store.PutConfig(t.Context(), scope.Tenant, agent, 0, json.RawMessage(`{"instructions":"original"}`))
	if err != nil {
		t.Fatal(err)
	}
	f := &agentControlFixture{}
	if err = AttachInvokeDispatcher(h, f); err != nil {
		t.Fatal(err)
	}
	initial := map[string]any{"installation_id": installation, "input": map[string]any{"content": "hello"}}
	code, started := callAgentControl(t, h, "SendAgentTurn", scope, "turn-one", "conversation", initial)
	if code != 200 {
		t.Fatalf("start: %d %v", code, started)
	}
	if len(f.requests) != 1 || f.requests[0].TaskID == "turn-one" || f.requests[0].VersionID != version || f.requests[0].Token != "coordinator-secret" {
		t.Fatalf("unscoped target: %+v", f.requests)
	}
	first := f.requests[0]
	wire := f.messages[0]
	if wire["role"] != "ROLE_USER" {
		t.Fatalf("Registry did not construct the pinned A2A 1.0 user role: %v", wire["role"])
	}
	if wire["messageId"] == "" || wire["contextId"] == "" || wire["taskId"] != nil {
		t.Fatal("first message was not an explicit new task", wire)
	}
	if _, err = h.store.PutConfig(t.Context(), scope.Tenant, agent, 1, json.RawMessage(`{"instructions":"updated"}`)); err != nil {
		t.Fatal(err)
	}
	code, replay := callAgentControl(t, h, "SendAgentTurn", scope, "turn-one", "conversation", initial)
	if code != 200 {
		t.Fatalf("retry: %d %v", code, replay)
	}
	a, _ := json.Marshal(started)
	b, _ := json.Marshal(replay)
	if !bytes.Equal(a, b) {
		t.Fatal("retry changed the mapped task")
	}
	a, _ = json.Marshal(wire)
	b, _ = json.Marshal(f.messages[1])
	if !bytes.Equal(a, b) || string(first.Config) != string(f.requests[1].Config) || first.TaskID != f.requests[1].TaskID {
		t.Fatal("retry changed the message or immutable config")
	}
	code, out := callAgentControl(t, h, "ReadAgentTurn", scope, "turn-one", "conversation", map[string]any{"installation_id": installation})
	if code != 200 || len(f.reads) != 1 || f.reads[0] != "opaque-"+first.TaskID || f.methods[0] != "GetTask" {
		t.Fatalf("opaque task mapping lost: %d %v", code, out)
	}
	code, _ = callAgentControl(t, h, "SendAgentTurn", scope, "turn-one", "conversation", map[string]any{"installation_id": installation, "continue": true, "key": "answer-one", "input": "approved"})
	if code != 200 {
		t.Fatalf("continue: %d", code)
	}
	continued := f.messages[2]
	if continued["taskId"] != f.reads[0] || continued["contextId"] != wire["contextId"] || continued["messageId"] == wire["messageId"] {
		t.Fatal("continuation opened another task/context", continued)
	}
	code, _ = callAgentControl(t, h, "CancelAgentTurn", scope, "turn-one", "conversation", map[string]any{"installation_id": installation})
	if code != 200 || f.methods[len(f.methods)-1] != "CancelTask" {
		t.Fatalf("cancel: %d", code)
	}
	code, pinned := callAgentTurnPin(t, h, scope, "turn-one", "conversation", map[string]any{"installation_id": installation})
	if code != 200 || pinned["target"].(map[string]any)["config_revision"] != float64(1) {
		t.Fatal("mapping consumed or rewrote its original pin")
	}
	text, _ := json.Marshal(started)
	if bytes.Contains(text, []byte("coordinator-secret")) {
		t.Fatal("execution credential returned in a receipt")
	}
}

func TestAgentTurnControlIsScopedAndDoesNotInventAnUnsentTask(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "agent-chat-scope-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	f := &agentControlFixture{}
	_ = AttachInvokeDispatcher(h, f)
	for _, op := range []string{"ReadAgentTurn", "CancelAgentTurn"} {
		code, _ := callAgentControl(t, h, op, scope, "never-sent", "conversation", map[string]any{"installation_id": installation})
		if code != 404 {
			t.Fatalf("%s fabricated unsent task: %d", op, code)
		}
	}
	code, _ := callAgentControl(t, h, "SendAgentTurn", scope, "own-turn", "conversation", map[string]any{"installation_id": installation, "input": "hello"})
	if code != 200 {
		t.Fatal(code)
	}
	for _, field := range []string{"task_id", "context_id", "endpoint", "version_id", "token"} {
		code, _ = callAgentControl(t, h, "SendAgentTurn", scope, "own-turn", "conversation", map[string]any{"installation_id": installation, "input": "hello", field: "injected"})
		if code != 400 {
			t.Fatalf("accepted %s: %d", field, code)
		}
	}
	for _, foreign := range []contracts.Scope{{Tenant: scope.Tenant, Deployment: "dot-second", Subject: scope.Subject}, {Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: "another-user"}} {
		code, _ = callAgentControl(t, h, "ReadAgentTurn", foreign, "own-turn", "conversation", map[string]any{"installation_id": installation})
		if code != 404 {
			t.Fatalf("foreign run obtained task mapping: %d", code)
		}
	}
	code, _ = callAgentControl(t, h, "ReadAgentTurn", scope, "another-turn", "conversation", map[string]any{"installation_id": installation})
	if code != 404 {
		t.Fatalf("sibling task obtained mapping: %d", code)
	}
	code, _ = callAgentControl(t, h, "SendAgentTurn", scope, "own-turn", "conversation", map[string]any{"installation_id": installation, "continue": true, "input": "hello"})
	if code != 400 {
		t.Fatalf("continuation without durable key: %d", code)
	}
}

func TestAgentTurnControlKeepsContextAcrossTurnsAndRefusesTaskSubstitution(t *testing.T) {
	h, _ := newHTTPFixture(t)
	scope := contracts.Scope{Tenant: "agent-chat-context-" + httpSuffix(t), Deployment: "dot", Subject: "owner"}
	installation, _, _ := agentTurnInstallation(t, h, scope)
	f := &agentControlFixture{}
	_ = AttachInvokeDispatcher(h, f)
	for _, task := range []string{"turn-one", "turn-two"} {
		code, _ := callAgentControl(t, h, "SendAgentTurn", scope, task, "same-conversation", map[string]any{"installation_id": installation, "input": "hello"})
		if code != 200 {
			t.Fatal(code)
		}
	}
	if f.messages[0]["contextId"] != f.messages[1]["contextId"] || f.requests[0].TaskID == f.requests[1].TaskID {
		t.Fatal("context not retained or separate turns merged")
	}
	f.changedID = true
	code, _ := callAgentControl(t, h, "SendAgentTurn", scope, "turn-one", "same-conversation", map[string]any{"installation_id": installation, "input": "hello"})
	if code != 409 {
		t.Fatalf("native task substitution accepted: %d", code)
	}
	code, _ = callAgentControl(t, h, "ReadAgentTurn", scope, "turn-one", "same-conversation", map[string]any{"installation_id": installation})
	if code != 200 || f.reads[len(f.reads)-1] != "opaque-"+f.requests[0].TaskID {
		t.Fatal("original task link overwritten")
	}
}
