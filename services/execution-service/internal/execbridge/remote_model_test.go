package execbridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func remoteTestContext() context.Context {
	ctx := core.WithRunScope(context.Background(), contracts.Scope{Tenant: "remote-tenant", Deployment: "remote-deployment", Subject: "remote-subject"}, "current-run-token")
	return core.WithRunBinding(ctx, core.RunBinding{TaskID: "durable-task", ContextID: "durable-context"})
}

func TestRemoteModelRequiresRunScopeCredentialAndBinding(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(model.Generation{Status: model.GenCompleted, Output: "unexpected"})
	}))
	defer srv.Close()
	client, err := NewRemoteModel(srv.URL, "default-chat", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "tenant", Subject: "subject", Deployment: "deployment"}
	for name, ctx := range map[string]context.Context{
		"no run":        context.Background(),
		"binding only":  core.WithRunBinding(context.Background(), core.RunBinding{TaskID: "task"}),
		"scope only":    core.WithRunScope(context.Background(), scope, "task-token"),
		"empty token":   core.WithRunBinding(core.WithRunScope(context.Background(), scope, ""), core.RunBinding{TaskID: "task"}),
		"no task":       core.WithRunBinding(core.WithRunScope(context.Background(), scope, "task-token"), core.RunBinding{ContextID: "context"}),
		"invalid scope": core.WithRunBinding(core.WithRunScope(context.Background(), contracts.Scope{}, "task-token"), core.RunBinding{TaskID: "task"}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := client.Generate(ctx, &core.ModelRequest{}); err == nil {
				t.Fatal("unauthenticated run reached the model boundary")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid contexts sent %d HTTP requests", calls.Load())
	}
}

func TestRemoteModelWirePreservesToolConversationAndDecodesGeneration(t *testing.T) {
	var submitted map[string]json.RawMessage
	var paths []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer current-run-token" {
			t.Error("request did not use the active run token")
		}
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Error("unexpected request method or content type")
		}
		if strings.HasSuffix(r.URL.Path, "/SubmitModel") {
			if err := json.NewDecoder(r.Body).Decode(&submitted); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(model.Generation{ID: "gen-wire", Status: model.GenRunning})
			return
		}
		var get map[string]string
		if err := json.NewDecoder(r.Body).Decode(&get); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(get, map[string]string{"id": "gen-wire"}) {
			t.Errorf("GetModel body=%v", get)
		}
		_ = json.NewEncoder(w).Encode(model.Generation{ID: "gen-wire", Status: model.GenCompleted,
			Output: `{"text":"continue","tool_calls":[{"id":"call-next","type":"function","function":{"name":"save_artifact","arguments":"{\"name\":\"result.txt\"}"}}]}`,
			Usage:  model.Usage{PromptTokens: 12, CompletionTokens: 7}})
	}))
	defer srv.Close()
	client, err := NewRemoteModel(srv.URL, "default-chat", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	request := &core.ModelRequest{Step: 2, MaxTokens: 128, Messages: []core.ChatMessage{
		{Role: "user", Content: json.RawMessage(`"create artifact"`)},
		{Role: "assistant", Content: json.RawMessage(`""`), ToolCalls: json.RawMessage(`[{"id":"call-previous","name":"read_file","args":{}}]`)},
		{Role: "tool", Content: json.RawMessage(`"file contents"`), ToolCallID: "call-previous"},
	}, Tools: []core.ToolDef{{Name: "save_artifact", Parameters: json.RawMessage(`{"type":"object"}`)}}}
	got, err := client.Generate(remoteTestContext(), request)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Content) != `"continue"` || got.Stop != "tool_calls" || len(got.ToolCalls) != 1 || got.ToolCalls[0].ID != "call-next" || got.ToolCalls[0].Name != "save_artifact" || string(got.ToolCalls[0].Args) != `{"name":"result.txt"}` {
		t.Fatalf("decoded generation=%+v", got)
	}
	if got.Usage == nil || got.Usage.Input != 12 || got.Usage.Output != 7 {
		t.Fatalf("usage=%+v", got.Usage)
	}
	if !reflect.DeepEqual(paths, []string{"/v1/model/SubmitModel", "/v1/model/GetModel"}) {
		t.Fatalf("paths=%v", paths)
	}
	allowed := map[string]bool{"key": true, "model": true, "messages": true, "tools": true, "max_tokens": true}
	for name := range submitted {
		if !allowed[name] {
			t.Errorf("caller authority leaked in field %q", name)
		}
	}
	if len(submitted) != len(allowed) {
		t.Fatalf("body fields=%v", submitted)
	}
	var messages []model.Message
	if err := json.Unmarshal(submitted["messages"], &messages); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 || len(messages[1].ToolCalls) != 1 || messages[1].ToolCalls[0].ID != "call-previous" || messages[2].ToolCallID != "call-previous" || messages[2].Role != "tool" {
		t.Fatalf("tool conversation lost=%+v", messages)
	}
}

func TestRemoteModelRetryKeyAndResumeRequestIdentity(t *testing.T) {
	var keys []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		keys = append(keys, body.Key)
		_ = json.NewEncoder(w).Encode(model.Generation{Status: model.GenCompleted, Output: "answer"})
	}))
	defer srv.Close()
	client, err := NewRemoteModel(srv.URL, "default-chat", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	req := &core.ModelRequest{Messages: []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"before checkpoint"`)}}, MaxTokens: 100}
	for i := 0; i < 2; i++ {
		got, err := client.Generate(remoteTestContext(), req)
		if err != nil || string(got.Content) != `"answer"` || got.Stop != "stop" {
			t.Fatalf("call response=%+v err=%v", got, err)
		}
	}
	req.Messages = append(req.Messages, core.ChatMessage{Role: "user", Content: json.RawMessage(`"after resume with the same loop step"`)})
	if _, err := client.Generate(remoteTestContext(), req); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 || keys[0] == "" || keys[0] != keys[1] || keys[0] == keys[2] {
		t.Fatalf("logical keys=%v", keys)
	}
}

func TestRemoteModelDoesNotFollowRedirectWithRunBearer(t *testing.T) {
	var destinationCalls atomic.Int32
	destination := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destinationCalls.Add(1) }))
	defer destination.Close()
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client, err := NewRemoteModel(origin.URL, "default-chat", origin.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Generate(remoteTestContext(), &core.ModelRequest{})
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirect err=%v", err)
	}
	if destinationCalls.Load() != 0 {
		t.Fatal("bearer-bearing redirect was followed")
	}
}
