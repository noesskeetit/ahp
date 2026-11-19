package space

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAgentControlUsesOwnerAuthorityAndOpaqueTask(t *testing.T) {
	var operations []string
	owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer run-token" {
			t.Error("agent control lost its own run authority")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["installation_id"] != "installed" || body["task_id"] != nil || body["endpoint"] != nil || body["token"] != nil {
			t.Error("runtime routing escaped the Registry owner")
		}
		operations = append(operations, r.URL.Path)
		switch r.URL.Path {
		case "/v1/registry/SendAgentTurn":
			if body["continue"] == true && body["key"] != "answer-1" {
				t.Error("continuation lost its durable key")
			}
		case "/v1/registry/ReadAgentTurn", "/v1/registry/CancelAgentTurn":
			if len(body) != 1 {
				t.Error("read/cancel injected additional authority")
			}
		default:
			t.Error("wrong owner operation")
		}
		_, _ = w.Write([]byte(`{"grant_task_id":"grant","task":{"id":"opaque-native","contextId":"native-context","status":{"state":"input-required"},"artifacts":[{"artifactId":"result","parts":[{"kind":"text","text":"Question?"}]}]}}`))
	}))
	defer owner.Close()
	client, err := NewAgentControlClient(owner.URL, owner.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, call := range []func() (*AgentTaskSnapshot, error){
		func() (*AgentTaskSnapshot, error) {
			return client.Send(ctx, "run-token", "installed", json.RawMessage(`"hello"`), "")
		},
		func() (*AgentTaskSnapshot, error) {
			return client.Send(ctx, "run-token", "installed", json.RawMessage(`"yes"`), "answer-1")
		},
		func() (*AgentTaskSnapshot, error) { return client.Read(ctx, "run-token", "installed") },
		func() (*AgentTaskSnapshot, error) { return client.Cancel(ctx, "run-token", "installed") },
	} {
		task, err := call()
		if err != nil {
			t.Fatal(err)
		}
		if task.ID != "opaque-native" || task.ContextID != "native-context" || task.Status.State != "input-required" || len(task.Artifacts) != 1 {
			t.Fatalf("native task was changed: %+v", task)
		}
	}
	if len(operations) != 4 {
		t.Fatal("missing control operation")
	}
}

func TestAgentControlRejectsRedirectAndMalformedTask(t *testing.T) {
	for _, body := range []string{`{"task":{}}`, `{"task":{"id":"id","status":{"state":"invented"}}}`, `redirect`} {
		owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if body == "redirect" {
				w.Header().Set("Location", "https://foreign.invalid")
				w.WriteHeader(307)
				return
			}
			_, _ = w.Write([]byte(body))
		}))
		client, err := NewAgentControlClient(owner.URL, owner.Client())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = client.Read(context.Background(), "token", "installed"); err == nil {
			t.Error("invalid task was accepted")
		}
		owner.Close()
	}
	if client, err := NewAgentControlClient("http://unsafe.invalid", http.DefaultClient); err == nil || client != nil {
		t.Fatal("failed control constructor returned a usable client")
	}
}

func TestAgentControlUnderstandsNativeA2A10States(t *testing.T) {
	states := map[string]string{
		"TASK_STATE_SUBMITTED": "submitted", "TASK_STATE_WORKING": "working",
		"TASK_STATE_COMPLETED": "completed", "TASK_STATE_CANCELED": "canceled",
		"TASK_STATE_FAILED": "failed", "TASK_STATE_REJECTED": "rejected",
		"TASK_STATE_INPUT_REQUIRED": "input-required", "TASK_STATE_AUTH_REQUIRED": "auth-required",
	}
	for native, local := range states {
		t.Run(native, func(t *testing.T) {
			owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"task": map[string]any{"id": "native", "contextId": "conversation", "status": map[string]any{"state": native}}})
			}))
			defer owner.Close()
			client, err := NewAgentControlClient(owner.URL, owner.Client())
			if err != nil {
				t.Fatal(err)
			}
			task, err := client.Read(t.Context(), "token", "installed")
			if err != nil || task.Status.State != local {
				t.Fatalf("native state %s must project to %s: task=%+v error=%v", native, local, task, err)
			}
		})
	}
}
