package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// This uses the actual pinned upstream HTTP server, not a hand-created
// ExecutorContext. State remains the explicitly labelled unit-test store.
// The trusted principal is installed server-side; no request body supplies it.
func TestNativeA2AHTTPSeparatesOpaqueTaskFromAHPGrant(t *testing.T) {
	for _, badContext := range []bool{false, true} {
		name := "verified-grant"
		if badContext {
			name = "wrong-conversation"
		}
		t.Run(name, func(t *testing.T) {
			var stateCalls, graphCalls atomic.Int32
			cfg, _ := executorFixture(t, func(http.ResponseWriter, *http.Request) bool {
				stateCalls.Add(1)
				return false
			})
			executor, err := NewLangGraphExecutor(cfg, func(_ context.Context, rc *RunContext) (json.RawMessage, error) {
				graphCalls.Add(1)
				if rc.Task.TaskID != "task-new" {
					t.Errorf("graph lost the verified AHP task: %s", rc.Task.TaskID)
				}
				return json.RawMessage(`{"ok":true}`), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			wire := a2asrv.NewJSONRPCHandler(NewA2AHandler(executor))
			scope := contracts.Scope{Tenant: "native-unit", Deployment: "agent", Subject: "owner"}
			principal := transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "task-new", Context: "context-new"}}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wire.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), principal)))
			}))
			defer server.Close()
			message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("NEW"))
			message.ContextID = "context-new"
			if badContext {
				message.ContextID = "foreign-conversation"
			}
			params := a2a.SendMessageRequest{Message: message, Config: &a2a.SendMessageConfig{}}
			body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "native-http", "method": "SendMessage", "params": params})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("A2A-Version", "1.0")
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var reply struct {
				Result struct {
					Task *a2a.Task `json:"task"`
				} `json:"result"`
				Error json.RawMessage `json:"error"`
			}
			if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
				t.Fatal(err)
			}
			if badContext {
				if graphCalls.Load() != 0 || stateCalls.Load() != 0 {
					t.Fatalf("wrong conversation reached State/graph: State=%d graph=%d", stateCalls.Load(), graphCalls.Load())
				}
				var refusal struct {
					Code int `json:"code"`
				}
				if json.Unmarshal(reply.Error, &refusal) != nil || refusal.Code != -31403 {
					t.Fatalf("wrong conversation must be an A2A permission refusal, not a failed native task: %s", reply.Error)
				}
				return
			}
			if reply.Result.Task == nil || reply.Result.Task.Status.State != a2a.TaskStateCompleted || graphCalls.Load() != 1 {
				t.Fatalf("native A2A task must execute under its distinct AHP grant: task=%+v graph_calls=%d error=%s", reply.Result.Task, graphCalls.Load(), reply.Error)
			}
			if string(reply.Result.Task.ID) == principal.Claims.Task {
				t.Fatal("probe did not exercise an opaque native A2A task ID")
			}
		})
	}
}
