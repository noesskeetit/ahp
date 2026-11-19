package sdk

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"iter"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type nativeOwnerProbe struct{ executions, cancellations atomic.Int32 }

func (p *nativeOwnerProbe) Execute(_ context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		p.executions.Add(1)
		if ec.StoredTask == nil && !yield(a2a.NewSubmittedTask(ec, ec.Message), nil) {
			return
		}
		question := a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("owner-private-marker"))
		question.ID = "private-question"
		yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateInputRequired, question), nil)
	}
}

func (p *nativeOwnerProbe) Cancel(_ context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		p.cancellations.Add(1)
		yield(a2a.NewStatusUpdateEvent(ec, a2a.TaskStateCanceled, nil), nil)
	}
}

// Public SDK composition under test; RED used the actual bare upstream handler.
func nativeOwnerTestHandler(executor a2asrv.AgentExecutor) a2asrv.RequestHandler {
	return NewA2AHandler(executor)
}

// Actual pinned native taskstore/JSON-RPC/SSE implementation. Principals are
// explicitly server-side fixtures; this component test does not verify JWTs,
// mTLS identity, State, LLM or process recovery.
func TestNativeA2AOwnershipBeforeHistoryReadsWritesAndCancel(t *testing.T) {
	for _, dimension := range []string{"subject", "tenant", "deployment", "task", "context", "human", "missing"} {
		t.Run(dimension, func(t *testing.T) {
			scope := contracts.Scope{Tenant: "native-owner", Deployment: "agent", Subject: "alice"}
			owner := transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "grant-one", Context: "context-one"}}
			foreign := owner
			switch dimension {
			case "subject":
				foreign.Scope.Subject, foreign.Claims.Subject = "bob", "bob"
			case "tenant":
				foreign.Scope.Tenant, foreign.Claims.Tenant = "other", "other"
			case "deployment":
				foreign.Scope.Deployment, foreign.Claims.Deployment = "other", "other"
			case "task":
				foreign.Claims.Task = "other-grant"
			case "context":
				foreign.Claims.Context = "other-conversation"
			case "human":
				foreign.Claims.Kind = auth.KindHuman
			}
			probe := &nativeOwnerProbe{}
			wire := a2asrv.NewJSONRPCHandler(nativeOwnerTestHandler(probe))
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				if r.Header.Get("X-Fixture-Principal") == "owner" {
					ctx = transport.WithPrincipal(ctx, owner)
				} else if dimension != "missing" {
					ctx = transport.WithPrincipal(ctx, foreign)
				}
				wire.ServeHTTP(w, r.WithContext(ctx))
			}))
			defer server.Close()
			type rpcError struct {
				Code int `json:"code"`
			}
			call := func(who, method string, params any) (json.RawMessage, *rpcError) {
				t.Helper()
				id := contracts.NewID()
				body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("A2A-Version", "1.0")
				request.Header.Set("X-Fixture-Principal", who)
				response, err := server.Client().Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var reply struct {
					ID     string          `json:"id"`
					Result json.RawMessage `json:"result"`
					Error  *rpcError       `json:"error"`
				}
				if strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
					scanner := bufio.NewScanner(response.Body)
					scanner.Buffer(make([]byte, 4096), 1<<20)
					found := false
					for scanner.Scan() {
						if data, ok := strings.CutPrefix(scanner.Text(), "data:"); ok {
							if err := json.Unmarshal([]byte(strings.TrimSpace(data)), &reply); err != nil {
								t.Fatal(err)
							}
							found = true
							break
						}
					}
					if !found {
						t.Fatalf("no native SSE response: %v", scanner.Err())
					}
				} else if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
					t.Fatal(err)
				}
				if reply.ID != id {
					t.Fatalf("native reply ID mismatch for %s", method)
				}
				return reply.Result, reply.Error
			}
			initial := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("owner-private-marker"))
			initial.ContextID = owner.Claims.Context
			raw, failure := call("owner", "SendMessage", a2a.SendMessageRequest{Message: initial})
			var started struct {
				Task *a2a.Task `json:"task"`
			}
			if json.Unmarshal(raw, &started) != nil || failure != nil || started.Task == nil || started.Task.ID == "" || started.Task.Status.State != a2a.TaskStateInputRequired {
				t.Fatalf("owner cannot create a native task: %s error=%+v", raw, failure)
			}
			before, failure := call("owner", "GetTask", map[string]any{"id": started.Task.ID})
			if failure != nil {
				t.Fatal("owner cannot read its task")
			}
			var beforeTask a2a.Task
			if json.Unmarshal(before, &beforeTask) != nil {
				t.Fatal("invalid task snapshot")
			}
			for _, method := range []string{"GetTask", "CancelTask", "SendMessage", "SubscribeToTask", "SendStreamingMessage"} {
				var params any = map[string]any{"id": started.Task.ID}
				if method == "SendMessage" || method == "SendStreamingMessage" {
					message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("forged answer"))
					message.TaskID, message.ContextID = started.Task.ID, owner.Claims.Context
					message.Metadata = map[string]any{"tenant": scope.Tenant, "subject": scope.Subject, "task_id": owner.Claims.Task, "context_id": owner.Claims.Context}
					params = a2a.SendMessageRequest{Message: message}
				}
				result, refusal := call("foreign", method, params)
				if refusal == nil || refusal.Code != -32001 && refusal.Code != -31401 && refusal.Code != -31403 || bytes.Contains(result, []byte("owner-private-marker")) {
					t.Errorf("foreign %s reached an owned task: method=%s refusal=%+v private=%v", dimension, method, refusal, bytes.Contains(result, []byte("owner-private-marker")))
				}
			}
			result, refusal := call("foreign", "ListTasks", map[string]any{"pageSize": 10})
			if refusal == nil {
				var list a2a.ListTasksResponse
				if json.Unmarshal(result, &list) != nil || len(list.Tasks) != 0 || list.TotalSize != 0 {
					t.Errorf("foreign %s listed owned native tasks", dimension)
				}
			} else if refusal.Code != -31401 && refusal.Code != -31403 {
				t.Errorf("unexpected list refusal: %+v", refusal)
			}
			after, refusal := call("owner", "GetTask", map[string]any{"id": started.Task.ID})
			var afterTask a2a.Task
			if refusal != nil || json.Unmarshal(after, &afterTask) != nil || afterTask.Status.State != a2a.TaskStateInputRequired || len(afterTask.History) != len(beforeTask.History) || probe.executions.Load() != 1 || probe.cancellations.Load() != 0 {
				t.Errorf("foreign requests changed owner task: executions=%d cancellations=%d history=%d->%d status=%s", probe.executions.Load(), probe.cancellations.Load(), len(beforeTask.History), len(afterTask.History), afterTask.Status.State)
			}
			result, refusal = call("owner", "ListTasks", map[string]any{"pageSize": 10})
			var ownList a2a.ListTasksResponse
			if refusal != nil || json.Unmarshal(result, &ownList) != nil || len(ownList.Tasks) != 1 {
				t.Errorf("owner cannot list its own task: refusal=%+v", refusal)
			}
		})
	}
}
