//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// Actual pinned A2A HTTP implementation, signed task JWT, ingress/State mTLS,
// native SDK and real PostgreSQL. Identity authorization is the cEnv fixture;
// no model, durable A2A taskstore or crash/HITL recovery is claimed here.
func TestSDKNativeA2AHTTPWritesOwnGrantCheckpointRealState(t *testing.T) {
	e := cNewEnv(t)
	const grant = "native-http-grant"
	token := e.signTask(t, grant)
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var graphs atomic.Int32
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "native-http-worker", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		graphs.Add(1)
		if rc.Task.TaskID != grant || rc.Task.ContextID != "ctx-1" {
			t.Errorf("graph authority/context differs: task=%s context=%s", rc.Task.TaskID, rc.Task.ContextID)
		}
		_, err := rc.CheckpointSaver.PutCheckpoint(ctx, "native-http", sdkstate.Checkpoint{ID: "native-http-result", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "result", Version: "1", Data: []byte("native-http-checkpoint")}}}, "native-http-result")
		return json.RawMessage(`{"ok":true}`), err
	})
	if err != nil {
		t.Fatal(err)
	}
	wire := a2asrv.NewJSONRPCHandler(sdk.NewA2AHandler(executor))
	server := nativeSDKProtocolServer(t, e, wire)
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("native HTTP checkpoint"))
	message.ContextID = "ctx-1"
	params := a2a.SendMessageRequest{Message: message, Config: &a2a.SendMessageConfig{}, Metadata: map[string]any{"ahp": map[string]string{"task_id": "untrusted-body-task"}}}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "native-real-state", "method": "SendMessage", "params": params})
	request, _ := http.NewRequestWithContext(e.ctx, http.MethodPost, server.URL, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("A2A-Version", "1.0")
	request.Header.Set("Authorization", "Bearer "+token)
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
	if reply.Result.Task == nil || reply.Result.Task.Status.State != a2a.TaskStateCompleted || graphs.Load() != 1 || string(reply.Result.Task.ID) == grant {
		t.Fatalf("native HTTP did not complete with distinct task/grant: task=%+v graphs=%d error=%s", reply.Result.Task, graphs.Load(), reply.Error)
	}
	// Valid signatures for other subjects/grants/contexts must still be refused
	// by the native owner binding, before any local task history is changed.
	type rpcRefusal struct {
		Code int `json:"code"`
	}
	call := func(bearer, method string, params any) (json.RawMessage, *rpcRefusal) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "owner-check", "method": method, "params": params})
		ctx, cancel := context.WithTimeout(e.ctx, 3*time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("A2A-Version", "1.0")
		request.Header.Set("Authorization", "Bearer "+bearer)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result struct {
			Result json.RawMessage `json:"result"`
			Error  *rpcRefusal     `json:"error"`
		}
		if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&result) != nil {
			t.Fatal("signed native owner probe failed before its A2A boundary")
		}
		return result.Result, result.Error
	}
	ownerClaims := auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject, Task: grant, Root: "run-1", Context: "ctx-1", Grant: "grant-1"}
	for _, dimension := range []string{"subject", "task", "context"} {
		claims := ownerClaims
		switch dimension {
		case "subject":
			claims.Subject = "other-user"
		case "task":
			claims.Task = "other-grant"
		case "context":
			claims.Context = "other-context"
		}
		foreignToken, err := e.signer.Sign(claims, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range []string{"GetTask", "CancelTask", "SendMessage"} {
			var params any = map[string]any{"id": reply.Result.Task.ID}
			if method == "SendMessage" {
				message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("foreign answer"))
				message.TaskID, message.ContextID = reply.Result.Task.ID, "ctx-1"
				message.Metadata = map[string]any{"subject": cSubject, "task_id": grant}
				params = a2a.SendMessageRequest{Message: message}
			}
			_, failure := call(foreignToken, method, params)
			if failure == nil || failure.Code != -32001 && failure.Code != -31403 {
				t.Fatalf("signed foreign %s reached native %s: %+v", dimension, method, failure)
			}
		}
		data, failure := call(foreignToken, "ListTasks", map[string]any{"pageSize": 10})
		var list a2a.ListTasksResponse
		if failure != nil || json.Unmarshal(data, &list) != nil || len(list.Tasks) != 0 || list.TotalSize != 0 {
			t.Fatalf("signed foreign %s listed native owner tasks", dimension)
		}
	}
	renewedToken, err := e.signer.Sign(ownerClaims, 4*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	current, refusal := call(renewedToken, "GetTask", map[string]any{"id": reply.Result.Task.ID})
	var currentTask a2a.Task
	if refusal != nil || json.Unmarshal(current, &currentTask) != nil || currentTask.ID != reply.Result.Task.ID || currentTask.Status.State != a2a.TaskStateCompleted || len(currentTask.History) != len(reply.Result.Task.History) || graphs.Load() != 1 {
		t.Fatal("foreign requests changed native history or a fresh owner JWT lost task ownership")
	}
	thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	runs, err := client.ListRuns(e.ctx, thread.ID)
	if err != nil || len(runs) != 1 || runs[0].TaskID != grant || runs[0].Status != sdkstate.RunDone {
		t.Fatalf("State did not retain the authenticated grant: runs=%+v err=%v", runs, err)
	}
	replayed, err := client.CreateRun(e.ctx, thread.ID, "task:"+string(reply.Result.Task.ID), runs[0].Payload)
	if err != nil || replayed.ID != runs[0].ID {
		t.Fatalf("native task did not replay its existing run: err=%v run=%s", err, replayed.ID)
	}
	part, err := client.GetPart(e.ctx, thread.ID, "native-http", "result", "1")
	if err != nil || string(part) != "native-http-checkpoint" {
		t.Fatalf("native checkpoint bytes missing: err=%v bytes=%d", err, len(part))
	}
	t.Logf("actual A2A1.0 server + ingress/State mTLS + PG: opaque task=%s, signed foreign subject/grant/context denied, owner with fresh JWT retained ownership/history, checkpoint exact, graphs=1; Identity authorization is a fixture, no LLM/crash claim", reply.Result.Task.ID)
}
