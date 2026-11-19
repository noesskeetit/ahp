//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/sdk"
	"github.com/ahp-platform/ahp/pkg/transport"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// A misconfigured shared host must not execute Bob's signed request using a
// State client pinned to Alice, even when their task/context names coincide.
// Real A2A/mTLS/State/PG; Identity policy is the explicit cEnv fixture.
func TestSDKNativeA2ARejectsForeignStateCredentialRealState(t *testing.T) {
	e := cNewEnv(t)
	const grant = "shared-grant-name"
	alice := e.signTask(t, grant)
	bob, err := e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: "bob", Task: grant, Context: "ctx-1", Root: "run-1", Grant: "grant-1"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return alice, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var graphs atomic.Int32
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "shared-scope-worker", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, func(context.Context, *sdk.RunContext) (json.RawMessage, error) {
		graphs.Add(1)
		return json.RawMessage(`{"wrong_subject_execution":true}`), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	server := nativeSDKProtocolServer(t, e, a2asrv.NewJSONRPCHandler(sdk.NewA2AHandler(executor)))
	message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("Bob must not execute as Alice"))
	message.ContextID = "ctx-1"
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "scope-probe", "method": "SendMessage", "params": a2a.SendMessageRequest{Message: message}})
	request, _ := http.NewRequestWithContext(e.ctx, http.MethodPost, server.URL, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("A2A-Version", "1.0")
	request.Header.Set("Authorization", "Bearer "+bob)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var reply struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.NewDecoder(response.Body).Decode(&reply) != nil {
		t.Fatal("invalid scope probe reply")
	}
	if graphs.Load() != 0 || reply.Error == nil || reply.Error.Code != -31403 {
		t.Errorf("foreign State credential reached graph: graphs=%d refusal=%+v", graphs.Load(), reply.Error)
	}
	thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	runs, err := client.ListRuns(e.ctx, thread.ID)
	if err != nil || len(runs) != 0 {
		t.Fatalf("Bob's input created a run under Alice: runs=%d err=%v", len(runs), err)
	}
}

// Two simultaneous native executions share one executor and one State client.
// Their task/context names deliberately coincide; only the signed subject
// distinguishes their State scope. The callback is not Python LangGraph/LLM.
func TestSDKNativeA2ASharedIngressRequestCredentialsRealState(t *testing.T) {
	e := cNewEnv(t)
	const grant = "shared-concurrent-grant"
	var graphs atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: sdk.NativeTaskTokenSource})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "shared-native-worker", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		principal, ok := transport.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("graph lost verified principal")
		}
		graphs.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		_, err := rc.CheckpointSaver.PutCheckpoint(ctx, "shared-native", sdkstate.Checkpoint{ID: "shared-result", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "result", Version: "1", Data: []byte("checkpoint:" + principal.Scope.Subject)}}}, "shared-result")
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"subject": principal.Scope.Subject, "task": rc.Task.TaskID})
	})
	if err != nil {
		t.Fatal(err)
	}
	server := nativeSDKProtocolServer(t, e, a2asrv.NewJSONRPCHandler(sdk.NewA2AHandler(executor)))
	type result struct {
		subject, token string
		task           *a2a.Task
		err            error
	}
	results := make(chan result, 2)
	for _, subject := range []string{cSubject, "bob"} {
		claims := auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: subject, Task: grant, Context: "ctx-1", Root: "run-1", Grant: "grant-1"}
		token, err := e.signer.Sign(claims, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		go func(subject, token string) {
			out := result{subject: subject, token: token}
			defer func() { results <- out }()
			message := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("input:"+subject))
			message.ContextID = "ctx-1"
			body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": subject, "method": "SendMessage", "params": a2a.SendMessageRequest{Message: message}})
			if err != nil {
				out.err = err
				return
			}
			ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
			if err != nil {
				out.err = err
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("A2A-Version", "1.0")
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := server.Client().Do(request)
			if err != nil {
				out.err = err
				return
			}
			defer response.Body.Close()
			var reply struct {
				Result struct {
					Task *a2a.Task `json:"task"`
				} `json:"result"`
				Error json.RawMessage `json:"error"`
			}
			if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
				out.err = err
				return
			}
			if response.StatusCode != http.StatusOK || reply.Result.Task == nil || reply.Result.Task.Status.State != a2a.TaskStateCompleted {
				out.err = fmt.Errorf("native request did not complete: HTTP %d", response.StatusCode)
				return
			}
			out.task = reply.Result.Task
		}(subject, token)
	}
	for range 2 {
		select {
		case <-entered:
		case failed := <-results:
			t.Fatalf("shared graph did not enter: %v", failed.err)
		case <-time.After(3 * time.Second):
			t.Fatal("two graphs did not overlap")
		}
	}
	// Both graphs have entered before either may checkpoint and finish.
	release <- struct{}{}
	release <- struct{}{}
	threads := make([]sdkstate.Thread, 0, 2)
	readers := make([]*sdkstate.Client, 0, 2)
	for range 2 {
		out := <-results
		if out.err != nil {
			t.Fatal(out.err)
		}
		reader := client.WithTokenSource(func(context.Context) (string, error) { return out.token, nil })
		thread, err := reader.GetThreadByContext(e.ctx, "ctx-1")
		if err != nil || thread.Scope.Subject != out.subject {
			t.Fatalf("wrong durable scope: subject=%s error=%v", out.subject, err)
		}
		runs, err := reader.ListRuns(e.ctx, thread.ID)
		if err != nil || len(runs) != 1 || runs[0].TaskID != grant || runs[0].Status != sdkstate.RunDone {
			t.Fatalf("wrong durable run for %s: count=%d error=%v", out.subject, len(runs), err)
		}
		part, err := reader.GetPart(e.ctx, thread.ID, "shared-native", "result", "1")
		if err != nil || string(part) != "checkpoint:"+out.subject {
			t.Fatalf("checkpoint crossed subjects: subject=%s error=%v", out.subject, err)
		}
		encoded, err := json.Marshal(out.task)
		other := "bob"
		if out.subject == "bob" {
			other = cSubject
		}
		if err != nil || bytes.Contains(encoded, []byte("input:"+other)) {
			t.Fatal("native task history crossed subjects")
		}
		threads = append(threads, thread)
		readers = append(readers, reader)
	}
	if graphs.Load() != 2 || threads[0].ID == threads[1].ID {
		t.Fatal("shared ingress did not retain two independent execution scopes")
	}
	for i := range readers {
		_, err := readers[i].GetThread(e.ctx, threads[1-i].ID)
		if code := contracts.Code(err); code != contracts.ErrNotFound && code != contracts.ErrForbidden {
			t.Fatalf("cross-subject State read was not refused: %v", err)
		}
	}
	if _, err := client.GetThreadByContext(e.ctx, "ctx-1"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("shared State client accepted an unverified background credential")
	}
	t.Log("two overlapping signed subjects, same grant/context names, one SDK/State client, actual ingress+State mTLS/PG: independent run/checkpoint/history, cross-read denied; no inference/process-recovery claim")
}
