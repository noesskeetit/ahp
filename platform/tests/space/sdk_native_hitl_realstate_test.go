//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

type nativeSDKExecutorSlot struct {
	current atomic.Pointer[sdk.LangGraphExecutor]
}

func (s *nativeSDKExecutorSlot) Execute(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return s.current.Load().Execute(ctx, ec)
}

func (s *nativeSDKExecutorSlot) Cancel(ctx context.Context, ec *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return s.current.Load().Cancel(ctx, ec)
}

// Actual upstream A2A server, signed JWT, ingress/State mTLS and PostgreSQL.
// The HTTP effect and Identity authorizer are labelled fixtures. The native
// taskstore is in memory, so this test does not claim process crash recovery.
func TestSDKNativeA2AHITLResumesSameRunRealState(t *testing.T) {
	testSDKNativeHITL(t, false)
}

// This discards the complete native handler/taskstore, not only the executor.
// It is an acceptance requirement for durable SDK hosting, not proof of an OS
// process restart; the separate process-kill leg must follow once this passes.
func TestSDKNativeA2AHITLResumesAfterColdHandlerRealState(t *testing.T) {
	testSDKNativeHITL(t, true)
}

func testSDKNativeHITL(t *testing.T, coldHandler bool) {
	t.Helper()
	e := cNewEnvProfile(t, coldHandler)
	const grant = "native-hitl-grant"
	token := e.signTask(t, grant)
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	var effects, graphs atomic.Int32
	effect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		_, _ = w.Write([]byte(`{"ticket":"fixture-ticket"}`))
	}))
	defer effect.Close()
	graph := func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		graphs.Add(1)
		if rc.Task.TaskID != grant {
			return nil, fmt.Errorf("grant changed during continuation")
		}
		checkpoint, err := rc.CheckpointSaver.GetLatest(ctx, "native-hitl")
		if contracts.Code(err) == contracts.ErrNotFound {
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, effect.URL, nil)
			response, err := effect.Client().Do(request)
			if err != nil {
				return nil, err
			}
			response.Body.Close()
			_, err = rc.CheckpointSaver.PutCheckpoint(ctx, "native-hitl", sdkstate.Checkpoint{ID: "before-human", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "phase", Version: "1", Data: []byte("ticket-created")}}}, "before-human")
			if err != nil {
				return nil, err
			}
			return nil, &sdk.InterruptError{WaitFor: "hitl_response", Payload: json.RawMessage(`{"question":"continue after fixture ticket?"}`)}
		}
		if err != nil {
			return nil, err
		}
		if checkpoint.ID != "before-human" || len(checkpoint.Parts) != 1 || string(checkpoint.Parts[0].Data) != "ticket-created" {
			return nil, fmt.Errorf("native checkpoint was not restored")
		}
		inbox, err := rc.Config.StateClient.ReadInbox(ctx, rc.Task.ThreadID, 0, 10)
		if err != nil {
			return nil, err
		}
		if len(inbox) != 1 || inbox[0].Kind != "hitl_response" {
			return nil, fmt.Errorf("native answer is not in durable inbox: %d events", len(inbox))
		}
		var answer struct {
			RunID     string `json:"run_id"`
			TaskID    string `json:"task_id"`
			MessageID string `json:"message_id"`
			Input     struct {
				Approved bool `json:"approved"`
			} `json:"input"`
		}
		if err := json.Unmarshal(inbox[0].Payload, &answer); err != nil || answer.RunID != rc.Task.RunID || answer.TaskID != grant || answer.MessageID != "answer-one" || !answer.Input.Approved {
			return nil, fmt.Errorf("inbox answer lost its run/message binding")
		}
		_, err = rc.CheckpointSaver.PutCheckpoint(ctx, "native-hitl", sdkstate.Checkpoint{ID: "after-human", Parent: checkpoint.ID, Version: 2, Parts: []sdkstate.CheckpointPart{{Channel: "phase", Version: "2", Data: []byte("approved")}}}, "after-human")
		if err != nil {
			return nil, err
		}
		// This test's graph explicitly persists its state before acknowledging.
		// Atomic checkpoint+ack and OS crash windows are not claimed by this leg.
		if !coldHandler {
			if err := rc.Config.StateClient.AckInbox(ctx, rc.Lease.Fence(), []string{inbox[0].ID}); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`{"approved":true}`), nil
	}
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "native-hitl", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, graph)
	if err != nil {
		t.Fatal(err)
	}
	slot := &nativeSDKExecutorSlot{}
	slot.current.Store(executor)
	newHandler := func(worker string) a2asrv.RequestHandler {
		t.Helper()
		if !coldHandler {
			return sdk.NewA2AHandler(slot)
		}
		handler, err := sdk.NewDurableA2AHandler(&sdk.Config{StateClient: client, Worker: worker, LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, graph)
		if err != nil {
			t.Fatal(err)
		}
		return handler
	}
	server := nativeSDKProtocolServer(t, e, a2asrv.NewJSONRPCHandler(newHandler("native-hitl")))
	send := func(message *a2a.Message) *a2a.Task {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": message.ID, "method": "SendMessage", "params": a2a.SendMessageRequest{Message: message, Config: &a2a.SendMessageConfig{}}})
		ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
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
		if err := json.NewDecoder(response.Body).Decode(&reply); err != nil || reply.Result.Task == nil {
			t.Fatalf("native HITL exchange failed: message=%s error=%s decode=%v", message.ID, reply.Error, err)
		}
		return reply.Result.Task
	}
	initial := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("original immutable input"))
	initial.ID, initial.ContextID = "initial-one", "ctx-1"
	first := send(initial)
	if first.Status.State != a2a.TaskStateInputRequired || first.Status.Message == nil || first.Status.Message.ID == "" {
		t.Fatalf("native task did not ask a stable question: %+v", first)
	}
	thread, err := client.GetThreadByContext(e.ctx, "ctx-1")
	if err != nil {
		t.Fatal(err)
	}
	runs, err := client.ListRuns(e.ctx, thread.ID)
	if err != nil || len(runs) != 1 || runs[0].Status != sdkstate.RunWaiting {
		t.Fatalf("native run did not suspend: %+v error=%v", runs, err)
	}
	runID, originalPayload, firstEpoch := runs[0].ID, append(json.RawMessage(nil), runs[0].Payload...), thread.Epoch
	// Drop the SDK executor's process-local maps while retaining the upstream
	// taskstore. This proves a fresh executor; it is not an OS-process restart.
	replacement, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "native-hitl-replacement", LeaseTTL: time.Second, HeartbeatInterval: 20 * time.Millisecond}, graph)
	if err != nil {
		t.Fatal(err)
	}
	slot.current.Store(replacement)
	if coldHandler {
		server.Close()
		server = nativeSDKProtocolServer(t, e, a2asrv.NewJSONRPCHandler(newHandler("native-hitl-replacement")))
		// An initial response lost before reaching the caller is replayed to
		// the original task/question, without executing the graph again.
		replay := send(initial)
		if replay.ID != first.ID || replay.Status.Message == nil || replay.Status.Message.ID != first.Status.Message.ID || graphs.Load() != 1 {
			t.Fatal("fresh handler did not recover original pending question")
		}
	}
	answer := a2a.NewMessage(a2a.MessageRoleUser, a2a.NewDataPart(map[string]any{"approved": true}))
	answer.ID, answer.ContextID, answer.TaskID = "answer-one", "ctx-1", first.ID
	second := send(answer)
	if second.ID != first.ID || second.Status.State != a2a.TaskStateCompleted {
		raw, _ := json.Marshal(second)
		t.Fatalf("native answer did not continue its original task: %s graphs=%d fixture-effects=%d", raw, graphs.Load(), effects.Load())
	}
	runs, err = client.ListRuns(e.ctx, thread.ID)
	inputUnchanged := len(runs) == 1 && bytes.Equal(runs[0].Payload, originalPayload)
	if coldHandler && len(runs) == 1 {
		// Atomic State completion journals close_reason in run.payload. The
		// immutable native input must remain byte-identical within that JSONB.
		var before, after map[string]json.RawMessage
		if json.Unmarshal(originalPayload, &before) != nil || json.Unmarshal(runs[0].Payload, &after) != nil {
			t.Fatal("invalid run payload")
		}
		inputUnchanged = bytes.Equal(before["native_input"], after["native_input"]) && string(after["close_reason"]) == `"graph_completed"`
	}
	if err != nil || len(runs) != 1 || runs[0].ID != runID || runs[0].Status != sdkstate.RunDone || !inputUnchanged {
		t.Fatalf("continuation replaced its original run/input: %+v error=%v", runs, err)
	}
	thread, err = client.GetThread(e.ctx, thread.ID)
	if err != nil || thread.Epoch <= firstEpoch || effects.Load() != 1 || graphs.Load() != 2 {
		t.Fatalf("native continuation lost fencing or repeated work: epoch=%d first=%d effects=%d graphs=%d error=%v", thread.Epoch, firstEpoch, effects.Load(), graphs.Load(), err)
	}
	inbox, err := client.ReadInbox(e.ctx, thread.ID, 0, 10)
	if err != nil || len(inbox) != 0 {
		t.Fatalf("processed answer was not acknowledged: %d events error=%v", len(inbox), err)
	}
	if coldHandler {
		server.Close()
		server = nativeSDKProtocolServer(t, e, a2asrv.NewJSONRPCHandler(newHandler("native-hitl-terminal-reader")))
		for _, message := range []*a2a.Message{initial, answer} {
			replay := send(message)
			if replay.ID != first.ID || replay.Status.State != a2a.TaskStateCompleted || graphs.Load() != 2 || effects.Load() != 1 {
				t.Fatal("terminal replay restarted graph/effect")
			}
		}
	}
	t.Logf("actual A2A1.0 + State mTLS/PG: initial -> input-required -> new SDK executor -> same-task continuation -> completed; one run, immutable input, epoch increased, checkpoint restored, fixture effect once; cold-handler=%t, no OS crash claim", coldHandler)
}
