//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The Model boundary is recorded deterministically; State, HTTP/mTLS, the Core
// loop, checkpoints and durable transcript are real. This is not an LLM claim.
type conversationModel struct{ calls chan []core.ChatMessage }

func (m *conversationModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	m.calls <- append([]core.ChatMessage(nil), req.Messages...)
	return &core.ModelResponse{Content: json.RawMessage(`"ASSISTANT-FIRST: код принят"`), Stop: "stop"}, nil
}

func waitConversationDone(t *testing.T, e *cEnv, token, runID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := e.client.GetRun(e.ctx, token, runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == "done" {
			return
		}
		if run.Status == "failed" || run.Status == "canceled" {
			t.Fatalf("run ended %s", run.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("run did not complete")
}

func TestConversationHistoryReachesModelRealState(t *testing.T) {
	for _, scenario := range []struct {
		name             string
		cold, noSnapshot bool
	}{
		{name: "warm"}, {name: "cold_worker", cold: true}, {name: "cold_without_snapshot", cold: true, noSnapshot: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			e := cNewEnv(t)
			token := e.signTask(t, "conversation")
			scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
			model := &conversationModel{calls: make(chan []core.ChatMessage, 8)}
			stateClient := e.client
			if scenario.noSnapshot {
				// Fault-injection proxy drops snapshot writes before reaching real
				// State; the authoritative transcript remains untouched.
				proxy := opFlakyProxy(t, e, "PutCheckpoint", "", 100)
				stateClient = space.NewStateClient(proxy.URL, nil)
			}
			makeCoordinator := func() *space.Coordinator {
				return space.NewCoordinator(space.CoordinatorConfig{StateClient: stateClient, Core: core.NewRuntime(core.RuntimeConfig{Model: model, State: e.client}), WorkerID: contracts.NewID()})
			}
			coord := makeCoordinator()
			first, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "first", ContextID: "ctx-1", Input: json.RawMessage(`"Запомни код БРАУЗЕР-17."`), MaxSteps: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
				t.Fatal(err)
			}
			waitConversationDone(t, e, token, first.TurnID)
			<-model.calls
			second, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "second", ThreadID: first.ThreadID, Input: json.RawMessage(`"Какой код из предыдущего сообщения?"`), MaxSteps: 2})
			if err != nil {
				t.Fatal(err)
			}
			if scenario.noSnapshot {
				checkpoints, err := e.client.ListCheckpoints(e.ctx, token, first.ThreadID, space.CheckpointNamespace, 10)
				if err != nil || len(checkpoints) != 0 {
					t.Fatalf("snapshot failure precondition: checkpoints=%d err=%v", len(checkpoints), err)
				}
			}
			stateClient = e.client
			if scenario.cold {
				coord = makeCoordinator()
			}
			if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
				t.Fatal(err)
			}
			waitConversationDone(t, e, token, second.TurnID)
			messages := <-model.calls
			if len(messages) != 3 || messages[0].Role != "user" || !strings.Contains(string(messages[0].Content), "БРАУЗЕР-17") || messages[1].Role != "assistant" || !strings.Contains(string(messages[1].Content), "ASSISTANT-FIRST") || messages[2].Role != "user" || !strings.Contains(string(messages[2].Content), "Какой код") {
				t.Fatalf("second model request lost history or current input: %+v", messages)
			}
			// A subsequent run must preserve both user turns exactly once.
			third, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "third", ThreadID: first.ThreadID, Input: json.RawMessage(`"Повтори код ещё раз."`), MaxSteps: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
				t.Fatal(err)
			}
			waitConversationDone(t, e, token, third.TurnID)
			messages = <-model.calls
			if len(messages) != 5 || !strings.Contains(string(messages[0].Content), "БРАУЗЕР-17") || !strings.Contains(string(messages[2].Content), "Какой код") || !strings.Contains(string(messages[4].Content), "Повтори код") {
				t.Fatalf("third model history not ordered: %+v", messages)
			}
			// A new conversation owned by the same person has no prior messages.
			separate, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "separate", ContextID: "another-conversation", Input: json.RawMessage(`"New unrelated chat"`), MaxSteps: 2})
			if err != nil {
				t.Fatal(err)
			}
			if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
				t.Fatal(err)
			}
			waitConversationDone(t, e, token, separate.TurnID)
			messages = <-model.calls
			if len(messages) != 1 || strings.Contains(string(messages[0].Content), "БРАУЗЕР-17") {
				t.Fatalf("another conversation leaked history: %+v", messages)
			}
		})
	}
}

func TestConversationHistoryReadFailureStopsBeforeModelRealState(t *testing.T) {
	e := cNewEnv(t)
	token := e.signTask(t, "conversation")
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	model := &conversationModel{calls: make(chan []core.ChatMessage, 8)}
	runtime := core.NewRuntime(core.RuntimeConfig{Model: model, State: e.client})
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: runtime, WorkerID: "first-worker"})
	first, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "first", ContextID: "ctx-1", Input: json.RawMessage(`"SECRET-HISTORY"`), MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := coord.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	waitConversationDone(t, e, token, first.TurnID)
	<-model.calls
	second, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "second", ThreadID: first.ThreadID, Input: json.RawMessage(`"Remember the previous message"`), MaxSteps: 2})
	if err != nil {
		t.Fatal(err)
	}
	proxy := opFlakyProxy(t, e, "ListRuns", "", 100)
	cold := space.NewCoordinator(space.CoordinatorConfig{StateClient: space.NewStateClient(proxy.URL, nil), Core: runtime, WorkerID: "second-worker"})
	if err := cold.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := e.client.GetRun(e.ctx, token, second.TurnID)
		if err != nil {
			t.Fatal(err)
		}
		if run.Status == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("history read refusal did not fail run: %s", run.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case messages := <-model.calls:
		t.Fatalf("model ran with incomplete history: %+v", messages)
	default:
	}
	snap, ok := cold.TurnSnapshot(second.TurnID)
	if !ok || !strings.Contains(snap.FailureReason, "conversation history read failed") {
		t.Fatalf("missing durable-read failure reason: %+v", snap)
	}
}
