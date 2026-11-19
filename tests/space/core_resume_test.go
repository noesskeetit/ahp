package space_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
)

// capturingFinishModel records the exact message history handed to the model on
// its first Generate call and immediately returns a final answer (no tool
// calls), so the Core loop terminates after one step. This makes the
// resume-from-checkpoint message-assembly contract observable deterministically.
type capturingFinishModel struct {
	mu    sync.Mutex
	calls int
	first []core.ChatMessage
}

func (m *capturingFinishModel) Generate(_ context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		m.first = append([]core.ChatMessage(nil), req.Messages...)
	}
	return &core.ModelResponse{Content: json.RawMessage(`"final answer"`)}, nil
}

func (m *capturingFinishModel) observed() []core.ChatMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.first
}

func resumeRegistry() *core.ToolRegistry {
	reg := core.NewToolRegistry(&echoExecutor{})
	reg.Register(core.ToolDef{Name: "echo", Description: "echoes"})
	return reg
}

// runOnce drives the runtime to completion synchronously (the fake model
// finalises on the first step) and returns the terminal step results.
func runOnce(t *testing.T, rt *core.Runtime, req *space.CoreRequest) []space.StepResult {
	t.Helper()
	results := make(chan space.StepResult, 64)
	if err := rt.Run(context.Background(), req, results); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return drain(results)
}

// TestCoreRuntimeResumeFromCheckpoint proves contract item 2 "resume" at the
// Core model/tool loop level:
//   - a fresh turn appends the user input to the (empty) history;
//   - a resumed turn fed a checkpoint whose Messages already carry the turn
//     history must NOT re-append the user input (no duplicate user message) and
//     must continue from the persisted history to completion.
func TestCoreRuntimeResumeFromCheckpoint(t *testing.T) {
	t.Run("fresh_appends_input", func(t *testing.T) {
		m := &capturingFinishModel{}
		rt := core.NewRuntime(core.RuntimeConfig{Model: m, Tools: resumeRegistry()})
		req := &space.CoreRequest{TurnID: "t-fresh", Input: json.RawMessage(`"hello"`), MaxSteps: 4}
		got := runOnce(t, rt, req)

		obs := m.observed()
		if len(obs) != 1 {
			t.Fatalf("fresh run must send exactly the user message, got %d: %+v", len(obs), obs)
		}
		if obs[0].Role != "user" {
			t.Fatalf("fresh history must start with the user input, got role %q", obs[0].Role)
		}
		var text string
		_ = json.Unmarshal(obs[0].Content, &text)
		if text != "hello" {
			t.Fatalf("fresh input must be %q, got %q", "hello", text)
		}
		if !containsDone(got) {
			t.Fatal("fresh run must reach a terminal StepDone")
		}
	})

	t.Run("resume_does_not_duplicate_input", func(t *testing.T) {
		// Mid-execution history: the user question, an assistant tool request,
		// and the tool's result. On resume the loop must continue from here.
		hist := []core.ChatMessage{
			cmsg("user", "original question"),
			{
				Role:      "assistant",
				Content:   json.RawMessage(`"need a tool"`),
				ToolCalls: json.RawMessage(`[{"id":"tc-1","name":"echo","args":{}}]`),
			},
			{Role: "tool", Content: json.RawMessage(`"tool output"`), ToolCallID: "tc-1"},
		}
		hb, err := json.Marshal(hist)
		if err != nil {
			t.Fatalf("marshal history: %v", err)
		}
		cp := &space.Checkpoint{ID: "cp-1", Messages: hb}

		m := &capturingFinishModel{}
		rt := core.NewRuntime(core.RuntimeConfig{Model: m, Tools: resumeRegistry()})
		// Input intentionally equals the first history message so any duplicate
		// re-append would surface as a second user message.
		req := &space.CoreRequest{
			TurnID:     "t-resume",
			Input:      json.RawMessage(`"original question"`),
			Messages:   hb,
			Checkpoint: cp,
			MaxSteps:   4,
		}
		got := runOnce(t, rt, req)

		obs := m.observed()
		if len(obs) != len(hist) {
			t.Fatalf("resume must reuse persisted history exactly (%d msgs), got %d: %+v", len(hist), len(obs), obs)
		}
		users := 0
		for _, msg := range obs {
			if msg.Role == "user" {
				users++
			}
		}
		if users != 1 {
			t.Fatalf("resume must not re-append user input; saw %d user messages", users)
		}
		var firstText string
		_ = json.Unmarshal(obs[0].Content, &firstText)
		if firstText != "original question" {
			t.Fatalf("resumed history must lead with persisted user turn, got %q", firstText)
		}
		if obs[len(obs)-1].Role != "tool" || obs[len(obs)-1].ToolCallID != "tc-1" {
			t.Fatal("resumed history must end with the persisted tool result to continue from it")
		}
		if !containsDone(got) {
			t.Fatal("resumed run must reach a terminal StepDone after the model finalises")
		}
	})
}

func containsDone(rs []space.StepResult) bool {
	for _, r := range rs {
		if r.StepType == space.StepDone && r.Done {
			return true
		}
	}
	return false
}
