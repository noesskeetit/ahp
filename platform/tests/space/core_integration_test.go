package space_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// scriptedModel is a deterministic ModelClient driving a fixed tool-call
// sequence, then returning a final answer — exercising the real Core loop
// without an external LLM endpoint.
type scriptedModel struct {
	mu        sync.Mutex
	call      int
	responses []core.ModelResponse
}

func (m *scriptedModel) Generate(_ context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := m.call
	m.call++
	if i < len(m.responses) {
		r := m.responses[i]
		return &r, nil
	}
	return &core.ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}

type echoExecutor struct{ calls int64 }

func (e *echoExecutor) Execute(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name == "echo" {
		atomic.AddInt64(&e.calls, 1)
		return args, nil
	}
	return nil, context.DeadlineExceeded
}

func TestCoreRuntimeToolLoop(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	reg := core.NewToolRegistry(&echoExecutor{})
	reg.Register(core.ToolDef{Name: "echo", Description: "echoes"})

	model := &scriptedModel{responses: []core.ModelResponse{
		{
			Content:   json.RawMessage(`"calling echo"`),
			ToolCalls: []core.ToolCall{{ID: "tc1", Name: "echo", Args: json.RawMessage(`{"text":"hi"}`)}},
		},
		{Content: json.RawMessage(`"echo returned hi"`)},
	}}

	rt := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: reg, State: sc})
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: rt, WorkerID: "w-core"})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "core-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"hello"`), MaxSteps: 8,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var status space.TurnStatus
	for time.Now().Before(deadline) {
		if tn, ok := coord.TurnSnapshot(accept.TurnID); ok && tn.Status.IsTerminal() {
			status = tn.Status
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status != space.TurnDone {
		t.Fatalf("expected done, got %s", status)
	}

	ms.mu.Lock()
	intents := len(ms.intents)
	ms.mu.Unlock()
	if intents == 0 {
		t.Fatal("expected at least one recorded intent for the tool call")
	}
}

func TestCoreRuntimeCompaction(t *testing.T) {
	reg := core.NewToolRegistry(&echoExecutor{})
	comp := &countingCompactor{}
	model := &scriptedModel{responses: []core.ModelResponse{
		{Content: json.RawMessage(`"a"`)},
	}}
	rt := core.NewRuntime(core.RuntimeConfig{
		Model: model, Tools: reg, Compactor: comp, CompBudget: 10,
	})

	results := make(chan space.StepResult, 32)
	go func() {
		for range results {
		}
	}()
	req := &space.CoreRequest{
		TurnID: "turn-1", Messages: json.RawMessage(longHistory(50)),
		Input: json.RawMessage(`"go"`), MaxSteps: 4,
	}
	if err := rt.Run(context.Background(), req, results); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if comp.invoked.Load() == 0 {
		t.Fatal("expected compactor to be invoked")
	}
}

type countingCompactor struct{ invoked atomic.Int64 }

func (c *countingCompactor) Compact(_ context.Context, msgs []core.ChatMessage, budget int) ([]core.ChatMessage, int, error) {
	c.invoked.Add(1)
	approx := len(msgs) * 40
	if approx <= budget {
		return msgs, 0, nil
	}
	trimmed := msgs[len(msgs)/2:]
	return trimmed, approx - len(trimmed)*40, nil
}

func longHistory(n int) string {
	msgs := make([]core.ChatMessage, 0, n)
	pad, _ := json.Marshal("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	for i := 0; i < n; i++ {
		msgs = append(msgs, core.ChatMessage{Role: "user", Content: pad})
	}
	b, _ := json.Marshal(msgs)
	return string(b)
}
