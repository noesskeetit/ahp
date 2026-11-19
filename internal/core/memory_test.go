package core

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type testMemoryDriver struct{ prepared, executed int }

func (d *testMemoryDriver) Prepare(context.Context, *space.CoreRequest) ([]ChatMessage, error) {
	d.prepared++
	return []ChatMessage{{Role: "user", Content: json.RawMessage(`"fresh memory"`)}}, nil
}
func (d *testMemoryDriver) Defs() []ToolDef {
	return []ToolDef{{Name: "memory_search", Parameters: json.RawMessage(`{"type":"object"}`)}}
}
func (d *testMemoryDriver) Handles(name string) bool { return name == "memory_search" }
func (d *testMemoryDriver) Execute(ctx context.Context, req *space.CoreRequest, call ToolCall) (json.RawMessage, error) {
	d.executed++
	return json.RawMessage(`{"facts":[]}`), nil
}

type memoryInspectModel struct {
	t     *testing.T
	calls int
}

func (m *memoryInspectModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.calls++
	fresh := 0
	for _, msg := range req.Messages {
		if msg.Source != "" {
			m.t.Fatal("checkpoint metadata reached provider", msg.Source)
		}
		if strings.Contains(string(msg.Content), "stale memory") {
			m.t.Fatal("old injection survived resume")
		}
		if strings.Contains(string(msg.Content), "fresh memory") {
			fresh++
		}
	}
	if fresh != 1 {
		m.t.Fatal("memory was missing or duplicated", fresh)
	}
	if len(req.MemoryMessages) != 1 {
		m.t.Fatal("immutable request lost memory provenance", req.MemoryMessages)
	}
	if len(req.Tools) != 1 || req.Tools[0].Name != "memory_search" {
		m.t.Fatal(req.Tools)
	}
	if m.calls == 1 {
		return &ModelResponse{Content: json.RawMessage(`"search"`), ToolCalls: []ToolCall{{ID: "m1", Name: "memory_search", Args: json.RawMessage(`{"scope":"personal","query":"preference"}`)}}}, nil
	}
	return &ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}
func TestMemoryInjectedOnceOnResumeAndToolsWorkWithoutRegistry(t *testing.T) {
	driver := &testMemoryDriver{}
	model := &memoryInspectModel{t: t}
	runtime := NewRuntime(RuntimeConfig{Model: model, Memory: driver})
	prior, _ := json.Marshal([]ChatMessage{{Role: "user", Content: json.RawMessage(`"stale memory"`), Source: memorySource}, {Role: "user", Content: json.RawMessage(`"actual question"`)}})
	results := make(chan space.StepResult, 20)
	request := &space.CoreRequest{Input: json.RawMessage(`"question"`), Messages: prior, Checkpoint: &space.Checkpoint{}}
	if err := runtime.Run(context.Background(), request, results); err != nil {
		t.Fatal(err)
	}
	if driver.prepared != 1 || driver.executed != 1 || model.calls != 2 {
		t.Fatal(driver, model.calls)
	}
}

func TestMemoryReplayKeepsImmutableContextWithoutFreshRecall(t *testing.T) {
	driver := &testMemoryDriver{}
	model := &memoryInspectModel{t: t}
	runtime := NewRuntime(RuntimeConfig{Model: model, Memory: driver})
	messages := []ChatMessage{{Role: "user", Content: json.RawMessage(`"fresh memory"`), Source: memorySource}, {Role: "user", Content: json.RawMessage(`"actual question"`)}}
	original := ModelRequest{Messages: providerMessages(messages), MemoryMessages: memoryMessageIndices(messages), Tools: driver.Defs()}
	raw, _ := json.Marshal(original)
	results := make(chan space.StepResult, 20)
	if err := runtime.Run(context.Background(), &space.CoreRequest{ModelReplay: &space.ModelReplay{Request: raw}}, results); err != nil {
		t.Fatal(err)
	}
	if driver.prepared != 0 || driver.executed != 1 || model.calls != 2 {
		t.Fatal("replay performed new recall or changed model context", driver, model.calls)
	}
	restored := restoreMemoryMessages(original.Messages, original.MemoryMessages)
	if len(withoutMemory(restored)) != 1 {
		t.Fatal("journal lost memory checkpoint provenance")
	}
}

type revokedReplayMemory struct {
	testMemoryDriver
	validated int
}

func (d *revokedReplayMemory) ValidateProject(context.Context, *space.CoreRequest) error {
	d.validated++
	return contracts.NewError(contracts.ErrForbidden, "project membership revoked")
}

func TestMemoryReplayCannotBypassCurrentProjectAuthorization(t *testing.T) {
	driver := &revokedReplayMemory{}
	model := &memoryInspectModel{t: t}
	runtime := NewRuntime(RuntimeConfig{Model: model, Memory: driver})
	messages := []ChatMessage{{Role: "user", Content: json.RawMessage(`"fresh memory"`), Source: memorySource}, {Role: "user", Content: json.RawMessage(`"actual question"`)}}
	original := ModelRequest{Messages: providerMessages(messages), MemoryMessages: memoryMessageIndices(messages), Tools: driver.Defs()}
	raw, _ := json.Marshal(original)
	err := runtime.Run(t.Context(), &space.CoreRequest{ProjectID: "selected", ProjectMemoryProfile: "shared", ModelReplay: &space.ModelReplay{Request: raw}}, make(chan space.StepResult, 20))
	if contracts.Code(err) != contracts.ErrForbidden || driver.validated != 1 || driver.prepared != 0 || model.calls != 0 {
		t.Fatalf("cached model request bypassed live project authorization: err=%v validated=%d recall=%d model=%d", err, driver.validated, driver.prepared, model.calls)
	}
}

type memoryGateModel struct{ calls int }

func (m *memoryGateModel) Generate(context.Context, *ModelRequest) (*ModelResponse, error) {
	m.calls++
	return &ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}

func TestSharedMemoryReplayRequiresAnAccessValidator(t *testing.T) {
	for _, test := range []struct {
		name   string
		memory MemoryDriver
	}{{"disabled", nil}, {"no_replay_authority", &testMemoryDriver{}}} {
		t.Run(test.name, func(t *testing.T) {
			model := &memoryGateModel{}
			runtime := NewRuntime(RuntimeConfig{Model: model, Memory: test.memory})
			raw, _ := json.Marshal(ModelRequest{Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"cached project data"`)}}})
			err := runtime.Run(t.Context(), &space.CoreRequest{ProjectID: "selected", ProjectMemoryProfile: "shared", ModelReplay: &space.ModelReplay{Request: raw}}, make(chan space.StepResult, 20))
			if contracts.Code(err) != contracts.ErrUnavailable || model.calls != 0 {
				t.Fatalf("shared replay continued without authority: model=%d err=%v", model.calls, err)
			}
		})
	}
}
