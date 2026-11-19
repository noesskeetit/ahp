package space_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func cmsg(role, text string) core.ChatMessage {
	c, _ := json.Marshal(text)
	return core.ChatMessage{Role: role, Content: c}
}

func TestTokenCompactorTrimsMiddle(t *testing.T) {
	c := core.NewTokenCompactor(200)
	msgs := []core.ChatMessage{cmsg("system", "you are helpful")}
	for i := 0; i < 40; i++ {
		msgs = append(msgs, cmsg("user", strings.Repeat("filler", 40)))
	}
	msgs = append(msgs, cmsg("user", "the final question"))

	out, saved, err := c.Compact(context.Background(), msgs, 200)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if saved <= 0 {
		t.Fatalf("expected positive savings, got %d", saved)
	}
	if out[0].Role != "system" {
		t.Fatal("system prompt must be preserved first")
	}
	if len(out) >= len(msgs) {
		t.Fatalf("expected history to shrink: %d -> %d", len(msgs), len(out))
	}
	var lastText string
	_ = json.Unmarshal(out[len(out)-1].Content, &lastText)
	if lastText != "the final question" {
		t.Fatalf("most recent message must be kept, got %q", lastText)
	}
	if core.EstimateHistory(out) > 200 {
		t.Fatalf("compacted history %d exceeds budget", core.EstimateHistory(out))
	}
}

func TestTokenCompactorNoopUnderBudget(t *testing.T) {
	c := core.NewTokenCompactor(10000)
	msgs := []core.ChatMessage{cmsg("system", "s"), cmsg("user", "hi")}
	out, saved, err := c.Compact(context.Background(), msgs, 10000)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if saved != 0 || len(out) != len(msgs) {
		t.Fatalf("expected no-op, got saved=%d len=%d", saved, len(out))
	}
}

func TestSkillRegistryDispatchAndScope(t *testing.T) {
	sr := core.NewSkillRegistry()

	scoped := &core.Skill{
		Name:    "scoped",
		Visible: func(s contracts.Scope) bool { return s.Tenant == "acme" },
		Tools:   []core.ToolDef{{Name: "scoped_tool", Description: "only acme"}},
		Execute: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			return args, nil
		},
	}
	global := &core.Skill{
		Name:  "global",
		Tools: []core.ToolDef{{Name: "global_echo", Description: "echo"}},
		Execute: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			return args, nil
		},
	}
	if err := sr.Register(global); err != nil {
		t.Fatalf("register global: %v", err)
	}
	if err := sr.Register(scoped); err != nil {
		t.Fatalf("register scoped: %v", err)
	}

	dup := &core.Skill{Name: "dup", Tools: []core.ToolDef{{Name: "global_echo"}},
		Execute: func(context.Context, string, json.RawMessage) (json.RawMessage, error) { return nil, nil }}
	if err := sr.Register(dup); err == nil {
		t.Fatal("expected duplicate tool registration to fail")
	}

	if defs := sr.ToolDefs(contracts.Scope{Tenant: "other"}); len(defs) != 1 || defs[0].Name != "global_echo" {
		t.Fatalf("unexpected defs for other scope: %+v", defs)
	}
	if len(sr.ToolDefs(contracts.Scope{Tenant: "acme"})) != 2 {
		t.Fatal("acme should see both tools")
	}

	res, err := sr.Execute(context.Background(), "scoped_tool", json.RawMessage(`{"x":1}`))
	if err != nil {
		t.Fatalf("execute scoped_tool: %v", err)
	}
	if !strings.Contains(string(res), `"x":1`) {
		t.Fatalf("unexpected result %s", res)
	}
	if _, err := sr.Execute(context.Background(), "nope", nil); err == nil {
		t.Fatal("expected error for unknown tool")
	}
}

func TestSkillBackedCoreLoop(t *testing.T) {
	sr := core.NewSkillRegistry()
	if err := sr.Register(&core.Skill{
		Name:  "calc",
		Tools: []core.ToolDef{{Name: "calc_add", Description: "add a and b"}},
		Execute: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			var in struct {
				A int `json:"a"`
				B int `json:"b"`
			}
			_ = json.Unmarshal(args, &in)
			out, _ := json.Marshal(map[string]int{"sum": in.A + in.B})
			return out, nil
		},
	}); err != nil {
		t.Fatalf("register calc: %v", err)
	}
	reg := core.NewToolRegistry(sr)
	sr.Bind(reg, contracts.Scope{Tenant: "t"})

	model := &scriptedModel{responses: []core.ModelResponse{
		{Content: json.RawMessage(`"using calc"`),
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "calc_add", Args: json.RawMessage(`{"a":2,"b":3}`)}}},
		{Content: json.RawMessage(`"result is 5"`)},
	}}
	rt := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: reg})
	results := make(chan space.StepResult, 32)
	req := &space.CoreRequest{TurnID: "tn", Input: json.RawMessage(`"2+3"`), MaxSteps: 5}
	runErr := make(chan error, 1)
	go func() {
		err := rt.Run(context.Background(), req, results)
		close(results) // mirror the coordinator, which owns channel close
		runErr <- err
	}()
	var gotToolPayload string
	for r := range results {
		if r.StepType == space.StepTool && r.ToolCall != nil && r.ToolCall.Name == "calc_add" {
			gotToolPayload = string(r.Content)
		}
	}
	if err := <-runErr; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(gotToolPayload, "5") {
		t.Fatalf("skill tool result should contain 5, got %q", gotToolPayload)
	}
}
