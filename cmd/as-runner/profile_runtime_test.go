package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
)

type profileCaptureModel struct{ requests []*core.ModelRequest }

func (m *profileCaptureModel) Generate(_ context.Context, r *core.ModelRequest) (*core.ModelResponse, error) {
	m.requests = append(m.requests, r)
	return &core.ModelResponse{Content: json.RawMessage(`"done"`)}, nil
}

// This is a Core/model contract test, not a provider E2E.
func TestProfileRuntimeUsesPinnedInstructionsSkillsAndNarrowTools(t *testing.T) {
	model := &profileCaptureModel{}
	tools := core.NewToolRegistry(nil)
	tools.Register(core.ToolDef{Name: "allowed"})
	tools.Register(core.ToolDef{Name: "forbidden"})
	var pin space.AgentConfigRef
	if err := json.Unmarshal([]byte(`{"revision":2,"profile_revision":3,"dot":{"instructions":"tenant + personal","max_steps":4,"allowed_tools":["allowed"],"computer_policy":"disabled","skills":[{"skill_id":"writing","revision":7,"bundle":{"instructions":"skill-seven"}}]}}`), &pin); err != nil {
		t.Fatal(err)
	}
	runner := newProfileRuntime(core.RuntimeConfig{Model: model, Tools: tools}, nil)
	req := &space.CoreRequest{Input: json.RawMessage(`"hi"`), MaxSteps: 2, AgentConfig: &pin}
	if err := runner.Run(context.Background(), req, make(chan space.StepResult, 16)); err != nil {
		t.Fatal(err)
	}
	if len(model.requests) != 1 {
		t.Fatalf("model calls=%d", len(model.requests))
	}
	got := model.requests[0]
	doc, _ := json.Marshal(got.Messages)
	if !strings.Contains(string(doc), "tenant + personal") || !strings.Contains(string(doc), "skill-seven") {
		t.Fatalf("missing pinned instructions: %s", doc)
	}
	if len(got.Tools) != 1 || got.Tools[0].Name != "allowed" {
		t.Fatalf("tools widened: %+v", got.Tools)
	}
	pin.Dot.AllowedTools = []string{}
	if err := runner.Run(context.Background(), req, make(chan space.StepResult, 16)); err != nil {
		t.Fatal(err)
	}
	if len(model.requests[1].Tools) != 0 {
		t.Fatal("empty allowed_tools permitted tools")
	}
}
