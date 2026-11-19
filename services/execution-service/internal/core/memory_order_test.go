package core

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// DEF-DOTE2E-11: the pinned Dot configuration enters the conversation as a
// system message at index 0 (cmd/as-runner/profile_runtime.go), while the Core
// loop used to glue the injected memory blocks in FRONT of everything. The
// provider then refused the turn with "System message must be at the
// beginning." (HTTP 400) and the run died terminally. Memory must keep
// arriving -- but only AFTER the leading system block.

type orderMemoryDriver struct{ blocks []ChatMessage }

func (d *orderMemoryDriver) Prepare(context.Context, *space.CoreRequest) ([]ChatMessage, error) {
	return d.blocks, nil
}
func (d *orderMemoryDriver) Defs() []ToolDef     { return nil }
func (d *orderMemoryDriver) Handles(string) bool { return false }
func (d *orderMemoryDriver) Execute(context.Context, *space.CoreRequest, ToolCall) (json.RawMessage, error) {
	return nil, nil
}

type orderCaptureModel struct{ requests []*ModelRequest }

func (m *orderCaptureModel) Generate(_ context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.requests = append(m.requests, req)
	return &ModelResponse{Content: json.RawMessage(`"ok"`)}, nil
}

func quietRuntime(cfg RuntimeConfig) *Runtime {
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewRuntime(cfg)
}

// TestMemoryInjectionKeepsLeadingSystemMessage reproduces the personal-Dot
// crash at the loop level: with a pinned-config system message leading the
// conversation and a live memory driver, every provider call must start with
// that system message, carry every injected block after it, and lose nothing.
func TestMemoryInjectionKeepsLeadingSystemMessage(t *testing.T) {
	driver := &orderMemoryDriver{blocks: []ChatMessage{
		{Role: "user", Content: json.RawMessage(`"memory block one"`)},
		{Role: "user", Content: json.RawMessage(`"memory block two"`)},
	}}
	model := &orderCaptureModel{}
	// Exactly what profile_runtime.go hands to the Core loop: the pinned Dot
	// system block at index 0; the turn's user input arrives via req.Input.
	pinned, _ := json.Marshal([]ChatMessage{{Role: "system", Content: json.RawMessage(`"[AHP Dot configuration]\nbe terse"`)}})
	runtime := quietRuntime(RuntimeConfig{Model: model, Memory: driver})
	results := make(chan space.StepResult, 10)
	if err := runtime.Run(context.Background(), &space.CoreRequest{Input: json.RawMessage(`"hello"`), Messages: pinned}, results); err != nil {
		t.Fatal(err)
	}
	if len(model.requests) != 1 {
		t.Fatalf("model calls = %d, want 1", len(model.requests))
	}
	msgs := model.requests[0].Messages
	if len(msgs) != 4 {
		t.Fatalf("provider messages = %d, want 4: %+v", len(msgs), msgs)
	}
	if msgs[0].Role != "system" {
		t.Fatalf("first provider message is %q (%s): the provider refuses any conversation where a system message is not first", msgs[0].Role, msgs[0].Content)
	}
	for i, want := range []string{"memory block one", "memory block two"} {
		if msgs[1+i].Role != "user" || !strings.Contains(string(msgs[1+i].Content), want) {
			t.Fatalf("message %d: want injected %q, got %+v", 1+i, want, msgs[1+i])
		}
	}
	if msgs[3].Role != "user" || !strings.Contains(string(msgs[3].Content), "hello") {
		t.Fatalf("turn input must arrive last, got %+v", msgs[3])
	}
	for _, m := range msgs {
		if m.Source != "" {
			t.Fatalf("checkpoint metadata reached the provider: %s", m.Source)
		}
	}
}

// TestNewRuntimeStatesDefaultCompletionCap: the gateway refuses a completion
// cap above its own bound (DEF-DOTE2E-9), so a default nobody asked for must
// at least be announced instead of silently shipped.
func TestNewRuntimeStatesDefaultCompletionCap(t *testing.T) {
	var logged strings.Builder
	runtime := NewRuntime(RuntimeConfig{Logger: slog.New(slog.NewTextHandler(&logged, nil))})
	if runtime.cfg.MaxTokens != 4096 {
		t.Fatalf("MaxTokens = %d, want the documented default", runtime.cfg.MaxTokens)
	}
	if !strings.Contains(logged.String(), "MaxTokens") || !strings.Contains(logged.String(), "4096") {
		t.Fatalf("silent substitution: defaulting MaxTokens to 4096 must be logged, got %q", logged.String())
	}
	logged.Reset()
	if NewRuntime(RuntimeConfig{MaxTokens: 1024, Logger: slog.New(slog.NewTextHandler(&logged, nil))}).cfg.MaxTokens != 1024 {
		t.Fatal("a stated cap must survive untouched")
	}
	if logged.Len() != 0 {
		t.Fatalf("a stated cap must not be logged as a substitution, got %q", logged.String())
	}
}
