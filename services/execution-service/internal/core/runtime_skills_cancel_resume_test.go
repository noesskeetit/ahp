package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// recordingModel is a deterministic in-process ModelClient used to exercise the
// Core loop's cancellation and resume semantics without any network. It records
// every ModelRequest it receives so tests can assert on the exact message
// history the loop builds.
type recordingModel struct {
	mu       sync.Mutex
	calls    int
	requests []*ModelRequest
	// respond produces a ModelResponse for the given call index.
	respond func(call int) *ModelResponse
}

func (m *recordingModel) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	// copy request (the loop reuses the slice header) for later assertions
	snap := &ModelRequest{Step: req.Step, MaxTokens: req.MaxTokens}
	snap.Messages = append([]ChatMessage(nil), req.Messages...)
	m.requests = append(m.requests, snap)
	return m.respond(m.calls - 1), nil
}

func (m *recordingModel) counts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func drain(ch chan space.StepResult) []space.StepResult {
	var out []space.StepResult
	for {
		select {
		case r := <-ch:
			out = append(out, r)
		default:
			return out
		}
	}
}

// ---- Skills (SkillRegistry) -------------------------------------------------

func TestSkillRegistryRegisterCollision(t *testing.T) {
	reg := NewSkillRegistry()
	s1 := &Skill{
		Name:    "alpha",
		Tools:   []ToolDef{{Name: "shared", Description: "d"}},
		Execute: func(context.Context, string, json.RawMessage) (json.RawMessage, error) { return nil, nil },
	}
	if err := reg.Register(s1); err != nil {
		t.Fatalf("register alpha: %v", err)
	}
	// duplicate skill name
	if err := reg.Register(s1); err == nil {
		t.Fatal("duplicate skill name must be rejected")
	}
	// distinct skill name but owned tool name collision
	s2 := &Skill{
		Name:    "beta",
		Tools:   []ToolDef{{Name: "shared", Description: "d2"}},
		Execute: func(context.Context, string, json.RawMessage) (json.RawMessage, error) { return nil, nil },
	}
	err := reg.Register(s2)
	if err == nil {
		t.Fatal("tool-name collision across skills must be rejected")
	}
	if !errors.Is(err, nil) && err.Error() == "" {
		t.Fatal("expected descriptive collision error")
	}
}

func TestSkillRegistryVisibilityAndRouting(t *testing.T) {
	const allScope = ""
	ownerCalled := ""
	reg := NewSkillRegistry()
	// public skill: visible everywhere
	pub := &Skill{
		Name:  "pub",
		Tools: []ToolDef{{Name: "ping"}},
		Execute: func(_ context.Context, name string, _ json.RawMessage) (json.RawMessage, error) {
			ownerCalled = "pub:" + name
			return json.RawMessage(`{"ok":1}`), nil
		},
	}
	// restricted skill: visible only for Deployment=="secret"
	priv := &Skill{
		Name:    "priv",
		Visible: func(sc contracts.Scope) bool { return sc.Deployment == "secret" },
		Tools:   []ToolDef{{Name: "admin"}},
		Execute: func(_ context.Context, name string, _ json.RawMessage) (json.RawMessage, error) {
			ownerCalled = "priv:" + name
			return json.RawMessage(`{"ok":1}`), nil
		},
	}
	if err := reg.Register(pub); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(priv); err != nil {
		t.Fatal(err)
	}

	openScope := contracts.Scope{Tenant: "t", Deployment: "d", Subject: allScope}
	secretScope := contracts.Scope{Tenant: "t", Deployment: "secret"}

	if defs := reg.ToolDefs(openScope); len(defs) != 1 || defs[0].Name != "ping" {
		t.Fatalf("open scope tools = %v, want [ping]", defs)
	}
	if defs := reg.ToolDefs(secretScope); len(defs) != 2 {
		t.Fatalf("secret scope tools = %v, want 2", defs)
	}

	if _, err := reg.Execute(context.Background(), "ping", nil); err != nil {
		t.Fatalf("route ping: %v", err)
	}
	if ownerCalled != "pub:ping" {
		t.Fatalf("ping routed to %q, want pub:ping", ownerCalled)
	}
	if _, err := reg.Execute(context.Background(), "admin", nil); err != nil {
		t.Fatalf("route admin: %v", err)
	}
	if ownerCalled != "priv:admin" {
		t.Fatalf("admin routed to %q, want priv:admin", ownerCalled)
	}
	if _, err := reg.Execute(context.Background(), "ghost", nil); err == nil {
		t.Fatal("unknown tool must error")
	}
}

func TestSkillRegistryUnregisterReleasesTool(t *testing.T) {
	reg := NewSkillRegistry()
	s1 := &Skill{
		Name:  "a",
		Tools: []ToolDef{{Name: "tool1"}},
		Execute: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`1`), nil
		},
	}
	if err := reg.Register(s1); err != nil {
		t.Fatal(err)
	}
	reg.Unregister("a")
	if _, err := reg.Execute(context.Background(), "tool1", nil); err == nil {
		t.Fatal("tool ownership must be released after Unregister")
	}
	// now a different skill may claim the freed tool name
	s2 := &Skill{
		Name:  "b",
		Tools: []ToolDef{{Name: "tool1"}},
		Execute: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`2`), nil
		},
	}
	if err := reg.Register(s2); err != nil {
		t.Fatalf("re-register freed tool: %v", err)
	}
	out, err := reg.Execute(context.Background(), "tool1", nil)
	if err != nil || string(out) != "2" {
		t.Fatalf("after re-register execute = %s, %v", out, err)
	}
}

func TestSkillRegistryBindToolRegistry(t *testing.T) {
	reg := NewSkillRegistry()
	if err := reg.Register(&Skill{
		Name:    "echo",
		Tools:   []ToolDef{{Name: "echo"}},
		Execute: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) { return args, nil },
	}); err != nil {
		t.Fatal(err)
	}
	tr := reg.BindToolRegistry()
	if got := len(tr.Defs()); got != 1 {
		t.Fatalf("bound registry defs = %d, want 1", got)
	}
	out, err := tr.Execute(context.Background(), "echo", json.RawMessage(`"hi"`))
	if err != nil || string(out) != `"hi"` {
		t.Fatalf("bound registry execute = %s, %v", out, err)
	}
}

// ---- Cancellation -----------------------------------------------------------

func TestRuntimeCancellationBeforeStart(t *testing.T) {
	m := &recordingModel{respond: func(int) *ModelResponse {
		return &ModelResponse{Content: json.RawMessage(`"done"`)}
	}}
	rt := NewRuntime(RuntimeConfig{Model: m, Tools: NewToolRegistry(nil)})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	results := make(chan space.StepResult, 16)
	err := rt.Run(ctx, &space.CoreRequest{Input: json.RawMessage(`"go"`), MaxSteps: 4}, results)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if m.counts() != 0 {
		t.Fatalf("model called %d times on pre-cancelled ctx, want 0", m.counts())
	}
	// the loop must surface a terminal error step
	sawErrDone := false
	for _, r := range drain(results) {
		if r.StepType == space.StepError && r.Done {
			sawErrDone = true
		}
	}
	if !sawErrDone {
		t.Fatal("expected terminal StepError step on cancellation")
	}
}

// cancelOnToolExecutor cancels the run's context from within a tool call so the
// loop must stop before issuing another model request or executing a later tool.
type cancelOnToolExecutor struct{ cancel context.CancelFunc }

func (e cancelOnToolExecutor) Execute(ctx context.Context, name string, _ json.RawMessage) (json.RawMessage, error) {
	e.cancel()
	return json.RawMessage(`{"cancelled":true}`), nil
}

func TestRuntimeCancellationDuringToolLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &recordingModel{respond: func(int) *ModelResponse {
		// always two tool calls: first cancels, second must NOT execute.
		return &ModelResponse{Content: json.RawMessage(`""`), ToolCalls: []ToolCall{
			{ID: "1", Name: "boom"},
			{ID: "2", Name: "boom"},
		}}
	}}
	tr := NewToolRegistry(cancelOnToolExecutor{cancel: cancel})
	tr.Register(ToolDef{Name: "boom"})
	rt := NewRuntime(RuntimeConfig{Model: m, Tools: tr})

	results := make(chan space.StepResult, 16)
	err := rt.Run(ctx, &space.CoreRequest{Input: json.RawMessage(`"go"`), MaxSteps: 8}, results)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	if m.counts() != 1 {
		t.Fatalf("model called %d times, want exactly 1 (loop must stop on cancel)", m.counts())
	}
}

// ---- Resume from checkpoint -------------------------------------------------

func TestRuntimeResumeFromCheckpointDoesNotReinjectInput(t *testing.T) {
	history := []ChatMessage{
		{Role: "user", Content: json.RawMessage(`"original ask"`)},
		{Role: "assistant", Content: json.RawMessage(`"partial"`)},
	}
	hist, _ := json.Marshal(history)

	m := &recordingModel{respond: func(int) *ModelResponse {
		return &ModelResponse{Content: json.RawMessage(`"finish"`)}
	}}
	rt := NewRuntime(RuntimeConfig{Model: m, Tools: NewToolRegistry(nil)})

	req := &space.CoreRequest{
		Input:      json.RawMessage(`"SHOULD-NOT-APPEAR"`),
		Messages:   hist,
		Checkpoint: &space.Checkpoint{ID: "cp1", TurnID: "t1", Messages: hist},
		MaxSteps:   4,
	}
	results := make(chan space.StepResult, 16)
	if err := rt.Run(context.Background(), req, results); err != nil {
		t.Fatalf("run: %v", err)
	}
	if m.counts() != 1 {
		t.Fatalf("model called %d, want 1", m.counts())
	}
	first := m.requests[0].Messages
	if len(first) != len(history) {
		t.Fatalf("resumed history len = %d, want %d (input must not be re-appended)", len(first), len(history))
	}
	for _, msg := range first {
		if string(msg.Content) == `"SHOULD-NOT-APPEAR"` {
			t.Fatal("turn input was re-injected on resume")
		}
	}
}

func TestRuntimeWithoutCheckpointAppendsInput(t *testing.T) {
	history := []ChatMessage{{Role: "user", Content: json.RawMessage(`"prior"`)}}
	hist, _ := json.Marshal(history)

	m := &recordingModel{respond: func(int) *ModelResponse {
		return &ModelResponse{Content: json.RawMessage(`"finish"`)}
	}}
	rt := NewRuntime(RuntimeConfig{Model: m, Tools: NewToolRegistry(nil)})

	req := &space.CoreRequest{
		Input:    json.RawMessage(`"fresh-input"`),
		Messages: hist,
		// Checkpoint nil -> must append Input
		MaxSteps: 4,
	}
	results := make(chan space.StepResult, 16)
	if err := rt.Run(context.Background(), req, results); err != nil {
		t.Fatalf("run: %v", err)
	}
	first := m.requests[0].Messages
	if len(first) != len(history)+1 {
		t.Fatalf("no-checkpoint history len = %d, want %d", len(first), len(history)+1)
	}
	last := first[len(first)-1]
	if last.Role != "user" || string(last.Content) != `"fresh-input"` {
		t.Fatalf("appended message = %+v, want user/fresh-input", last)
	}
}
