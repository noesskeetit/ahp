package core

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// fakeOpenAI is a minimal OpenAI-compatible /v1/chat/completions server used to
// exercise the real HTTP client (not a scripted in-process ModelClient). It
// records every decoded request so tests can assert the exact wire shape we
// emit, and replays queued response bodies in order.
type fakeOpenAI struct {
	t         *testing.T
	responses []string // raw JSON bodies, consumed in order
	mu        sync.Mutex
	requests  []ocRequest
	authHdrs  []string
	idx       int
}

func (f *fakeOpenAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/chat/completions" {
		http.NotFound(w, r)
		return
	}
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	var req ocRequest
	_ = json.Unmarshal(body, &req)
	f.mu.Lock()
	f.requests = append(f.requests, req)
	f.authHdrs = append(f.authHdrs, r.Header.Get("Authorization"))
	idx := f.idx
	f.idx++
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if idx < len(f.responses) {
		_, _ = io.WriteString(w, f.responses[idx])
		return
	}
	// Default: a plain stop so an over-eager loop terminates cleanly.
	_, _ = io.WriteString(w, `{"id":"d","choices":[{"index":0,"message":{"role":"assistant","content":"done","tool_calls":[]},"finish_reason":"stop"}]}`)
}

func (f *fakeOpenAI) lastRequests() []ocRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ocRequest, len(f.requests))
	copy(out, f.requests)
	return out
}

func newFake(t *testing.T, responses ...string) (*httptest.Server, *fakeOpenAI) {
	t.Helper()
	f := &fakeOpenAI{t: t, responses: responses}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv, f
}

const (
	respToolCall = `{"id":"c1","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"echo","arguments":"{\"text\":\"hi\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":11,"completion_tokens":5}}`
	respFinal    = `{"id":"c2","choices":[{"index":0,"message":{"role":"assistant","content":"done","tool_calls":[]},"finish_reason":"stop"}],"usage":{"prompt_tokens":20,"completion_tokens":3}}`
)

// TestOpenAICompatGenerateRoundTrip proves the wire mapping in both
// directions: parsing a provider tool-call reply, and serializing our
// assistant(tool_calls)+tool history back into OpenAI's expected shape.
func TestOpenAICompatGenerateRoundTrip(t *testing.T) {
	srv, fake := newFake(t, respToolCall, respFinal)

	c, err := NewOpenAICompat(srv.URL, "secret-key-xyz", "test-model")
	if err != nil {
		t.Fatalf("construct: %v", err)
	}

	// Turn 1: model asks for a tool call.
	resp, err := c.Generate(context.Background(), &ModelRequest{
		Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"hello"`)}},
		Tools:    []ToolDef{{Name: "echo", Parameters: json.RawMessage(`{"type":"object"}`)}},
	})
	if err != nil {
		t.Fatalf("generate turn1: %v", err)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("want 1 tool call, got %d", len(resp.ToolCalls))
	}
	tc := resp.ToolCalls[0]
	if tc.ID != "call_1" || tc.Name != "echo" {
		t.Fatalf("tool call mapping wrong: %+v", tc)
	}
	if string(tc.Args) != `{"text":"hi"}` {
		t.Fatalf("args not parsed: %s", string(tc.Args))
	}
	if resp.Stop != "tool_calls" {
		t.Fatalf("finish_reason not mapped: %q", resp.Stop)
	}
	if resp.Usage == nil || resp.Usage.Input != 11 || resp.Usage.Output != 5 {
		t.Fatalf("usage not mapped: %+v", resp.Usage)
	}
	// Null content normalizes to a JSON empty string, never Go nil.
	if string(resp.Content) != `""` {
		t.Fatalf("null content should normalize to \"\", got %s", string(resp.Content))
	}

	// Turn 2: send the assistant(tool_calls)+tool turn back and inspect the wire.
	assistantTC, _ := json.Marshal([]ToolCall{{ID: "call_1", Name: "echo", Args: json.RawMessage(`{"text":"hi"}`)}})
	_, err = c.Generate(context.Background(), &ModelRequest{
		Messages: []ChatMessage{
			{Role: "user", Content: json.RawMessage(`"hello"`)},
			{Role: "assistant", Content: json.RawMessage(`""`), ToolCalls: assistantTC},
			{Role: "tool", Content: json.RawMessage(`{"ok":true}`), ToolCallID: "call_1"},
		},
	})
	if err != nil {
		t.Fatalf("generate turn2: %v", err)
	}
	reqs := fake.lastRequests()
	if len(reqs) < 2 {
		t.Fatalf("fake saw %d requests, want >=2", len(reqs))
	}
	got := reqs[1]
	var assistant, tool *ocMessageReq
	for i := range got.Messages {
		switch got.Messages[i].Role {
		case "assistant":
			assistant = &got.Messages[i]
		case "tool":
			tool = &got.Messages[i]
		}
	}
	if assistant == nil || len(assistant.ToolCalls) != 1 {
		t.Fatalf("assistant tool_calls missing on the wire: %+v", assistant)
	}
	if assistant.ToolCalls[0].Function.Name != "echo" || assistant.ToolCalls[0].Function.Arguments != `{"text":"hi"}` {
		t.Fatalf("assistant tool_calls wire shape wrong: %+v", assistant.ToolCalls[0])
	}
	if tool == nil || tool.ToolCallID != "call_1" {
		t.Fatalf("tool message tool_call_id missing: %+v", tool)
	}
	// Tool result must be passed as an opaque JSON string, not a nested object.
	if s, ok := tool.Content.(string); !ok || s != `{"ok":true}` {
		t.Fatalf("tool content should be raw JSON string, got %#v", tool.Content)
	}
}

// echoTool is a deterministic ToolExecutor for the runtime-over-HTTP test.
type echoTool struct{ name string }

func (e *echoTool) Execute(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name != e.name {
		return nil, errUnknownTool(name)
	}
	return json.Marshal(map[string]any{"echo": json.RawMessage(args)})
}

type toolErr string

func (e toolErr) Error() string { return string(e) }

func errUnknownTool(name string) error { return toolErr("unknown tool: " + name) }

// TestOpenAIRuntimeLoopOverHTTP drives the real Core runtime against the real
// OpenAI-compatible HTTP client (no scripted ModelClient): turn 1 requests a
// tool, the tool executes, turn 2 finalizes. This is the acceptance evidence
// that the model/tool loop runs end-to-end over the actual wire.
func TestOpenAIRuntimeLoopOverHTTP(t *testing.T) {
	srv, _ := newFake(t, respToolCall, respFinal)

	c, err := NewOpenAICompat(srv.URL, "", "test-model")
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	reg := NewToolRegistry(&echoTool{name: "echo"})
	reg.Register(ToolDef{Name: "echo", Parameters: json.RawMessage(`{"type":"object"}`)})

	rt := NewRuntime(RuntimeConfig{Model: c, Tools: reg})

	results := make(chan space.StepResult, 16)
	req := &space.CoreRequest{
		TurnID: "t1", ThreadID: "th1", RunID: "r1", Worker: "w1", Epoch: 1,
		Input: json.RawMessage(`"say hi"`),
	}
	if err := rt.Run(context.Background(), req, results); err != nil {
		t.Fatalf("runtime run: %v", err)
	}
	close(results)

	var types []space.StepType
	var toolSeen json.RawMessage
	for sr := range results {
		types = append(types, sr.StepType)
		if sr.StepType == space.StepTool {
			toolSeen = sr.Content
		}
	}
	want := []space.StepType{space.StepModel, space.StepTool, space.StepModel, space.StepDone}
	if len(types) != len(want) {
		t.Fatalf("step sequence = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("step %d = %s, want %s (full=%v)", i, types[i], want[i], types)
		}
	}
	if !strings.Contains(string(toolSeen), `"echo"`) {
		t.Fatalf("tool result not observed on loop: %s", string(toolSeen))
	}
}

// TestOpenAICompatFailClosed ensures a misconfigured endpoint cannot silently
// call a different backend.
func TestOpenAICompatFailClosed(t *testing.T) {
	if _, err := NewOpenAICompat("", "k", "m"); err == nil {
		t.Fatal("empty base URL must error")
	}
	if _, err := NewOpenAICompat("ftp://x", "k", "m"); err == nil {
		t.Fatal("non-http base URL must error")
	}
	if _, err := NewOpenAICompat("http://x", "k", ""); err == nil {
		t.Fatal("empty model must error")
	}
	// Trailing /v1 must not double the suffix.
	c, err := NewOpenAICompat("http://example.com/v1", "k", "m")
	if err != nil {
		t.Fatalf("construct with /v1 suffix: %v", err)
	}
	if c.baseURL != "http://example.com" {
		t.Fatalf("base URL should drop /v1, got %q", c.baseURL)
	}
	// FromEnv must fail without echoing values.
	os.Unsetenv(EnvOpenAIBaseURL)
	os.Unsetenv(EnvOpenAIModel)
	if _, err := NewOpenAICompatFromEnv(); err == nil {
		t.Fatal("FromEnv must error when unset")
	}
}

// TestOpenAICompatProviderErrorBounded ensures a provider error never leaks the
// API key and is bounded.
func TestOpenAICompatProviderErrorBounded(t *testing.T) {
	long := "boom " + strings.Repeat("x", 4000)
	srv, _ := newFake(t, `{"error":{"message":"`+long+`","type":"server_error"}}`)
	// Force a 400 so both the error object and the status path are covered.
	c, err := NewOpenAICompat(srv.URL, "SECRET-API-KEY-DO-NOT-LEAK", "m")
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	_, gerr := c.Generate(context.Background(), &ModelRequest{
		Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"hi"`)}},
	})
	if gerr == nil {
		t.Fatal("expected provider error")
	}
	if !strings.Contains(gerr.Error(), "provider error") {
		t.Fatalf("error should mention provider error: %v", gerr)
	}
	if strings.Contains(gerr.Error(), "SECRET-API-KEY-DO-NOT-LEAK") {
		t.Fatal("API key leaked in error message")
	}
	if len(gerr.Error()) > 512 {
		t.Fatalf("error not bounded: %d bytes", len(gerr.Error()))
	}
}

// TestOpenAICompatAuthHeader proves the key is sent as a Bearer header when set
// and omitted entirely when empty; the content of a plain user turn is sent as
// a JSON string.
func TestOpenAICompatAuthHeader(t *testing.T) {
	srv, fake := newFake(t, respFinal)

	withKey, _ := NewOpenAICompat(srv.URL, "topsecret", "m")
	_, _ = withKey.Generate(context.Background(), &ModelRequest{
		Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"hi"`)}},
	})
	// A second request from a keyless client.
	noKey, _ := NewOpenAICompat(srv.URL, "", "m")
	_, _ = noKey.Generate(context.Background(), &ModelRequest{
		Messages: []ChatMessage{{Role: "user", Content: json.RawMessage(`"hi"`)}},
	})

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.authHdrs) != 2 {
		t.Fatalf("want 2 auth header samples, got %d", len(fake.authHdrs))
	}
	if fake.authHdrs[0] != "Bearer topsecret" {
		t.Fatalf("keyed client auth header = %q", fake.authHdrs[0])
	}
	if fake.authHdrs[1] != "" {
		t.Fatalf("keyless client must send no auth header, got %q", fake.authHdrs[1])
	}
	if s, ok := fake.requests[0].Messages[0].Content.(string); !ok || s != "hi" {
		t.Fatalf("user content should serialize to a plain string, got %#v", fake.requests[0].Messages[0].Content)
	}
}

// strictOpenAI mirrors what a real OpenAI-compatible server (pydantic
// ChatCompletionContentPartTextParam et al.) enforces: message.content must be
// a string, or an array of objects each carrying a "type". A bare string inside
// a content array -- which is exactly what an object-form user input produced
// before the normalization fix -- is a hard 400. This harness turns that
// provider behaviour into a local, deterministic gate.
type strictOpenAI struct {
	t        *testing.T
	requests []map[string]any
	rejected []string
}

func (s *strictOpenAI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	var raw map[string]any
	_ = json.Unmarshal(body, &raw)
	s.requests = append(s.requests, raw)
	if msgs, _ := raw["messages"].([]any); true {
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			switch c := mm["content"].(type) {
			case nil, string:
			case []any:
				for _, p := range c {
					if _, ok := p.(map[string]any); !ok {
						s.rejected = append(s.rejected, "content part is not an object")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte(`{"error":{"message":"ChatCompletionContentPartTextParam Input should be a valid dictionary","type":"validation_error"}}`))
						return
					}
				}
			default:
				s.rejected = append(s.rejected, "content is an object, not string/parts")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"message":"content must be string or parts","type":"validation_error"}}`))
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"id":"ok","choices":[{"index":0,"message":{"role":"assistant","content":"принято","tool_calls":[]},"finish_reason":"stop"}]}`))
}

// TestOpenAICompatObjectInputIsDeliveredAsValidParts is the regression for the
// stand measurement (b661c60, runner.log 18:16:13): UI/object input failed the
// whole run at step 0 with provider HTTP 400. Both accepted shapes -- plain
// string input and object input -- must produce a request the strict provider
// validates, and the object's text must survive inside a text part.
func TestOpenAICompatObjectInputIsDeliveredAsValidParts(t *testing.T) {
	s := &strictOpenAI{t: t}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)
	c, err := NewOpenAICompat(srv.URL, "k", "test-model")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	for _, in := range []struct {
		name  string
		input json.RawMessage
	}{
		{"string form", json.RawMessage(`"Составь один короткий абзац"`)},
		{"object form", json.RawMessage(`{"message":"Составь один короткий абзац"}`)},
		{"parts form", json.RawMessage(`[{"type":"text","text":"а"},{"type":"text","text":"б"}]`)},
		{"bare-string array", json.RawMessage(`["message","привет"]`)},
	} {
		req := &ModelRequest{Messages: []ChatMessage{{Role: "user", Content: in.input}}, Step: 0}
		resp, err := c.Generate(ctx, req)
		if err != nil {
			t.Fatalf("%s: strict provider rejected our wire: %v", in.name, err)
		}
		if len(s.rejected) != 0 {
			t.Fatalf("%s: rejected=%v", in.name, s.rejected)
		}
		var answer string
		if err := json.Unmarshal(resp.Content, &answer); err != nil || answer == "" {
			t.Fatalf("%s: empty/foreign answer: %s", in.name, resp.Content)
		}
	}
	// The object form must carry the user's text inside a valid part, not an
	// opaque rejection or a dropped message.
	objectReq := s.requests[1]
	raw, _ := json.Marshal(objectReq)
	if !strings.Contains(string(raw), "Составь один короткий абзац") {
		t.Fatalf("object input text must reach the provider: %s", raw)
	}
}
