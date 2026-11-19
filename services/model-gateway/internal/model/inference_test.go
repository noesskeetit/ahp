package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
)

// sseBody builds an OpenAI-compatible SSE stream body from text deltas plus a
// final usage-only chunk and the [DONE] sentinel.
func sseBody(deltas []string, usage *Usage) string {
	var b strings.Builder
	for _, d := range deltas {
		fmt.Fprintf(&b, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", d)
	}
	if usage != nil {
		fmt.Fprintf(&b, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d,\"total_tokens\":%d}}\n\n",
			usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens)
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func qwenFixtureHandler(t *testing.T, wantAuth string, body string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("path = %s, want .../chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("auth header = %q, want %q", got, wantAuth)
		}
		if got := r.Header.Get("Accept"); got != "text/event-stream" {
			t.Errorf("accept = %q, want text/event-stream", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}
}

func TestQwenClientStreamParsesDeltasAndUsage(t *testing.T) {
	apiKey := "sk-test-secret-do-not-leak"
	usage := &Usage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18}
	srv := httptest.NewServer(qwenFixtureHandler(t, "Bearer "+apiKey,
		sseBody([]string{"Hello", ", ", "Qwen"}, usage)))
	defer srv.Close()

	secret := secretstore.QwenSecret{APIKey: apiKey, BaseURL: srv.URL + "/v1", Model: "model-run-tqmbu-road"}
	c, err := NewQwenClientWithHTTP(secret, srv.Client())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if c.Model() != "model-run-tqmbu-road" {
		t.Fatalf("Model() = %q", c.Model())
	}
	st, err := c.Stream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	defer st.Close()

	var got strings.Builder
	for {
		d, err := st.Next(context.Background())
		if err == ErrStreamDone {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		got.WriteString(d.Text)
	}
	if got.String() != "Hello, Qwen" {
		t.Fatalf("streamed text = %q", got.String())
	}
	if u := st.Usage(); u != *usage {
		t.Fatalf("usage = %+v, want %+v", u, *usage)
	}
}

func TestQwenClientErrorRedactsKey(t *testing.T) {
	apiKey := "sk-super-secret-abc"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		// Provider bodies occasionally echo the auth header; the client must redact.
		fmt.Fprintf(w, `{"error":{"message":"bad key: %s"}}`, apiKey)
	}))
	defer srv.Close()

	secret := secretstore.QwenSecret{APIKey: apiKey, BaseURL: srv.URL, Model: "m"}
	c, err := NewQwenClientWithHTTP(secret, srv.Client())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	_, err = c.Stream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "x"}}})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if strings.Contains(err.Error(), apiKey) {
		t.Fatalf("error leaked api key: %v", err)
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("error should mention status: %v", err)
	}
}

func TestQwenClientTransportErrorIsAmbiguous(t *testing.T) {
	// Point at a closed server so Do() fails at the transport layer: the request
	// may or may not have reached the provider, so it must be classified as a
	// transport error the generation layer treats as ambiguous.
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // close immediately -> connection refused

	secret := secretstore.QwenSecret{APIKey: "k", BaseURL: url, Model: "m"}
	c, err := NewQwenClientWithHTTP(secret, &http.Client{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	_, err = c.Stream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "x"}}})
	if err == nil {
		t.Fatal("expected transport error")
	}
	if !IsTransportError(err) {
		t.Fatalf("want transport error, got %T: %v", err, err)
	}
}

func TestNewQwenClientValidatesAndRejectsBadSecret(t *testing.T) {
	if _, err := NewQwenClient(context.Background(), secretstore.QwenSecret{}); err == nil {
		t.Fatal("expected validation error for empty secret")
	}
	if _, err := NewQwenClient(context.Background(), secretstore.QwenSecret{APIKey: "k", BaseURL: "://nope", Model: "m"}); err == nil {
		t.Fatal("expected error for malformed base_url")
	}
}

// TestQwenClientBareHostNormalizesV1Path guards the real deployment shape: the
// scoped secret's base_url is a bare host with no API prefix, so the client
// must target the OpenAI-compatible /v1/chat/completions mount (otherwise the
// provider answers 404). The live Qwen smoke relies on this normalization.
func TestQwenClientBareHostNormalizesV1Path(t *testing.T) {
	apiKey := "sk-test-secret-do-not-leak"
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sseBody([]string{"ok"}, &Usage{TotalTokens: 1})))
	}))
	defer srv.Close()

	// base_url WITHOUT a /v1 prefix, exactly as the platform Vault stores it.
	secret := secretstore.QwenSecret{APIKey: apiKey, BaseURL: srv.URL, Model: "m"}
	c, err := NewQwenClientWithHTTP(secret, srv.Client())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	st, err := c.Stream(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	_ = st.Close()
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("normalized path = %q, want /v1/chat/completions", gotPath)
	}
}

// TestToolCallStreamRig is the contract T1 acceptance rig: a provider stream
// whose tool call arrives SHREDDED across fragments (id+name first, arguments
// in pieces, sometimes empty) must reach the consumer as exactly ONE complete
// call with a whole arguments document, delivered on the terminal delta with
// finish_reason "tool_calls" -- never as half a JSON. The request must carry
// the tool definitions and tool_choice "auto".
func TestToolCallStreamRig(t *testing.T) {
	apiKey := "sk-rig-secret"
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_9","type":"function","function":{"name":"write_artifact","arguments":""}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"na"}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"me\":\"a."}}]}}]}`,
			`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"txt\",\"content\":\"hi\"}"}}]}}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":6,"total_tokens":11}}`,
			"data: [DONE]",
		}, "\n\n") + "\n\n"))
	}))
	defer srv.Close()
	secret := secretstore.QwenSecret{APIKey: apiKey, BaseURL: srv.URL + "/v1", Model: "m"}
	c, err := NewQwenClientWithHTTP(secret, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	req := ChatRequest{Messages: []Message{{Role: "user", Content: "piши файл"}},
		Tools: []ToolDef{{Name: "write_artifact", Description: "seal a file", Parameters: json.RawMessage(`{"type":"object"}`)}}}
	st, err := c.Stream(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	toolsRaw, _ := json.Marshal(gotBody["tools"])
	if !strings.Contains(string(toolsRaw), "write_artifact") || gotBody["tool_choice"] != "auto" {
		t.Fatalf("request must carry tool definitions + tool_choice=auto, got tools=%s choice=%v", toolsRaw, gotBody["tool_choice"])
	}

	var text strings.Builder
	var calls []ToolCall
	var finish string
	for {
		d, err := st.Next(context.Background())
		if errors.Is(err, ErrStreamDone) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		text.WriteString(d.Text)
		if d.FinishReason != "" {
			finish = d.FinishReason
			calls = d.ToolCalls
		}
	}
	if finish != "tool_calls" {
		t.Fatalf("finish_reason = %q, want tool_calls", finish)
	}
	if len(calls) != 1 {
		t.Fatalf("calls = %d, want exactly 1: %v", len(calls), calls)
	}
	if calls[0].ID != "call_9" || calls[0].Name != "write_artifact" {
		t.Fatalf("assembled call lost identity: %+v", calls[0])
	}
	var args map[string]any
	if err := json.Unmarshal(calls[0].Arguments, &args); err != nil {
		t.Fatalf("arguments must arrive WHOLE and valid: %q err=%v", calls[0].Arguments, err)
	}
	if args["name"] != "a.txt" || args["content"] != "hi" {
		t.Fatalf("stitched arguments wrong: %v", args)
	}
}
