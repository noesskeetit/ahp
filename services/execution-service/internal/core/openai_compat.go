package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

// EnvOpenAIBaseURL / EnvOpenAIAPIKey / EnvOpenAIModel configure an
// OpenAI-compatible model endpoint from the environment. No provider key is
// read from any other source and there is NO fallback to another provider:
// when the base URL or model is unset the client refuses to construct, so a
// misconfigured deployment fails closed rather than silently calling a
// different backend.
const (
	EnvOpenAIBaseURL = "AHP_LLM_BASE_URL"
	EnvOpenAIAPIKey  = "AHP_LLM_API_KEY"
	EnvOpenAIModel   = "AHP_LLM_MODEL"
)

// OpenAICompatClient is a ModelClient backed by an OpenAI-compatible
// /v1/chat/completions endpoint. It uses only the standard library and never
// logs or returns the API key.
type OpenAICompatClient struct {
	baseURL string // no trailing slash, no /v1 suffix
	apiKey  string // in-memory only; never logged or serialized
	model   string
	hc      *http.Client
}

// NewOpenAICompat builds a client for an explicit base URL, API key and model.
// The base URL may include or omit the /v1 suffix; either way requests target
// {base}/v1/chat/completions. An empty model is an error (fail closed).
func NewOpenAICompat(baseURL, apiKey, model string) (*OpenAICompatClient, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if baseURL == "" {
		return nil, fmt.Errorf("openai-compat: empty base URL")
	}
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		return nil, fmt.Errorf("openai-compat: base URL must be http(s)")
	}
	// Accept a base that already carries the /v1 suffix without doubling it.
	if strings.HasSuffix(baseURL, "/v1") {
		baseURL = strings.TrimSuffix(baseURL, "/v1")
	}
	if model == "" {
		return nil, fmt.Errorf("openai-compat: empty model")
	}
	return &OpenAICompatClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		model:   model,
		hc:      &http.Client{Timeout: 120 * time.Second},
	}, nil
}

// NewOpenAICompatFromEnv constructs the client from AHP_LLM_* environment
// variables. It errors (without echoing values) when the endpoint or model is
// not configured.
func NewOpenAICompatFromEnv() (*OpenAICompatClient, error) {
	base := os.Getenv(EnvOpenAIBaseURL)
	if base == "" {
		return nil, fmt.Errorf("openai-compat: %s not set", EnvOpenAIBaseURL)
	}
	model := os.Getenv(EnvOpenAIModel)
	if model == "" {
		return nil, fmt.Errorf("openai-compat: %s not set", EnvOpenAIModel)
	}
	return NewOpenAICompat(base, os.Getenv(EnvOpenAIAPIKey), model)
}

// Model reports the configured model id (non-secret).
func (c *OpenAICompatClient) Model() string { return c.model }

// --- wire types (OpenAI chat completions) ---

type ocFunctionCall struct {
	Name        string          `json:"name"`
	Arguments   string          `json:"arguments"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type ocToolCallReq struct {
	ID       string         `json:"id"`
	Type     string         `json:"type"`
	Function ocFunctionCall `json:"function"`
}

type ocMessageReq struct {
	Role       string          `json:"role"`
	Content    any             `json:"content"`
	ToolCalls  []ocToolCallReq `json:"tool_calls,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	Name       string          `json:"name,omitempty"`
}

type ocToolDefReq struct {
	Type     string         `json:"type"`
	Function ocFunctionCall `json:"function"`
}

type ocRequest struct {
	Model     string         `json:"model"`
	Messages  []ocMessageReq `json:"messages"`
	Tools     []ocToolDefReq `json:"tools,omitempty"`
	MaxTokens int            `json:"max_tokens,omitempty"`
	Stream    bool           `json:"stream"`
}

type ocToolCallResp struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type ocMessageResp struct {
	Role      string           `json:"role"`
	Content   json.RawMessage  `json:"content"`
	ToolCalls []ocToolCallResp `json:"tool_calls"`
}

type ocResponse struct {
	ID      string `json:"id"`
	Choices []struct {
		Index        int           `json:"index"`
		Message      ocMessageResp `json:"message"`
		FinishReason string        `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

// Generate implements ModelClient over HTTP.
func (c *OpenAICompatClient) Generate(ctx context.Context, req *ModelRequest) (*ModelResponse, error) {
	body := ocRequest{
		Model:     c.model,
		Messages:  toOpenAIMessages(req.Messages),
		Tools:     toOpenAITools(req.Tools),
		MaxTokens: req.MaxTokens,
		Stream:    false,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai-compat: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/chat/completions", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("openai-compat: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.hc.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai-compat: request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("openai-compat: read body: %w", err)
	}

	var parsed ocResponse
	if uerr := json.Unmarshal(raw, &parsed); uerr != nil {
		return nil, fmt.Errorf("openai-compat: decode response (HTTP %d): %s", resp.StatusCode, truncate(string(raw), 256))
	}
	if parsed.Error != nil {
		// Error messages come from the provider; echo a bounded slice only.
		return nil, fmt.Errorf("openai-compat: provider error (HTTP %d): %s", resp.StatusCode, truncate(parsed.Error.Message, 256))
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("openai-compat: HTTP %d: %s", resp.StatusCode, truncate(string(raw), 256))
	}
	if len(parsed.Choices) == 0 {
		return nil, fmt.Errorf("openai-compat: empty choices (HTTP %d)", resp.StatusCode)
	}

	ch := parsed.Choices[0]
	out := &ModelResponse{
		Content: normalizeContent(ch.Message.Content),
		Stop:    ch.FinishReason,
	}
	for _, tc := range ch.Message.ToolCalls {
		args := json.RawMessage(tc.Function.Arguments)
		if !json.Valid(args) {
			// Some providers return empty/!JSON arguments; normalize to {}.
			args = json.RawMessage("{}")
		}
		out.ToolCalls = append(out.ToolCalls, ToolCall{
			ID:   tc.ID,
			Name: tc.Function.Name,
			Args: args,
		})
	}
	if parsed.Usage.PromptTokens != 0 || parsed.Usage.CompletionTokens != 0 {
		out.Usage = &Usage{Input: parsed.Usage.PromptTokens, Output: parsed.Usage.CompletionTokens}
	}
	return out, nil
}

// normalizeContent ensures Content is a valid JSON value. A null/absent
// OpenAI content becomes an empty JSON string, matching the ChatMessage
// convention used across Core.
func normalizeContent(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage(`""`)
	}
	if !json.Valid(raw) {
		return json.RawMessage(`""`)
	}
	return raw
}

// toOpenAIContent turns our ChatMessage.Content (a JSON value) into the shape
// OpenAI-compatible providers accept. This is the single contract point for
// user input shapes: a JSON string stays text; an object or any other scalar
// becomes ONE valid text part; an array is normalized element-wise (bare
// strings become text parts, valid objects pass through). Handing the provider
// a raw object/array of strings is what made every UI object-form input die
// with "ChatCompletionContentPartTextParam Input should be a valid dictionary"
// (measured on stand b661c60): the run failed at step 0 before the model ever
// saw the request. Normalizing here means the object form is DELIVERED rather
// than rejected -- the user's message survives as text, which is what a
// text-first product owes, instead of silently dropping the turn.
func toOpenAIContent(raw json.RawMessage) any {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var arr []any
	if err := json.Unmarshal(raw, &arr); err == nil {
		parts := make([]any, 0, len(arr))
		for _, el := range arr {
			if str, ok := el.(string); ok {
				parts = append(parts, map[string]any{"type": "text", "text": str})
				continue
			}
			if m, ok := el.(map[string]any); ok && m["type"] != nil {
				parts = append(parts, m)
				continue
			}
			b, _ := json.Marshal(el)
			parts = append(parts, map[string]any{"type": "text", "text": string(b)})
		}
		return parts
	}
	return []any{map[string]any{"type": "text", "text": string(raw)}}
}

func toOpenAIMessages(msgs []ChatMessage) []ocMessageReq {
	out := make([]ocMessageReq, 0, len(msgs))
	for _, m := range msgs {
		om := ocMessageReq{Role: m.Role, ToolCallID: m.ToolCallID}
		switch m.Role {
		case "tool":
			// Tool results are passed as a JSON string so the provider treats
			// them as opaque tool output regardless of their structure.
			if len(m.Content) > 0 {
				om.Content = string(m.Content)
			} else {
				om.Content = ""
			}
		default:
			om.Content = toOpenAIContent(m.Content)
		}
		if len(m.ToolCalls) > 0 {
			var tcs []ToolCall
			if err := json.Unmarshal(m.ToolCalls, &tcs); err == nil && len(tcs) > 0 {
				for _, tc := range tcs {
					om.ToolCalls = append(om.ToolCalls, ocToolCallReq{
						ID:   tc.ID,
						Type: "function",
						Function: ocFunctionCall{
							Name:      tc.Name,
							Arguments: string(tc.Args),
						},
					})
				}
			}
		}
		out = append(out, om)
	}
	// Providers accept system messages only in the leading block (measured
	// refusal on the stand: HTTP 400 "System message must be at the
	// beginning."), so no calling path -- including a direct OpenAI-compatible
	// run -- may put one elsewhere on the wire (DEF-DOTE2E-11). Positional
	// hoist only: nothing is dropped, folded or otherwise resequenced.
	leading := make([]ocMessageReq, 0, len(out))
	rest := make([]ocMessageReq, 0, len(out))
	for _, m := range out {
		if m.Role == "system" {
			leading = append(leading, m)
		} else {
			rest = append(rest, m)
		}
	}
	return append(leading, rest...)
}

func toOpenAITools(defs []ToolDef) []ocToolDefReq {
	if len(defs) == 0 {
		return nil
	}
	out := make([]ocToolDefReq, 0, len(defs))
	for _, d := range defs {
		out = append(out, ocToolDefReq{
			Type: "function",
			Function: ocFunctionCall{
				Name:        d.Name,
				Description: d.Description,
				Parameters:  d.Parameters,
			},
		})
	}
	return out
}

// truncate bounds s to at most n bytes for safe inclusion in error messages,
// cutting on a rune boundary to avoid emitting partial UTF-8. The original
// length is not reported to avoid leaking size information about bodies.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "\u2026[truncated]"
}
