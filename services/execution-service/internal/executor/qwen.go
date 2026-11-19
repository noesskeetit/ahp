package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/retry"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// qwenModel is the production native-Qwen ModelClient for the execution plane.
// Unlike the generic OpenAI-compatible client, it reaches ONLY the single Qwen
// host from the scoped Vault secret, over a netpolicy-guarded transport (https,
// POST, the chat-completions path) that bypasses any ambient HTTP proxy. There
// is no fallback provider: a missing/invalid secret fails construction.
type qwenModel struct {
	completionsURL string // https://host/<base>/chat/completions
	apiKey         string
	model          string
	http           *http.Client
	// retry bounds repeated TRANSPORT attempts of one provider call. Zero value
	// means "ask once", which is what unit tests get; production reads the
	// operator's single retry budget (internal/retry), the same env the Model
	// Gateway wiring uses. A TLS handshake timeout on a saturated node is node
	// health, not a model answer, and without a bounded repeat it fails the run
	// before its first snapshot boundary.
	retry retry.Policy
}

// NewQwenModel builds a netpolicy-guarded native-Qwen tool-calling client.
func NewQwenModel(_ context.Context, secret secretstore.QwenSecret) (core.ModelClient, error) {
	if err := secret.Validate(); err != nil {
		return nil, err
	}
	u, err := url.Parse(secret.BaseURL)
	if err != nil || u.Host == "" {
		return nil, errors.New("executor: qwen base_url is not a valid URL")
	}
	p := netpolicy.Policy{
		AllowedHosts:        []string{u.Hostname()},
		AllowedMethods:      []string{http.MethodPost},
		AllowedPathPrefixes: []string{completionsPath(u.Path)},
		MaxResponseBytes:    8 << 20,
		Timeout:             120 * time.Second,
	}
	httpc, err := netpolicy.NewClient(p)
	if err != nil {
		return nil, fmt.Errorf("executor: netpolicy for qwen host: %w", err)
	}
	scheme := "https"
	return &qwenModel{
		completionsURL: scheme + "://" + u.Host + completionsPath(u.Path),
		apiKey:         secret.APIKey,
		model:          secret.Model,
		http:           httpc,
		retry:          retry.FromEnv(),
	}, nil
}

// Model reports the native-Qwen model id.
func (q *qwenModel) Model() string { return q.model }

// Generate performs one OpenAI-compatible chat turn with tool definitions and
// returns the model content plus any requested tool calls.
func (q *qwenModel) Generate(ctx context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	body := qwenRequest{
		Model:     q.model,
		Messages:  toQwenMessages(req.Messages),
		Tools:     toQwenTools(req.Tools),
		MaxTokens: req.MaxTokens,
		Stream:    false,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("executor: encode qwen request: %w", err)
	}
	// A fresh request per attempt: an http.Request carries a body reader that the
	// first attempt consumes, and replaying a spent reader is how a retry turns
	// into a silently empty POST. Only a failure with no response at all is
	// repeated (retry.Network); anything the server actually answered is returned
	// as the answer it is.
	resp, err := retry.Do(ctx, q.retry, retry.Network, func() (*http.Response, error) {
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, q.completionsURL, bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if q.apiKey != "" {
			httpReq.Header.Set("Authorization", "Bearer "+q.apiKey)
		}
		resp, err := q.http.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("executor: qwen request: %w", err)
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, fmt.Errorf("executor: read qwen response: %w", err)
	}
	var parsed qwenResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("executor: decode qwen response (HTTP %d): %s", resp.StatusCode, clip(string(raw), 256))
	}
	if resp.StatusCode >= 400 {
		if parsed.Error != nil {
			return nil, fmt.Errorf("executor: qwen error (HTTP %d): %s", resp.StatusCode, clip(parsed.Error.Message, 256))
		}
		return nil, fmt.Errorf("executor: qwen HTTP %d: %s", resp.StatusCode, clip(string(raw), 256))
	}
	if len(parsed.Choices) == 0 {
		return nil, fmt.Errorf("executor: qwen empty choices (HTTP %d)", resp.StatusCode)
	}
	ch := parsed.Choices[0]
	out := &core.ModelResponse{Content: normalizeJSONString(ch.Message.Content), Stop: ch.FinishReason}
	for _, tc := range ch.Message.ToolCalls {
		args := json.RawMessage(tc.Function.Arguments)
		if !json.Valid(args) {
			args = json.RawMessage("{}")
		}
		out.ToolCalls = append(out.ToolCalls, core.ToolCall{ID: tc.ID, Name: tc.Function.Name, Args: args})
	}
	if parsed.Usage.PromptTokens != 0 || parsed.Usage.CompletionTokens != 0 {
		out.Usage = &core.Usage{Input: parsed.Usage.PromptTokens, Output: parsed.Usage.CompletionTokens}
	}
	return out, nil
}

// --- wire types (minimal OpenAI-compatible subset) ---

type qwenRequest struct {
	Model     string        `json:"model"`
	Messages  []qwenMessage `json:"messages"`
	Tools     []qwenTool    `json:"tools,omitempty"`
	MaxTokens int           `json:"max_tokens,omitempty"`
	Stream    bool          `json:"stream"`
}

type qwenMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  json.RawMessage `json:"tool_calls,omitempty"`
}

type qwenTool struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

type qwenResponse struct {
	Choices []struct {
		Message struct {
			Content   json.RawMessage `json:"content"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func toQwenMessages(msgs []core.ChatMessage) []qwenMessage {
	out := make([]qwenMessage, 0, len(msgs))
	for _, m := range msgs {
		content := m.Content
		if len(content) == 0 || string(content) == "null" {
			content = json.RawMessage(`""`)
		}
		qm := qwenMessage{Role: m.Role, Content: content, ToolCallID: m.ToolCallID}
		if qm.Role == "" {
			qm.Role = "user"
		}
		if qm.Role == "assistant" && len(m.ToolCalls) > 0 {
			qm.ToolCalls = toOpenAIToolCalls(m.ToolCalls)
		}
		if qm.Role == "tool" {
			// OpenAI chat-completions expects the tool result as a string body.
			qm.Content = jsonStringBytes(content)
		}
		out = append(out, qm)
	}
	return out
}

func toQwenTools(defs []core.ToolDef) []qwenTool {
	if len(defs) == 0 {
		return nil
	}
	out := make([]qwenTool, 0, len(defs))
	for _, d := range defs {
		var t qwenTool
		t.Type = "function"
		t.Function.Name = d.Name
		t.Function.Description = d.Description
		t.Function.Parameters = d.Parameters
		out = append(out, t)
	}
	return out
}

func normalizeJSONString(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" || !json.Valid(raw) {
		return json.RawMessage(`""`)
	}
	return raw
}

// toOpenAIToolCalls re-encodes Core-shaped tool calls ([{id,name,args}]) into
// the OpenAI chat-completions assistant.tool_calls wire shape
// ([{id,type:"function",function:{name,arguments:<string>}}]).
func toOpenAIToolCalls(raw json.RawMessage) json.RawMessage {
	var core []struct {
		ID   string          `json:"id"`
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	}
	if err := json.Unmarshal(raw, &core); err != nil {
		return nil
	}
	type fn struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	}
	type oc struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function fn     `json:"function"`
	}
	out := make([]oc, 0, len(core))
	for _, c := range core {
		args := string(c.Args)
		if args == "" {
			args = "{}"
		}
		out = append(out, oc{ID: c.ID, Type: "function", Function: fn{Name: c.Name, Arguments: args}})
	}
	b, err := json.Marshal(out)
	if err != nil {
		return nil
	}
	return b
}

// jsonStringBytes wraps a raw JSON value as a JSON string, so a tool result
// object is delivered to the provider as a string body (per the OpenAI schema).
func jsonStringBytes(raw json.RawMessage) json.RawMessage {
	b, err := json.Marshal(string(raw))
	if err != nil {
		return json.RawMessage(`""`)
	}
	return b
}

func completionsPath(base string) string {
	base = strings.TrimRight(base, "/")
	if base == "" || base == "." {
		base = "/v1"
	}
	return base + "/chat/completions"
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
