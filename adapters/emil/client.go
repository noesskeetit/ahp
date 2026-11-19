package emil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

// AgentCardPath is the canonical A2A Agent Card location.
const AgentCardPath = "/.well-known/agent-card.json"

// Client talks to an Emil A2A server over JSON-RPC 2.0.
type Client struct {
	baseURL string
	hc      *http.Client
	bearer  string // optional outbound bearer; never logged or serialized
	// runAuth resolves the credential of the RUN that is issuing the call, per
	// request. When set it wins over the static bearer, which is what keeps a
	// delegated child acting as the ORIGINAL subject instead of as a
	// deployment-wide service identity. See RunAuthority.
	runAuth func(context.Context) *RunAuthority
	nextID  int64
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient supplies a custom HTTP client (e.g. one carrying mTLS or a
// caller-managed RoundTripper for auth).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.hc = hc
		}
	}
}

// WithBearer attaches an outbound bearer token to every request. The value is
// held only in memory and is never written to logs or provenance. It is the
// fallback for calls made OUTSIDE a run (a CLI probe, a card prefetch); inside a
// run the per-run authority from WithRunAuthority is what a child deployment
// sees.
func WithBearer(token string) Option {
	return func(c *Client) { c.bearer = token }
}

// WithRunAuthority attaches the per-request authority resolver. A nil resolver,
// or one that returns nil for a request, leaves the static bearer in charge, so
// wiring this option never weakens an existing configuration.
func WithRunAuthority(fn func(context.Context) *RunAuthority) Option {
	return func(c *Client) { c.runAuth = fn }
}

// authorityFor resolves the run authority for one request (nil when unwired).
func (c *Client) authorityFor(ctx context.Context) *RunAuthority {
	if c.runAuth == nil {
		return nil
	}
	return c.runAuth(ctx)
}

// bearerFor returns the bearer to present for this request: the run's own
// credential when one is resolved, else the static deployment bearer.
func (c *Client) bearerFor(ctx context.Context) string {
	if ra := c.authorityFor(ctx); ra != nil && ra.Bearer != "" {
		return ra.Bearer
	}
	return c.bearer
}

// NewClient creates a client pointed at an Emil base URL. The URL scheme must
// be http or https (a minimal allowlist); credentials belong in options, never
// embedded in the URL.
func NewClient(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("emil: invalid base URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("emil: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("emil: base URL missing host")
	}
	if u.User != nil {
		return nil, fmt.Errorf("emil: credentials must not be embedded in the base URL")
	}
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		hc:      &http.Client{Timeout: 60 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// AgentCard fetches and parses the A2A Agent Card. It also returns the raw
// response bytes so callers can record a content digest in provenance.
func (c *Client) AgentCard(ctx context.Context) (*AgentCard, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+AgentCardPath, nil)
	if err != nil {
		return nil, nil, err
	}
	if b := c.bearerFor(ctx); b != "" {
		req.Header.Set("Authorization", "Bearer "+b)
	}
	body, err := c.do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("emil agent card: %w", err)
	}
	var card AgentCard
	if err := json.Unmarshal(body, &card); err != nil {
		return nil, body, fmt.Errorf("emil agent card decode: %w", err)
	}
	return &card, body, nil
}

// SendRequest is the input to message/send.
type SendRequest struct {
	Text      string
	ContextID string // optional; Emil assigns a contextId when empty
	MessageID string // optional; generated when empty
}

// Send issues a JSON-RPC message/send and returns the resulting Task plus the
// raw result bytes (for provenance digests).
func (c *Client) Send(ctx context.Context, req SendRequest) (*Task, []byte, error) {
	msgID := req.MessageID
	if msgID == "" {
		msgID = fmt.Sprintf("msg-%d", c.nextID+1)
	}
	msg := map[string]any{
		"kind":      "message",
		"role":      "user",
		"messageId": msgID,
		"parts":     []Part{{Kind: "text", Text: req.Text}},
	}
	if req.ContextID != "" {
		msg["contextId"] = req.ContextID
	}
	task, raw, err := c.rpc(ctx, "message/send", map[string]any{"message": msg})
	if err != nil {
		return nil, raw, err
	}
	return task, raw, nil
}

// GetTask polls a task by id via JSON-RPC tasks/get.
func (c *Client) GetTask(ctx context.Context, taskID string) (*Task, []byte, error) {
	return c.rpc(ctx, "tasks/get", map[string]any{"id": taskID})
}

// rpc performs a JSON-RPC call at the root path (protocol version 0.3 binding).
func (c *Client) rpc(ctx context.Context, method string, params any) (*Task, []byte, error) {
	envelope := jsonrpcRequest{
		JSONRPC: "2.0",
		ID:      atomic.AddInt64(&c.nextID, 1),
		Method:  method,
		Params:  params,
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/", bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b := c.bearerFor(ctx); b != "" {
		req.Header.Set("Authorization", "Bearer "+b)
	}
	body, err := c.do(req)
	if err != nil {
		return nil, body, err
	}
	var resp jsonrpcResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, body, fmt.Errorf("emil %s decode: %w", method, err)
	}
	if resp.Error != nil {
		return nil, body, resp.Error
	}
	var task Task
	if err := json.Unmarshal(resp.Result, &task); err != nil {
		return nil, body, fmt.Errorf("emil %s task decode: %w", method, err)
	}
	return &task, body, nil
}

func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return body, fmt.Errorf("emil: HTTP %d: %s", resp.StatusCode, truncate(string(body), 256))
	}
	return body, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// --- A2A v2 ("JSONRPC"/"1.0") binding: the platform's own execution plane ---
//
// The Agent Card is the A2A contract for BOTH the RPC endpoint and the protocol
// binding. The platform deploy advertises interface {url: .../a2a/public,
// protocolBinding: "JSONRPC", protocolVersion: "1.0"} and speaks a2a-go v2.7.0
// wire (method names SendMessage/GetTask/CancelTask, message role enum
// "ROLE_USER", states "TASK_STATE_*"). The Emil Python reference (0.3) advertises
// no JSONRPC/1.0 interface and speaks message/send at the root with lowercase
// states. A single adapter must honor whichever the card advertises -- this is
// why the RPC path and protocol are resolved from the card, never hardcoded.

// Binding is the resolved RPC endpoint + protocol for one Agent Card.
type Binding struct {
	// RPCURL is the absolute endpoint to POST JSON-RPC to (from the card).
	RPCURL string
	// Canonical selects the a2a-go v2.7.0 wire (true) vs the 0.3 reference (false).
	Canonical bool
}

// ResolveBinding resolves the RPC endpoint and wire from the card, in this order:
//
//  1. the advertised JSONRPC interface -- its url is the endpoint, its
//     protocolVersion picks the wire;
//  2. an interface that decides the wire but carries no url, plus a top-level
//     card url -- the interface still decides the wire, the card supplies the
//     endpoint;
//  3. a top-level card url with no interface -- the endpoint as advertised. Such a
//     card is emitted by a platform deploy speaking the a2a-go v1 wire, whereas
//     the Emil Python reference advertises neither an interface nor a url, so it
//     keeps the legacy branch below;
//  4. nothing advertised -- the legacy root path + 0.3 wire, unchanged.
//
// Nothing is invented: the endpoint is whatever the card advertised, and the base
// root is used only when the card genuinely says nothing.
func (c *Client) ResolveBinding(card *AgentCard) Binding {
	advertised := strings.TrimSpace(card.URL)
	for _, it := range card.SupportedInterfaces {
		if !strings.EqualFold(strings.TrimSpace(it.ProtocolBinding), "JSONRPC") {
			continue
		}
		ver := strings.TrimSpace(it.ProtocolVersion)
		canonical := strings.HasPrefix(ver, "1")
		u := strings.TrimSpace(it.URL)
		if u == "" {
			u = advertised
		}
		if u == "" {
			u = c.baseURL + "/"
		}
		return Binding{RPCURL: u, Canonical: canonical}
	}
	if advertised != "" {
		return Binding{RPCURL: advertised, Canonical: true}
	}
	return Binding{RPCURL: c.baseURL + "/", Canonical: false}
}

// rpcV2 issues one a2a-go v2.7.0 JSON-RPC call. When resultIsTask is true the
// result object is the Task itself (GetTask/CancelTask); otherwise the result is
// a SendMessageResult whose "task" member holds the Task (oneof).
func (c *Client) rpcV2(ctx context.Context, rpcURL, method string, params any, resultIsTask bool) (*Task, []byte, error) {
	envelope := jsonrpcRequest{JSONRPC: "2.0", ID: atomic.AddInt64(&c.nextID, 1), Method: method, Params: params}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b := c.bearerFor(ctx); b != "" {
		req.Header.Set("Authorization", "Bearer "+b)
	}
	body, err := c.do(req)
	if err != nil {
		return nil, body, err
	}
	var resp jsonrpcResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, body, fmt.Errorf("emil %s decode: %w", method, err)
	}
	if resp.Error != nil {
		return nil, body, resp.Error
	}
	raw := resp.Result
	if !resultIsTask {
		var wrap struct {
			Task json.RawMessage `json:"task"`
		}
		if err := json.Unmarshal(resp.Result, &wrap); err != nil {
			return nil, body, fmt.Errorf("emil %s envelope: %w", method, err)
		}
		if len(wrap.Task) == 0 {
			return nil, body, fmt.Errorf("emil %s: result carries no task", method)
		}
		raw = wrap.Task
	}
	var task Task
	if err := json.Unmarshal(raw, &task); err != nil {
		return nil, body, fmt.Errorf("emil %s task decode: %w", method, err)
	}
	task.Status.State = NormalizeState(task.Status.State)
	return &task, body, nil
}

// SendV2 submits a canonical v2 task and returns immediately (the caller polls).
// contextID places the child task in the parent's conversation, and metadata
// carries the tree labels/budget the parent wants the child to hold (A2A has no
// other channel for them: see internal/gateway/outbox.go for the keys the
// execution plane reads). Empty contextID / nil metadata omit the fields.
func (c *Client) SendV2(ctx context.Context, rpcURL, text, contextID string, metadata map[string]any) (*Task, []byte, error) {
	return c.SendV2WithID(ctx, rpcURL, text, contextID, NewDelegationRef(), metadata)
}

// SendV2WithID preserves the logical operation identity on retries/reconciliation.
func (c *Client) SendV2WithID(ctx context.Context, rpcURL, text, contextID, messageID string, metadata map[string]any) (*Task, []byte, error) {
	if messageID == "" {
		return nil, nil, fmt.Errorf("emil: message ID required")
	}

	msg := map[string]any{
		"role":      "ROLE_USER",
		"parts":     []map[string]any{{"text": text}},
		"messageId": messageID,
	}
	if contextID != "" {
		msg["contextId"] = contextID
	}
	params := map[string]any{
		"message":       msg,
		"configuration": map[string]any{"returnImmediately": true},
	}
	if len(metadata) > 0 {
		// Request level is what the execution plane reads for the task's own
		// attributes; the copy on the message journalled with the inbound turn.
		params["metadata"] = metadata
		msg["metadata"] = metadata
	}
	return c.rpcV2(ctx, rpcURL, "SendMessage", params, false)
}

// GetTaskV2 fetches a canonical v2 task by id.
func (c *Client) GetTaskV2(ctx context.Context, rpcURL, taskID string) (*Task, []byte, error) {
	return c.rpcV2(ctx, rpcURL, "GetTask", map[string]any{"id": taskID}, true)
}

// CancelV2 cancels a canonical v2 task by id and returns its new state.
func (c *Client) CancelV2(ctx context.Context, rpcURL, taskID string) (*Task, []byte, error) {
	return c.rpcV2(ctx, rpcURL, "CancelTask", map[string]any{"id": taskID}, true)
}
