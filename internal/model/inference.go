package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// Message is one chat-completion turn as the provider sees it. Tool calling is
// native on this boundary since the tool-wire contract (T1): an assistant turn
// may carry ToolCalls, and a tool result turn names the call it answers via
// ToolCallID. Text-only conversations serialize exactly as before.
type Message struct {
	Role       string         `json:"role"`
	Content    string         `json:"content"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
	ToolCalls  []WireToolCall `json:"tool_calls,omitempty"`
}

// WireToolCall is the provider wire shape of one tool invocation: an assistant
// turn lists them; a tool-result turn references one by ID via ToolCallID.
type WireToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function WireFunctionCall `json:"function"`
}

// WireFunctionCall carries the tool name and its arguments as a JSON document
// (the OpenAI-compatible convention: arguments is a STRINGIFIED JSON object).
type WireFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// ToolDef is one tool the model may invoke, in the provider's own shape:
// Parameters is the JSON Schema of the arguments object.
type ToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ChatRequest is a single streaming generation request to the model provider.
type ChatRequest struct {
	Model    string
	Messages []Message
	// Tools are offered to the model with tool_choice "auto": the model decides
	// whether to answer in text or to request one of them. No Tools means the
	// request carries no tools field at all -- byte-identical to the text-only
	// wire this transport had before the tool-wire contract.
	Tools       []ToolDef
	Temperature float64
	MaxTokens   int
}

// Usage reports token counts for one generation.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// Delta is one streamed chunk: either generated text, a finish reason, or both.
type Delta struct {
	Text         string
	FinishReason string
	// ToolCalls is set exactly once per stream: on the terminal delta whose
	// FinishReason is "tool_calls", carrying the calls REASSEMBLED from the
	// provider's incremental fragments (arguments arrive in pieces and are
	// stitched here, so a consumer never sees a half a JSON document).
	ToolCalls []ToolCall
}

// ToolCall is one complete tool invocation requested by the model. Arguments
// is the whole arguments document, never a fragment.
type ToolCall struct {
	ID        string
	Name      string
	Arguments json.RawMessage
}

// ErrStreamDone marks the end of a stream (returned by Next after the last
// chunk). It is a sentinel, not a failure.
var ErrStreamDone = errors.New("model: stream done")

// Stream yields generated deltas for one generation. Callers must Close it.
type Stream interface {
	// Next returns the next delta, blocking until one arrives, the stream ends
	// (ErrStreamDone) or ctx is cancelled.
	Next(ctx context.Context) (Delta, error)
	// Usage returns token accounting once the provider has reported it. It is
	// valid to read after Next returns ErrStreamDone.
	Usage() Usage
	Close() error
}

// Inference abstracts the upstream model provider so tests can substitute a
// deterministic fake while production uses the real Qwen client.
type Inference interface {
	Stream(ctx context.Context, req ChatRequest) (Stream, error)
	// Model reports the configured model identifier.
	Model() string
}

// QwenClient talks to an OpenAI-compatible Qwen endpoint through the
// network-policy guard. Credentials come only from secretstore.
type QwenClient struct {
	secret          secretstore.QwenSecret
	client          *http.Client
	completionsPath string
}

// NewQwenClient builds a production client whose egress is restricted by
// netpolicy to the single host in the secret and to POST /chat/completions.
func NewQwenClient(ctx context.Context, secret secretstore.QwenSecret) (*QwenClient, error) {
	return newQwenClient(secret)
}

// newQwenClient is the guarded construction seam. Production calls it with no
// options; tests inject a deterministic resolver and dial path so the egress
// policy can be exercised without a live provider or a real route. PrivateHosts
// from the secret are forwarded verbatim so an internal provider may be reached
// over http/RFC1918 when the operator explicitly registered it; the forbidden
// address classes remain unconditionally blocked inside the guard.
func newQwenClient(secret secretstore.QwenSecret, opts ...netpolicy.Option) (*QwenClient, error) {
	if err := secret.Validate(); err != nil {
		return nil, err
	}
	u, err := parseQwenBaseURL(secret.BaseURL)
	if err != nil {
		return nil, err
	}
	publicEntry, privateOnly, err := qwenOriginRegistration(u, secret.PrivateHosts)
	if err != nil {
		return nil, err
	}
	allowed := []string{publicEntry}
	if privateOnly {
		// The destination is an explicitly registered internal origin: the
		// private branch of the guard admits it, and listing the same address as
		// a public entry would be both redundant and rejected by the guard.
		allowed = nil
	}
	policy := netpolicy.Policy{
		AllowedHosts:        allowed,
		PrivateHosts:        secret.PrivateHosts,
		AllowedMethods:      []string{http.MethodPost},
		AllowedPathPrefixes: []string{completionsPath(u.Path)},
		MaxResponseBytes:    8 << 20,
		Timeout:             120 * time.Second,
	}
	httpc, err := netpolicy.NewClient(policy, opts...)
	if err != nil {
		return nil, fmt.Errorf("model: netpolicy for qwen host: %w", err)
	}
	return &QwenClient{secret: secret, client: httpc, completionsPath: completionsPath(u.Path)}, nil
}

// qwenOriginRegistration decides what the guarded client registers as its own
// destination, given the configured base_url and the operator's explicit
// PrivateHosts list. The registration is an ORIGIN -- host and port together:
// a base_url that names a port must register that port, otherwise the guard
// answers only the scheme default and the client cannot reach the endpoint it
// was configured for.
//
// It returns the public entry to register, and whether the destination is
// covered by PrivateHosts instead. When the client provably cannot reach its own
// base_url because of a port the operator never registered, construction says so
// with that fact; letting it fail per request would surface netpolicy's
// "only reachable over https", which is not what happened.
//
// An unregistered private IP literal keeps today's behavior: it is still handed
// to the guard as a public entry, and the guard rejects the policy outright.
func qwenOriginRegistration(u *url.URL, privateHosts []string) (publicEntry string, privateOnly bool, err error) {
	host := u.Hostname()
	explicit := u.Port()
	scheme := strings.ToLower(u.Scheme)
	reqPort := explicit
	if reqPort == "" {
		reqPort = schemeDefaultPort(scheme)
	}
	named, matched, shown, anyBare := privateEntryStatus(privateHosts, host, reqPort, scheme)

	if ip := net.ParseIP(host); ip != nil && !netpolicy.IsForbiddenIP(ip) && netpolicy.IsPrivateRangeIP(ip) {
		switch {
		case matched:
			return "", true, nil
		case named:
			return "", false, privatePortMismatch(u, reqPort, shown, scheme, anyBare)
		default:
			return host, false, nil
		}
	}
	if named && !matched {
		// The operator named this host for a different port. Answering the
		// configured port anyway -- which the self-registered public entry would
		// do, for https as well as http -- would widen egress past the explicit
		// registration, so the client refuses to exist.
		return "", false, privatePortMismatch(u, reqPort, shown, scheme, anyBare)
	}
	if explicit == "" {
		return host, false, nil
	}
	canonical := host
	if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
		canonical = "[" + host + "]"
	}
	return canonical + ":" + explicit, false, nil
}

// privateEntryStatus reports whether privateHosts names host at all, whether an
// entry answers (host, reqPort), and a printable rendering of the entries that
// name the host. The port rule mirrors netpolicy: an entry without a port
// answers the request port only when that port is the scheme default.
func privateEntryStatus(entries []string, host, reqPort, scheme string) (named, matched bool, shown []string, anyBare bool) {
	want := strings.ToLower(host)
	for _, entry := range entries {
		canonical := strings.TrimSpace(entry)
		if canonical == "" {
			continue
		}
		entryHost, entryPort := strings.ToLower(canonical), ""
		if h, port, splitErr := net.SplitHostPort(canonical); splitErr == nil {
			entryHost, entryPort = strings.ToLower(strings.Trim(h, "[]")), port
		}
		if entryHost != want {
			continue
		}
		named = true
		shown = append(shown, canonical)
		if entryPort == "" {
			anyBare = true
			if reqPort != "" && reqPort == schemeDefaultPort(scheme) {
				matched = true
			}
			continue
		}
		if entryPort == reqPort {
			matched = true
		}
	}
	return named, matched, shown, anyBare
}

func privatePortMismatch(u *url.URL, reqPort string, shown []string, scheme string, anyBare bool) error {
	// One quoting rule for the whole file: describeAuthority prints an authority it
	// can prove is an authority and says "value not quoted" otherwise. PrivateHosts
	// entries reach this message from operator configuration and from the route's
	// secret record, so joining them verbatim was the same defect that was closed
	// for base_url: a refusal is not a place to read configuration back out of.
	described := make([]string, 0, len(shown))
	for _, entry := range shown {
		described = append(described, describeAuthority(entry))
	}
	registrations := strings.Join(described, ", ")
	if registrations == "" {
		registrations = "none"
	}
	// Say what was actually registered. Quoting the bare-entry rule at someone who
	// pinned a port sends them looking in the wrong place.
	explanation := "none of them answers that port"
	if anyBare {
		explanation = "an entry without a port answers only the scheme default port " + schemeDefaultPort(scheme)
	}
	return fmt.Errorf(
		"model: qwen base_url %s dials port %s, but PrivateHosts registers %s; %s -- register the exact host:port the client is configured to call",
		describeAuthority(u.Host), reqPort, registrations, explanation)
}

func schemeDefaultPort(scheme string) string {
	switch scheme {
	case "https":
		return "443"
	case "http":
		return "80"
	}
	return ""
}

// NewQwenClientWithHTTP builds a client with an injected HTTP client. Tests use
// this to point at a local fixture; production must use NewQwenClient so the
// network policy guard is always applied.
func NewQwenClientWithHTTP(secret secretstore.QwenSecret, httpc *http.Client) (*QwenClient, error) {
	if err := secret.Validate(); err != nil {
		return nil, err
	}
	if httpc == nil {
		return nil, errors.New("model: nil http client")
	}
	// The SAME gate as the guarded constructor. This seam is exported, so it is a
	// way around the configuration refusals for any caller that uses it -- and a
	// client built from a doubled path does not fail loudly: it dials a path no
	// provider serves and the generation dies as "ambiguous side effect" with its
	// reservation held, for a request that was never sent.
	u, err := parseQwenBaseURL(secret.BaseURL)
	if err != nil {
		return nil, err
	}
	return &QwenClient{secret: secret, client: httpc, completionsPath: completionsPath(u.Path)}, nil
}

// Model reports the configured model id.
func (c *QwenClient) Model() string { return c.secret.Model }

// Endpoint reports the host:port this client dials -- the physical destination a
// routing decision resolved to. It is the smallest fact that lets an operator
// reconcile "the router chose copy X" with "the copy X socket was used", which is
// exactly what the ledger could not say before (ARCH-02). It carries no scheme,
// no path and never a credential.
func (c *QwenClient) Endpoint() string {
	u, err := url.Parse(c.secret.BaseURL)
	if err != nil || u == nil {
		return ""
	}
	return u.Host
}

// completionsPath returns the chat-completions path rooted at the base path so
// both "https://host/v1" and "https://host" work.
func completionsPath(base string) string {
	base = strings.TrimRight(base, "/")
	if base == "" || base == "." {
		// A bare-host base_url (no API prefix) still targets an
		// OpenAI-compatible endpoint, which serves chat completions under the
		// conventional /v1 mount. Without this the provider returns 404.
		base = "/v1"
	}
	return base + "/chat/completions"
}

// openAIRequest is the wire request body for OpenAI-compatible chat endpoints.
type openAIRequest struct {
	Model         string               `json:"model"`
	Messages      []Message            `json:"messages"`
	Stream        bool                 `json:"stream"`
	StreamOptions *openAIStreamOptions `json:"stream_options,omitempty"`
	Temperature   *float64             `json:"temperature,omitempty"`
	MaxTokens     *int                 `json:"max_tokens,omitempty"`
	Tools         []openAIToolReq      `json:"tools,omitempty"`
	ToolChoice    string               `json:"tool_choice,omitempty"`
}

type openAIToolReq struct {
	Type     string `json:"type"`
	Function struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		Parameters  json.RawMessage `json:"parameters,omitempty"`
	} `json:"function"`
}

type openAIStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type openAIChunk struct {
	Choices []struct {
		Delta struct {
			Content   string             `json:"content"`
			Role      string             `json:"role"`
			ToolCalls []openAIToolCallFr `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
}

// openAIToolCallFr is one INCREMENTAL fragment of a streamed tool call: the
// first fragment of a call carries id/name, later ones only index and a slice
// of the arguments string. Assembling them is sseStream's job.
type openAIToolCallFr struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

// Stream issues a streaming chat completion and returns a reader over deltas.
func (c *QwenClient) Stream(ctx context.Context, req ChatRequest) (Stream, error) {
	model := req.Model
	if model == "" {
		model = c.secret.Model
	}
	body := openAIRequest{Model: model, Messages: req.Messages, Stream: true, StreamOptions: &openAIStreamOptions{IncludeUsage: true}}
	if req.MaxTokens > 0 {
		body.MaxTokens = &req.MaxTokens
	}
	if req.Temperature > 0 {
		t := req.Temperature
		body.Temperature = &t
	}
	if len(req.Tools) > 0 {
		for _, td := range req.Tools {
			var one openAIToolReq
			one.Type = "function"
			one.Function.Name = td.Name
			one.Function.Description = td.Description
			one.Function.Parameters = td.Parameters
			body.Tools = append(body.Tools, one)
		}
		body.ToolChoice = "auto"
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	u, _ := url.Parse(c.secret.BaseURL)
	u.Path = c.completionsPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.secret.APIKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	resp, err := c.client.Do(httpReq)
	if err != nil {
		// A guard refusal (scheme/host/method/address, or a failed resolution)
		// is deterministic and happens before any request byte leaves the
		// process, so it is NOT an unknown side effect: classify it apart from
		// an ambiguous post-dial transport failure.
		if isPreSendNetpolicy(err) {
			return nil, &preSendRefusalError{err}
		}
		// A transport error may or may not have reached the provider; callers
		// classify it as ambiguous at the generation layer, never assume retry.
		return nil, &transportError{err}
	}
	if resp.StatusCode/100 != 2 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("model: provider returned %d: %s", resp.StatusCode, redact(string(snippet), c.secret.APIKey))
	}
	return &sseStream{resp: resp, scanner: bufio.NewScanner(resp.Body)}, nil
}

// transportError wraps a network failure so the generation layer can decide it
// is ambiguous (the request may have been processed upstream).
type transportError struct{ err error }

func (e *transportError) Error() string { return "model: transport: " + e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// IsTransportError reports whether err is an ambiguous transport failure that
// occurred after the request may have left the process.
func IsTransportError(err error) bool {
	var te *transportError
	return errors.As(err, &te)
}

// preSendRefusalError marks a network-policy refusal that the guard returned
// before the request was written to any provider. No byte left the process, so
// the provider outcome is known (it never happened) and must not be journalled
// as an unknown side effect.
type preSendRefusalError struct{ err error }

func (e *preSendRefusalError) Error() string {
	return "model: egress refused before send: " + e.err.Error()
}
func (e *preSendRefusalError) Unwrap() error { return e.err }

// IsPreSendRefusal reports whether err is a deterministic pre-send egress
// refusal, distinct from an ambiguous post-dial transport failure.
func IsPreSendRefusal(err error) bool {
	var pre *preSendRefusalError
	return errors.As(err, &pre)
}

// preSendRefusalSentinels are the guard denials that can only occur before the
// request body is written. The redirect sentinel is deliberately absent: it is
// raised only after a response was already received, where a side effect may
// have happened, and so must remain ambiguous.
var preSendRefusalSentinels = []error{
	netpolicy.ErrSchemeNotAllowed,
	netpolicy.ErrHostNotAllowed,
	netpolicy.ErrMethodNotAllowed,
	netpolicy.ErrAddressForbidden,
}

// isPreSendNetpolicy reports whether err wraps a netpolicy guard refusal in the
// pre-send set (URL validation or address gate, neither of which dials).
func isPreSendNetpolicy(err error) bool {
	var ne *netpolicy.Error
	if !errors.As(err, &ne) {
		return false
	}
	for _, sentinel := range preSendRefusalSentinels {
		if errors.Is(ne, sentinel) {
			return true
		}
	}
	return false
}

// sseStream parses text/event-stream chunks.
type sseStream struct {
	resp    *http.Response
	scanner *bufio.Scanner
	usage   Usage
	// partial tool calls stitched across fragments, ordered by first sight
	partials []partialToolCall
}

type partialToolCall struct {
	id   string
	name string
	args strings.Builder
}

// mergeToolCallFr folds one incremental tool-call fragment into the ordered
// partial list. Providers address fragments by index, but the index space is
// the stream's, not this generation's guaranteed one; keying by the call id
// when present (and by position otherwise) is what keeps two calls from
// interleaving their arguments into mush.
func (s *sseStream) mergeToolCallFr(fr openAIToolCallFr) {
	at := -1
	if fr.ID != "" {
		for i := range s.partials {
			if s.partials[i].id == fr.ID {
				at = i
				break
			}
		}
	} else if fr.Index >= 0 && fr.Index < len(s.partials) {
		at = fr.Index
	}
	if at < 0 {
		s.partials = append(s.partials, partialToolCall{})
		at = len(s.partials) - 1
	}
	p := &s.partials[at]
	if fr.ID != "" {
		p.id = fr.ID
	}
	if fr.Function.Name != "" {
		p.name = fr.Function.Name
	}
	p.args.WriteString(fr.Function.Arguments)
}

// completeToolCalls reassembles the accumulated fragments. A fragment stream
// that never produced a name/arguments pair yields nothing -- an empty tool
// call is a provider fault, not a call with empty strings.
func (s *sseStream) completeToolCalls() []ToolCall {
	var out []ToolCall
	for _, p := range s.partials {
		if p.name == "" && p.args.Len() == 0 {
			continue
		}
		id := p.id
		if id == "" {
			id = "call-" + itoa(len(out)+1)
		}
		out = append(out, ToolCall{ID: id, Name: p.name, Arguments: json.RawMessage(p.args.String())})
	}
	return out
}

func (s *sseStream) Usage() Usage { return s.usage }

func (s *sseStream) Next(ctx context.Context) (Delta, error) {
	for s.scanLine(ctx) {
		line := s.scanner.Text()
		if line == "" || strings.HasPrefix(line, ":") {
			continue // keep-alive / comments
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" {
			continue
		}
		if data == "[DONE]" {
			return Delta{}, ErrStreamDone
		}
		var chunk openAIChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return Delta{}, fmt.Errorf("model: malformed stream chunk: %w", err)
		}
		if chunk.Usage != nil {
			s.usage = *chunk.Usage
		}
		for _, ch := range chunk.Choices {
			for _, fr := range ch.Delta.ToolCalls {
				s.mergeToolCallFr(fr)
			}
			if ch.FinishReason == "tool_calls" {
				// The terminal fact of the stream: calls are complete NOW.
				calls := s.completeToolCalls()
				s.partials = nil
				return Delta{Text: ch.Delta.Content, FinishReason: ch.FinishReason, ToolCalls: calls}, nil
			}
			if ch.Delta.Content != "" || ch.FinishReason != "" {
				return Delta{Text: ch.Delta.Content, FinishReason: ch.FinishReason}, nil
			}
		}
		// chunk carried only usage/role: keep reading
	}
	if err := s.scanner.Err(); err != nil {
		return Delta{}, err
	}
	return Delta{}, ErrStreamDone
}

// scanLine advances the scanner unless ctx is cancelled first.
func (s *sseStream) scanLine(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	return s.scanner.Scan()
}

func (s *sseStream) Close() error {
	if s.resp != nil && s.resp.Body != nil {
		return s.resp.Body.Close()
	}
	return nil
}

// redact removes the api key from an error snippet as a defence-in-depth
// measure; provider error bodies occasionally echo headers.
func redact(s, secret string) string {
	if secret != "" {
		s = strings.ReplaceAll(s, secret, "<redacted>")
	}
	if len(s) > 512 {
		s = s[:512]
	}
	return s
}
