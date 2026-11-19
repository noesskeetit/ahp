package triggers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ErrDispatchRefused marks a fire that was provably REFUSED before any effect
// could happen: the platform answered the A2A request with a client-side
// verdict (bad request, unauthorized, forbidden, not found, conflict) so no
// task was created.
//
// The distinction is the platform's own: PDF rev 3.2, c. 13 and c. 54 separate
// "неотправленное закрыто" from "отправленное ... становится UNKNOWN". A refused
// fire may therefore be sealed as FAILED and the schedule may move on, while an
// ambiguous outcome stays SIDE_EFFECT_UNKNOWN and is never auto-replayed.
var ErrDispatchRefused = errors.New("triggers: dispatch refused")

// A2AError is the dispatcher's error type. It carries only non-secret fields:
// the trigger, the sequence, the HTTP status and a machine code. The bearer
// token and the response body never enter the message.
type A2AError struct {
	TriggerID string
	Sequence  int64
	Status    int
	Code      string
	// Permanent marks a provable pre-effect refusal (wraps ErrDispatchRefused).
	Permanent bool
}

func (e *A2AError) Error() string {
	return fmt.Sprintf("triggers: a2a fire trigger=%s seq=%d status=%d code=%s",
		e.TriggerID, e.Sequence, e.Status, e.Code)
}

// Is makes errors.Is(err, ErrDispatchRefused) work for refused fires.
func (e *A2AError) Is(target error) bool {
	if target == ErrDispatchRefused {
		return e.Permanent
	}
	return false
}

// Unwrap exposes the sentinel for errors.Is/As users that prefer it explicitly.
func (e *A2AError) Unwrap() error {
	if e.Permanent {
		return ErrDispatchRefused
	}
	return nil
}

// A2AConfig wires the dispatcher to the gateway's A2A entrypoint.
type A2AConfig struct {
	// URL is the A2A JSON-RPC endpoint a fire posts message/send to (the
	// platform gateway, never an agent pod directly).
	URL string
	// TLS optionally carries a client certificate: the platform's A2A entry is
	// mTLS, and the bearer token alone is not a workload credential.
	TLS *tls.Config
	// Timeout bounds one fire. Default 30s.
	Timeout time.Duration
	// Blocking asks the gateway for a blocking send. Default false: a trigger
	// fire starts background work and must not pin a worker for the task's life.
	Blocking bool
	// HTTPClient optionally overrides the client (tests). TLS is ignored then.
	HTTPClient *http.Client
	// MaxResponseBytes caps how much of a gateway reply is read. Default 1 MiB.
	MaxResponseBytes int64
}

// A2ADispatcher is the Dispatcher that satisfies the platform contract "a fire
// is an A2A message to the agent" (PDF rev 3.2, c. 28, Triggers row:
// "срабатывание — A2A-сообщение агенту").
//
// It sends the freshly re-issued task token as the bearer, and derives a
// deterministic messageId from (trigger id, fire sequence) so that a retry of
// the same fire cannot create a second task -- the platform's own dedup rule
// (PDF rev 3.2, c. 30, п.1: "Повтор message/send с тем же messageId не создаёт
// вторую задачу").
type A2ADispatcher struct {
	cfg A2AConfig
	hc  *http.Client
}

// NewA2ADispatcher validates the configuration and builds the dispatcher.
func NewA2ADispatcher(cfg A2AConfig) (*A2ADispatcher, error) {
	if cfg.URL == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: a2a dispatcher needs a URL")
	}
	if !strings.HasPrefix(cfg.URL, "https://") && !strings.HasPrefix(cfg.URL, "http://127.0.0.1") &&
		!strings.HasPrefix(cfg.URL, "http://localhost") {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"triggers: a2a dispatcher URL must be https (or loopback http for tests)")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = 1 << 20
	}
	hc := cfg.HTTPClient
	if hc == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if cfg.TLS != nil {
			transport.TLSClientConfig = cfg.TLS
		}
		hc = &http.Client{Transport: transport, Timeout: cfg.Timeout}
	}
	return &A2ADispatcher{cfg: cfg, hc: hc}, nil
}

// FireMessageID is the deterministic A2A messageId for one fire.
func FireMessageID(triggerID string, sequence int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("ahp-trigger-fire|%s|%d", triggerID, sequence)))
	return "trg-" + hex.EncodeToString(sum[:])[:40]
}

// firePayload is the convention a trigger payload must satisfy to become an
// A2A message: an optional "text" string, otherwise the whole payload object is
// sent as a JSON text part. It is deliberately small: the trigger stores what
// the agent needs to start, not a transcript.
type firePayload struct {
	Text string `json:"text,omitempty"`
}

// Fire posts the A2A message and returns the task id the platform assigned.
func (d *A2ADispatcher) Fire(ctx context.Context, _ contracts.Scope, req FireRequest) (FireResult, error) {
	if req.Token == "" {
		return FireResult{}, contracts.NewError(contracts.ErrInvalid, "triggers: a2a fire needs the re-issued token")
	}
	text := ""
	if len(req.Payload) > 0 {
		var p firePayload
		if err := json.Unmarshal(req.Payload, &p); err != nil {
			return FireResult{}, contracts.NewError(contracts.ErrInvalid, "triggers: trigger payload is not a JSON object")
		}
		text = p.Text
		if text == "" {
			text = string(req.Payload)
		}
	}
	if text == "" {
		text = fmt.Sprintf("Trigger %s fire %d: start the delegated work.", req.TriggerID, req.Sequence)
	}
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      req.Sequence,
		"method":  "SendMessage",
		"params": map[string]any{
			"message": map[string]any{
				"role":      "user",
				"kind":      "message",
				"messageId": FireMessageID(req.TriggerID, req.Sequence),
				"parts":     []any{map[string]any{"kind": "text", "text": text}},
			},
			"configuration": map[string]any{"blocking": d.cfg.Blocking},
		},
	}
	doc, err := json.Marshal(body)
	if err != nil {
		return FireResult{}, contracts.NewError(contracts.ErrInvalid, "triggers: a2a request cannot be encoded")
	}
	callCtx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(callCtx, http.MethodPost, d.cfg.URL, bytes.NewReader(doc))
	if err != nil {
		return FireResult{}, contracts.NewError(contracts.ErrInvalid, "triggers: a2a request cannot be built")
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("authorization", "Bearer "+req.Token)

	resp, err := d.hc.Do(httpReq)
	if err != nil {
		// Transport-level ambiguity: the request may or may not have reached the
		// gateway, so this is NOT a refusal and the caller must seal UNKNOWN.
		return FireResult{}, &A2AError{TriggerID: req.TriggerID, Sequence: req.Sequence,
			Status: 0, Code: "TRANSPORT"}
	}
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10)); resp.Body.Close() }()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, d.cfg.MaxResponseBytes))
	if readErr != nil {
		return FireResult{}, &A2AError{TriggerID: req.TriggerID, Sequence: req.Sequence,
			Status: resp.StatusCode, Code: "BODY_UNREADABLE"}
	}

	if resp.StatusCode >= 400 {
		return FireResult{}, &A2AError{TriggerID: req.TriggerID, Sequence: req.Sequence,
			Status: resp.StatusCode, Code: refusedCode(resp.StatusCode), Permanent: refused(resp.StatusCode)}
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		// A malformed answer tells us nothing: ambiguous.
		return FireResult{}, &A2AError{TriggerID: req.TriggerID, Sequence: req.Sequence,
			Status: resp.StatusCode, Code: "MALFORMED_REPLY"}
	}
	if envelope.Error != nil {
		// A JSON-RPC error to a well-formed send is a verdict about THIS message.
		return FireResult{}, &A2AError{TriggerID: req.TriggerID, Sequence: req.Sequence,
			Status: resp.StatusCode, Code: "A2A_ERROR", Permanent: true}
	}
	// DEF-H9-1 shape: the result is the A2A oneof envelope {task|message}.
	var result struct {
		Task struct {
			ID      string `json:"id"`
			Context string `json:"contextId"`
		} `json:"task"`
	}
	_ = json.Unmarshal(envelope.Result, &result)
	detail := map[string]any{"message_id": FireMessageID(req.TriggerID, req.Sequence)}
	taskID := result.Task.ID
	var loose map[string]any
	if json.Unmarshal(envelope.Result, &loose) == nil {
		if taskID == "" {
			if id, ok := loose["id"].(string); ok {
				taskID = id
			}
		}
		if state, ok := loose["status"].(map[string]any); ok {
			detail["state"] = state["state"]
		}
	}
	// reply_kind is a non-secret diagnostic: it distinguishes "the platform
	// started a task" from "the platform answered with a message" from "the
	// platform answered with something we cannot read", which is exactly what a
	// failed fire has to be able to prove later.
	switch {
	case taskID != "":
		detail["reply_kind"] = "task"
	case envelope.Result == nil:
		detail["reply_kind"] = "empty"
	default:
		if _, ok := loose["message"]; ok {
			detail["reply_kind"] = "message"
		} else {
			detail["reply_kind"] = "no_task"
		}
	}
	out, err := json.Marshal(detail)
	if err != nil {
		out = json.RawMessage(`{}`)
	}
	return FireResult{TaskID: taskID, Detail: out}, nil
}

// refused reports whether an HTTP status is a provable pre-effect refusal.
// 408 and 429 are timeouts/backpressure: the request may have been processed.
func refused(status int) bool {
	switch {
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests:
		return false
	case status >= 400 && status < 500:
		return true
	default:
		return false
	}
}

func refusedCode(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "UNAUTHENTICATED"
	case status == http.StatusForbidden:
		return "FORBIDDEN"
	case status == http.StatusNotFound:
		return "NOT_FOUND"
	case status == http.StatusConflict:
		return "CONFLICT"
	case status == http.StatusTooManyRequests:
		return "RESOURCE_EXHAUSTED"
	case status >= 500:
		return "UNAVAILABLE"
	default:
		return fmt.Sprintf("HTTP_%d", status)
	}
}
