// Package emil implements a Go adapter for the Emil "core-agent" A2A server.
//
// Emil (go-platform core_agent @ 5ed238b4) exposes an Agent-to-Agent (A2A)
// endpoint built on the official Python a2a SDK. It speaks JSON-RPC 2.0 at the
// root path (protocol version 0.3) and serves an Agent Card at
// /.well-known/agent-card.json. This adapter targets that wire shape exactly.
//
// The adapter is intentionally stdlib-only: it does not import internal/space,
// internal/stateapi, or internal/core. It structurally satisfies
// core.ToolExecutor via Execute so it can be registered as the "emil.delegate"
// tool in the Core runtime without creating an import cycle.
package emil

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// A2A task states (lowercase, as emitted by Emil / the a2a SDK).
const (
	StateSubmitted     = "submitted"
	StateWorking       = "working"
	StateInputRequired = "input-required"
	StateAuthRequired  = "auth-required"
	StateCompleted     = "completed"
	StateFailed        = "failed"
	StateCanceled      = "canceled"
	StateRejected      = "rejected"
)

// NormalizeState maps an A2A task state to the adapter's canonical lowercase
// form across protocol versions. The Emil Python SDK (protocol 0.3) emits bare
// lowercase ("completed"); the a2a-go server (v2 / "1.0" binding, the platform's
// own execution plane) emits the enum spelling ("TASK_STATE_COMPLETED"). One
// adapter must read both, so every state check normalizes first. Unknown values
// pass through lowercased.
func NormalizeState(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	v = strings.TrimPrefix(v, "task_state_")
	if v == "cancelled" { // tolerate the double-l spelling
		return StateCanceled
	}
	return v
}

// IsTerminalState reports whether a task state ends the task lifecycle.
func IsTerminalState(s string) bool {
	switch NormalizeState(s) {
	case StateCompleted, StateFailed, StateCanceled, StateRejected:
		return true
	}
	return false
}

// AgentCard is the public A2A Agent Card document.
type AgentCard struct {
	Name                string               `json:"name"`
	Description         string               `json:"description"`
	SupportedInterfaces []SupportedInterface `json:"supportedInterfaces"`
	// URL is the endpoint a card may advertise at the top level instead of, or in
	// addition to, an entry in SupportedInterfaces. Decoding it is not decoration:
	// a client that drops it has to invent a path, and inventing a path is what
	// put JSON-RPC on the base root and produced the 404.
	URL                string       `json:"url"`
	Version            string       `json:"version"`
	Capabilities       Capabilities `json:"capabilities"`
	DefaultInputModes  []string     `json:"defaultInputModes"`
	DefaultOutputModes []string     `json:"defaultOutputModes"`
	Skills             []Skill      `json:"skills"`
}

// SupportedInterface declares one protocol binding Emil exposes.
type SupportedInterface struct {
	URL             string `json:"url"`
	ProtocolBinding string `json:"protocolBinding"`
	ProtocolVersion string `json:"protocolVersion"`
}

// Capabilities reports optional A2A capabilities.
type Capabilities struct {
	Streaming         bool `json:"streaming"`
	PushNotifications bool `json:"pushNotifications"`
}

// Skill is one advertised agent capability (maps to an Emil core_* tool).
type Skill struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// Part is an A2A message/artifact part. A2A v0.3 uses a tagged union keyed on
// the "kind" field: "text", "data", or "file".
type Part struct {
	Kind string
	Text string
	Data json.RawMessage // raw JSON object for kind=data
	File *FilePart       // for kind=file
}

// FilePart carries an inlined (base64) or referenced file part.
type FilePart struct {
	Bytes    string `json:"bytes,omitempty"`
	URI      string `json:"uri,omitempty"`
	Name     string `json:"name,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
}

// UnmarshalJSON decodes the tagged-union Part shape.
func (p *Part) UnmarshalJSON(b []byte) error {
	var raw struct {
		Kind string          `json:"kind"`
		Text string          `json:"text"`
		Data json.RawMessage `json:"data"`
		File *FilePart       `json:"file"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	p.Kind = raw.Kind
	p.Text = raw.Text
	p.Data = raw.Data
	p.File = raw.File
	return nil
}

// MarshalJSON re-encodes the tagged-union Part shape.
func (p Part) MarshalJSON() ([]byte, error) {
	switch p.Kind {
	case "data":
		data := p.Data
		if len(data) == 0 {
			data = json.RawMessage("{}")
		}
		return json.Marshal(map[string]json.RawMessage{
			"kind": mustMarshal("data"),
			"data": data,
		})
	case "file":
		return json.Marshal(struct {
			Kind string    `json:"kind"`
			File *FilePart `json:"file"`
		}{Kind: "file", File: p.File})
	default:
		return json.Marshal(struct {
			Kind string `json:"kind"`
			Text string `json:"text"`
		}{Kind: firstNonEmpty(p.Kind, "text"), Text: p.Text})
	}
}

// TextBytes returns the raw bytes whose digest is recorded for this part.
func (p Part) ContentBytes() []byte {
	switch p.Kind {
	case "data":
		return []byte(p.Data)
	case "file":
		if p.File != nil {
			return []byte(p.File.Bytes)
		}
		return nil
	default:
		return []byte(p.Text)
	}
}

// Artifact is an output object produced by a task.
type Artifact struct {
	ArtifactID string          `json:"artifactId"`
	Name       string          `json:"name,omitempty"`
	Parts      []Part          `json:"parts"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	LastChunk  bool            `json:"lastChunk,omitempty"`
	Index      int             `json:"index,omitempty"`
}

// TaskStatus is the current state of a task with an optional status message.
type TaskStatus struct {
	State     string   `json:"state"`
	Timestamp string   `json:"timestamp"`
	Message   *Message `json:"message,omitempty"`
}

// Task is the A2A task object returned by message/send and tasks/get.
type Task struct {
	ID        string     `json:"id"`
	ContextID string     `json:"contextId"`
	Kind      string     `json:"kind"`
	Status    TaskStatus `json:"status"`
	Artifacts []Artifact `json:"artifacts,omitempty"`
	History   []Message  `json:"history,omitempty"`
	// Metadata is the task's own journal. The AHP execution plane writes what the
	// last attempt consumed into it (internal/gateway/budget.go
	// MetadataStepsConsumed), and a delegating run has to converge its budget tree
	// with that report rather than with a guess -- so the adapter has to parse it.
	Metadata map[string]any `json:"metadata,omitempty"`
}

// Message is an A2A message (a turn in history or a status message).
type Message struct {
	Kind      string `json:"kind"`
	Role      string `json:"role"`
	Parts     []Part `json:"parts"`
	MessageID string `json:"messageId"`
	ContextID string `json:"contextId,omitempty"`
	TaskID    string `json:"taskId,omitempty"`
}

// Text joins the text parts of a message (used to surface status error text).
func (m *Message) Text() string {
	if m == nil {
		return ""
	}
	var b bytes.Buffer
	for _, p := range m.Parts {
		if p.Kind == "" || p.Kind == "text" {
			if b.Len() > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(p.Text)
		}
	}
	return b.String()
}

// jsonrpcRequest is the JSON-RPC 2.0 request envelope.
type jsonrpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params"`
}

// jsonrpcError is the JSON-RPC 2.0 error object.
type jsonrpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *jsonrpcError) Error() string {
	return "emil jsonrpc error " + strconv.Itoa(e.Code) + ": " + e.Message
}

// jsonrpcResponse is the JSON-RPC 2.0 response envelope.
type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *jsonrpcError   `json:"error,omitempty"`
}

func mustMarshal(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// extractToolCalls scans A2A message history for Emil tool invocations and
// returns the invoked tool names in first-seen order, de-duplicated. A part is
// treated as a function_call when its data carries both a "name" and an "args"
// key but no "response" key (function_response parts carry "response" instead).
func extractToolCalls(history []Message) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range history {
		for _, p := range m.Parts {
			if p.Kind != "data" || len(p.Data) == 0 {
				continue
			}
			var d map[string]json.RawMessage
			if err := json.Unmarshal(p.Data, &d); err != nil {
				continue
			}
			_, hasArgs := d["args"]
			_, hasResp := d["response"]
			if !hasArgs || hasResp {
				continue
			}
			var name string
			if raw, ok := d["name"]; ok {
				_ = json.Unmarshal(raw, &name)
			}
			if name == "" || seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}
