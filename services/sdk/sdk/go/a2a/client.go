// Package a2a is a task-authorized AHP A2A 1.0 client for standalone agents.
// The machine face is /a2a/internal; it never copies a human credential.
package a2a

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
	"iter"
	"sync/atomic"
)

type Config = clienthttp.Config
type Error = clienthttp.Error
type Client struct {
	wire *clienthttp.Client
	seq  atomic.Uint64
}

func New(cfg Config) (*Client, error) {
	w, e := clienthttp.New(cfg)
	if e != nil {
		return nil, e
	}
	return &Client{wire: w}, nil
}

type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}
type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// RPCError retains the numeric A2A error and extension data without flattening it.
type RPCError struct {
	Code    int
	Message string
	Data    json.RawMessage
}

func (e *RPCError) Error() string { return fmt.Sprintf("A2A %d: %s", e.Code, e.Message) }
func (c *Client) request(method string, p any) (uint64, map[string]any) {
	id := c.seq.Add(1)
	return id, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": p}
}
func result(e envelope, id uint64, out any) error {
	if e.JSONRPC != "2.0" || e.ID != id {
		return &Error{Code: "INVALID_RESPONSE", Message: "invalid JSON-RPC envelope", OutcomeUnknown: true}
	}
	if e.Error != nil {
		return &RPCError{e.Error.Code, e.Error.Message, e.Error.Data}
	}
	if len(e.Result) == 0 || json.Unmarshal(e.Result, out) != nil {
		return &Error{Code: "INVALID_RESPONSE", Message: "invalid A2A result", OutcomeUnknown: true}
	}
	return nil
}
func (c *Client) call(ctx context.Context, method string, p, out any, mutation bool) error {
	id, req := c.request(method, p)
	var env envelope
	if e := c.wire.Call(ctx, "/a2a/internal", req, &env, mutation); e != nil {
		return e
	}
	return result(env, id, out)
}
func checkSend(r *protocol.SendMessageRequest) error {
	if r == nil || r.Message == nil {
		return contracts.NewError(contracts.ErrInvalid, "message required")
	}
	return clienthttp.Required(r.Message.ID)
}

// Send requires a stable caller-owned message ID; the JSON-RPC ID is not the effect key.
func (c *Client) Send(ctx context.Context, r *protocol.SendMessageRequest) (protocol.SendMessageResult, error) {
	if e := checkSend(r); e != nil {
		return nil, e
	}
	var out protocol.StreamResponse
	if e := c.call(ctx, "SendMessage", r, &out, true); e != nil {
		return nil, e
	}
	v, ok := out.Event.(protocol.SendMessageResult)
	if !ok {
		return nil, &Error{Code: "INVALID_RESPONSE", Message: "expected task or message"}
	}
	return v, nil
}

// Delegate links a child to the presenting parent. The owner derives grants/root/depth;
// maxSteps is the explicit travelling reserve, subject to server admission.
func (c *Client) Delegate(ctx context.Context, parent string, maxSteps int, r *protocol.SendMessageRequest) (protocol.SendMessageResult, error) {
	if e := checkSend(r); e != nil {
		return nil, e
	}
	if e := clienthttp.Required(parent); e != nil {
		return nil, e
	}
	if maxSteps <= 0 {
		return nil, contracts.NewError(contracts.ErrInvalid, "positive delegated step reserve required")
	}
	copy := *r
	copy.Metadata = make(map[string]any, len(r.Metadata)+2)
	for k, v := range r.Metadata {
		copy.Metadata[k] = v
	}
	copy.Metadata["ahp_parent_task"] = parent
	copy.Metadata["max_steps"] = maxSteps
	return c.Send(ctx, &copy)
}
func (c *Client) Get(ctx context.Context, id protocol.TaskID) (*protocol.Task, error) {
	if e := clienthttp.Required(string(id)); e != nil {
		return nil, e
	}
	var out protocol.Task
	e := c.call(ctx, "GetTask", protocol.GetTaskRequest{ID: id}, &out, false)
	return &out, e
}
func (c *Client) Cancel(ctx context.Context, id protocol.TaskID) (*protocol.Task, error) {
	if e := clienthttp.Required(string(id)); e != nil {
		return nil, e
	}
	var out protocol.Task
	e := c.call(ctx, "CancelTask", protocol.CancelTaskRequest{ID: id}, &out, true)
	return &out, e
}

// Resubscribe replays the owner's durable task snapshot/history, then live events.
// There is no cursor parameter on this API; callers must tolerate replay.
func (c *Client) Resubscribe(ctx context.Context, id protocol.TaskID) iter.Seq2[protocol.Event, error] {
	return c.stream(ctx, "SubscribeToTask", protocol.SubscribeToTaskRequest{ID: id})
}
func (c *Client) SendStream(ctx context.Context, r *protocol.SendMessageRequest) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		if e := checkSend(r); e != nil {
			yield(nil, e)
			return
		}
		c.stream(ctx, "SendStreamingMessage", r)(yield)
	}
}
func (c *Client) stream(ctx context.Context, method string, p any) iter.Seq2[protocol.Event, error] {
	return func(yield func(protocol.Event, error) bool) {
		id, req := c.request(method, p)
		data := []byte{}
		stopped := errors.New("reader stopped")
		terminal := false
		dispatch := func() error {
			if len(data) == 0 {
				return nil
			}
			var env envelope
			if json.Unmarshal(data, &env) != nil {
				return &Error{Code: "INVALID_RESPONSE", Message: "invalid SSE JSON-RPC record", OutcomeUnknown: true}
			}
			data = nil
			var out protocol.StreamResponse
			if e := result(env, id, &out); e != nil {
				return e
			}
			switch v := out.Event.(type) {
			case *protocol.Task:
				terminal = (v.Status.State.Terminal() || v.Status.State == protocol.TaskStateInputRequired)
			case *protocol.TaskStatusUpdateEvent:
				terminal = (v.Status.State.Terminal() || v.Status.State == protocol.TaskStateInputRequired)
			case *protocol.Message:
				terminal = true
			}
			if !yield(out.Event, nil) || terminal {
				return stopped
			}
			return nil
		}
		e := c.wire.Lines(ctx, "/a2a/internal", req, true, func(line []byte) error {
			if len(line) == 0 {
				return dispatch()
			}
			if bytes.HasPrefix(line, []byte("data:")) {
				part := bytes.TrimPrefix(line, []byte("data:"))
				part = bytes.TrimPrefix(part, []byte(" "))
				data = append(data, part...)
				data = append(data, '\n')
				if len(data) > clienthttp.MaxBytes {
					return &Error{Code: "INVALID_RESPONSE", Message: "SSE record exceeds 16 MiB", OutcomeUnknown: true}
				}
			}
			return nil
		})
		if errors.Is(e, stopped) {
			return
		}
		if e == nil {
			e = dispatch()
		}
		if e == nil && !terminal {
			e = &Error{Code: "UNKNOWN", Message: "A2A stream ended before interaction boundary; resubscribe task", OutcomeUnknown: true}
		}
		if e != nil && !errors.Is(e, stopped) {
			yield(nil, e)
		}
	}
}
