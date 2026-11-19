package state

import (
	"context"
	"encoding/json"
)

// StreamEvent is one entry in a thread's event stream.
// Wire format matches the server's state.StreamEvent.
type StreamEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// AppendStream appends an event to the thread stream.
func (c *Client) AppendStream(ctx context.Context, f Fence, key, kind string, payload any) (StreamEvent, error) {
	if err := validateFence(f); err != nil {
		return StreamEvent{}, err
	}
	if key == "" {
		return StreamEvent{}, newInvalid("key must not be empty")
	}
	var out StreamEvent
	req := struct {
		Fence   fenceDTO `json:"fence"`
		Key     string   `json:"key"`
		Kind    string   `json:"kind"`
		Payload any      `json:"payload,omitempty"`
	}{toFenceDTO(f), key, kind, payload}
	err := c.callTask(ctx, "AppendStream", req, &out)
	return out, err
}

// ReadStream reads stream events from a thread.
func (c *Client) ReadStream(ctx context.Context, threadID string, after int64, limit int) ([]StreamEvent, error) {
	if threadID == "" {
		return nil, newInvalid("thread_id must not be empty")
	}
	var out struct {
		Events []StreamEvent `json:"events"`
	}
	req := struct {
		ThreadID string `json:"thread_id"`
		After    int64  `json:"after,omitempty"`
		Limit    int    `json:"limit,omitempty"`
	}{threadID, after, limit}
	err := c.callTask(ctx, "ReadStream", req, &out)
	return out.Events, err
}
