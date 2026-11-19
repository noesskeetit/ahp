package state

import (
	"context"
	"encoding/json"
)

// InboxEvent is one incoming event delivered to a thread.
type InboxEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

// Enqueue places an event in the thread inbox. key is the deduplication key.
func (c *Client) Enqueue(ctx context.Context, threadID, key, kind string, payload json.RawMessage) (InboxEvent, error) {
	if threadID == "" {
		return InboxEvent{}, newInvalid("thread_id must not be empty")
	}
	if key == "" {
		return InboxEvent{}, newInvalid("key must not be empty")
	}
	var out InboxEvent
	req := struct {
		ThreadID string          `json:"thread_id"`
		Key      string          `json:"key"`
		Kind     string          `json:"kind"`
		Payload  json.RawMessage `json:"payload,omitempty"`
	}{threadID, key, kind, payload}
	if err := c.callTask(ctx, "Enqueue", req, &out); err != nil {
		return InboxEvent{}, err
	}
	return out, nil
}

// ReadInbox returns unacknowledged inbox events for a thread after the given
// sequence. limit caps the returned count (default server-side).
func (c *Client) ReadInbox(ctx context.Context, threadID string, after int64, limit int) ([]InboxEvent, error) {
	if threadID == "" {
		return nil, newInvalid("thread_id must not be empty")
	}
	out := struct {
		Events []InboxEvent `json:"events"`
	}{}
	req := struct {
		ThreadID string `json:"thread_id"`
		After    int64  `json:"after,omitempty"`
		Limit    int    `json:"limit,omitempty"`
	}{threadID, after, limit}
	if err := c.callTask(ctx, "ReadInbox", req, &out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

// AckInbox acknowledges inbox events under the current lease.
func (c *Client) AckInbox(ctx context.Context, f Fence, ids []string) error {
	if err := validateFence(f); err != nil {
		return err
	}
	if len(ids) == 0 {
		return newInvalid("ids must not be empty")
	}
	var out okResponse
	req := struct {
		Fence fenceDTO `json:"fence"`
		IDs   []string `json:"ids"`
	}{toFenceDTO(f), ids}
	return c.callTask(ctx, "AckInbox", req, &out)
}
