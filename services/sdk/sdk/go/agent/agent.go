// Package agent provides the Go SDK for building Agents Space deployments.
// It wraps the Space API client with a high-level interface for accepting
// turns, streaming results, HITL interaction, and agent-to-agent delegation.
package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client is the Agents Space SDK client for a single deployment.
type Client struct {
	baseURL string
	token   string
	client  *http.Client
}

// Config configures the SDK client.
type Config struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// NewClient creates an SDK client.
func NewClient(cfg Config) *Client {
	hc := cfg.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	return &Client{baseURL: cfg.BaseURL, token: cfg.Token, client: hc}
}

// TurnInput describes a turn submission.
type TurnInput struct {
	Key       string          `json:"key"`
	ContextID string          `json:"context_id"`
	Input     json.RawMessage `json:"input"`
	MaxSteps  int             `json:"max_steps,omitempty"`
}

// TurnOutput is the response after accepting a turn.
type TurnOutput struct {
	TurnID   string `json:"turn_id"`
	ThreadID string `json:"thread_id"`
	Status   string `json:"status"`
}

// AcceptTurn submits a new turn to the Space.
func (c *Client) AcceptTurn(ctx context.Context, input TurnInput) (*TurnOutput, error) {
	var out TurnOutput
	if err := c.call(ctx, "AcceptTurn", input, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Dispatch requests the Space to claim and start execution.
func (c *Client) Dispatch(ctx context.Context) error {
	return c.call(ctx, "DispatchTurn", map[string]bool{}, nil)
}

// CancelTurn cancels an active turn.
func (c *Client) CancelTurn(ctx context.Context, turnID string) error {
	return c.call(ctx, "CancelTurn", map[string]string{"turn_id": turnID}, nil)
}

// Steer sends a steering instruction.
func (c *Client) Steer(ctx context.Context, turnID, instruction string) error {
	return c.call(ctx, "Steer", map[string]string{
		"turn_id":     turnID,
		"instruction": instruction,
	}, nil)
}

// HITLRequest sends a HITL gate request.
func (c *Client) HITLRequest(ctx context.Context, turnID, kind string, prompt json.RawMessage, deadline *time.Time, runID string) error {
	body := map[string]any{
		"turn_id": turnID,
		"kind":    kind,
		"prompt":  prompt,
		"run_id":  runID,
	}
	if deadline != nil {
		body["deadline"] = deadline
	}
	return c.call(ctx, "SubmitHITL", body, nil)
}

// HITLRespond approves or rejects a HITL gate.
func (c *Client) HITLRespond(ctx context.Context, hitlID string, approved bool, threadID string, response json.RawMessage) error {
	return c.call(ctx, "RespondHITL", map[string]any{
		"hitl_id":   hitlID,
		"approved":  approved,
		"thread_id": threadID,
		"response":  response,
	}, nil)
}

// CheckpointSave persists turn state.
func (c *Client) CheckpointSave(ctx context.Context, turnID string, seq int, state, messages json.RawMessage) error {
	return c.call(ctx, "SaveCheckpoint", map[string]any{
		"turn_id":  turnID,
		"sequence": seq,
		"state":    state,
		"messages": messages,
	}, nil)
}

// OutboxEmit emits a delivery item.
func (c *Client) OutboxEmit(ctx context.Context, turnID, key, kind string, payload json.RawMessage, threadID string) error {
	return c.call(ctx, "EmitOutbox", map[string]any{
		"turn_id":   turnID,
		"key":       key,
		"kind":      kind,
		"payload":   payload,
		"thread_id": threadID,
	}, nil)
}

// GetTurn retrieves turn state.
func (c *Client) GetTurn(ctx context.Context, turnID string) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.call(ctx, "GetTurn", map[string]string{"turn_id": turnID}, &out)
	return out, err
}

// ListTurns retrieves all tracked turns.
func (c *Client) ListTurns(ctx context.Context) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.call(ctx, "ListTurns", map[string]bool{}, &out)
	return out, err
}

// ReadStream retrieves stream events for a thread.
func (c *Client) ReadStream(ctx context.Context, threadID string, after int64, limit int) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.call(ctx, "ReadStream", map[string]any{
		"thread_id": threadID,
		"after":     after,
		"limit":     limit,
	}, &out)
	return out, err
}

// Recover triggers recovery of stale turns.
func (c *Client) Recover(ctx context.Context) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.call(ctx, "Recover", map[string]bool{}, &out)
	return out, err
}

// Delegate sends a turn to another deployment (agent-to-agent).
func (c *Client) Delegate(ctx context.Context, target *Client, input TurnInput) (*TurnOutput, error) {
	return target.AcceptTurn(ctx, input)
}

func (c *Client) call(ctx context.Context, op string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("agent sdk marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/space/"+op, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("agent sdk %s: %w", op, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("agent sdk %s read: %w", op, err)
	}
	if resp.StatusCode >= 400 {
		var errBody struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(respBody, &errBody)
		if errBody.Code != "" {
			return fmt.Errorf("agent sdk %s: %s: %s", op, errBody.Code, errBody.Message)
		}
		return fmt.Errorf("agent sdk %s: HTTP %d: %s", op, resp.StatusCode, string(respBody))
	}
	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}
