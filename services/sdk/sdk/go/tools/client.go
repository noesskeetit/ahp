// Package tools calls the AHP Tool Gateway, never a provider or credential store.
package tools

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
	"time"
)

type Config = clienthttp.Config
type Error = clienthttp.Error
type Failure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	TraceID string `json:"trace_id"`
}

// Operation is a journal receipt. The current owner deliberately omits raw tool output.
type Operation struct {
	Key       string    `json:"key"`
	Kind      string    `json:"kind"`
	Target    string    `json:"target"`
	Digest    string    `json:"digest"`
	Status    string    `json:"status"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Failure   *Failure  `json:"failure,omitempty"`
}
type Connection struct {
	Connection string `json:"connection"`
	State      string `json:"state"`
}
type Client struct{ wire *clienthttp.Client }

func New(cfg Config) (*Client, error) {
	w, e := clienthttp.New(cfg)
	if e != nil {
		return nil, e
	}
	return &Client{w}, nil
}

// Call preserves SIDE_EFFECT_UNKNOWN as a receipt, never retries an uncertain effect.
func (c *Client) Call(ctx context.Context, key, tool string, arguments json.RawMessage) (Operation, error) {
	var out Operation
	if e := clienthttp.Required(key); e != nil {
		return out, e
	}
	if e := clienthttp.Required(tool); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/tools/Call", map[string]any{"key": key, "tool": tool, "arguments": arguments}, &out, true)
	return out, e
}
func (c *Client) Get(ctx context.Context, key string) (Operation, error) {
	return c.operation(ctx, "Get", key, false)
}
func (c *Client) Fence(ctx context.Context, key string) (Operation, error) {
	return c.operation(ctx, "Fence", key, true)
}
func (c *Client) operation(ctx context.Context, op, key string, mutation bool) (Operation, error) {
	var out Operation
	if e := clienthttp.Required(key); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/tools/"+op, map[string]string{"key": key}, &out, mutation)
	return out, e
}
func (c *Client) ConnectionState(ctx context.Context) ([]Connection, error) {
	var out struct {
		Connections []Connection `json:"connections"`
	}
	e := c.wire.Call(ctx, "/v1/tools/ConnectionState", struct{}{}, &out, false)
	return out.Connections, e
}

// ApprovalClient requires an independently supplied human token source. Construction
// grants no authority; task tokens are refused by the owner even with tools capability.
type ApprovalClient struct{ wire *clienthttp.Client }

func NewApprovalClient(cfg Config) (*ApprovalClient, error) {
	w, e := clienthttp.New(cfg)
	if e != nil {
		return nil, e
	}
	return &ApprovalClient{w}, nil
}

type Approval struct {
	RequestID string `json:"request_id"`
	Digest    string `json:"digest"`
	Tool      string `json:"tool"`
}
type Approved struct {
	Approved   bool   `json:"approved"`
	Tool       string `json:"tool"`
	Digest     string `json:"digest"`
	ApprovedBy string `json:"approved_by"`
}

func (c *ApprovalClient) Approve(ctx context.Context, r Approval) (Approved, error) {
	var out Approved
	if e := clienthttp.Required(r.RequestID); e != nil {
		return out, e
	}
	if e := clienthttp.Required(r.Digest); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/tools/Approve", r, &out, true)
	return out, e
}
func (c *ApprovalClient) Approvals(ctx context.Context, limit int) ([]json.RawMessage, error) {
	var out struct {
		Approvals []json.RawMessage `json:"approvals"`
	}
	e := c.wire.Call(ctx, "/v1/tools/Approvals", map[string]int{"limit": limit}, &out, false)
	return out.Approvals, e
}
