// Package sdk is a thin AHP Memory API client. It never chooses physical banks.
package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

type Client struct {
	BaseURL string
	Token   func(context.Context) (string, error)
	HTTP    *http.Client
}

func New(base string, token func(context.Context) (string, error)) *Client {
	return &Client{strings.TrimRight(base, "/"), token, &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("memory API: %d %s: %s", e.Status, e.Code, e.Message)
}
func (c *Client) call(ctx context.Context, method, path, key string, payload, out any) error {
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	token, err := c.Token(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
			return &Error{Status: resp.StatusCode, Code: "invalid_response", Message: "API returned an invalid error response"}
		}
		return &Error{resp.StatusCode, body.Error.Code, body.Error.Message}
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
}
func (c *Client) Remember(ctx context.Context, key string, req memory.RememberRequest) (memory.Operation, error) {
	var out memory.Operation
	err := c.call(ctx, http.MethodPost, "/v1/memory/remember", key, req, &out)
	return out, err
}
func (c *Client) BindProject(ctx context.Context, key string, req memory.BindRequest) error {
	var out memory.ProjectBinding
	return c.call(ctx, http.MethodPost, "/v1/memory/bind", key, req, &out)
}
func (c *Client) Search(ctx context.Context, req memory.SearchRequest) (memory.SearchResult, error) {
	var out memory.SearchResult
	err := c.call(ctx, http.MethodPost, "/v1/memory/search", "", req, &out)
	return out, err
}
func (c *Client) Context(ctx context.Context, req memory.ContextRequest) (memory.SearchResult, error) {
	var out memory.SearchResult
	err := c.call(ctx, http.MethodPost, "/v1/memory/context", "", req, &out)
	return out, err
}
func (c *Client) TurnCompleted(ctx context.Context, key string, req memory.TurnRequest) error {
	var out map[string]any
	return c.call(ctx, http.MethodPost, "/v1/memory/runtime/turns", key, req, &out)
}
func (c *Client) Flush(ctx context.Context, key string, req memory.FlushRequest) error {
	var out map[string]any
	return c.call(ctx, http.MethodPost, "/v1/memory/runtime/flush", key, req, &out)
}
func (c *Client) RunEvent(ctx context.Context, key string, req memory.RunRequest) error {
	var out map[string]any
	return c.call(ctx, http.MethodPost, "/v1/memory/runtime/runs", key, req, &out)
}
func (c *Client) Correct(ctx context.Context, key, ref string, req memory.CorrectRequest) (memory.Operation, error) {
	var out memory.Operation
	err := c.call(ctx, http.MethodPatch, "/v1/memory/items/"+url.PathEscape(ref), key, req, &out)
	return out, err
}
func (c *Client) Operation(ctx context.Context, id, project string, scope memory.Scope) (memory.Operation, error) {
	var out memory.Operation
	q := url.Values{"project_id": {project}, "scope": {string(scope)}}
	err := c.call(ctx, http.MethodGet, "/v1/memory/operations/"+url.PathEscape(id)+"?"+q.Encode(), "", nil, &out)
	return out, err
}
