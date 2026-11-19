package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client calls the AHP Memory task API. Supply workload mTLS in the HTTP client.
// It never retries mutations: after transport failure, retry the same payload
// with the same idempotency key, then inspect Operation until it is terminal.
type Client struct {
	base  string
	token func(context.Context) (string, error)
	http  *http.Client
}

// Error preserves the platform response without exposing credentials or URLs.
type Error struct {
	Status        int
	Code, Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("memory API: %d %s: %s", e.Status, e.Code, e.Message)
}

// New constructs an agent client. Redirects are refused even on a supplied HTTP
// client, preventing a redirect from forwarding workload credentials elsewhere.
func New(base string, token func(context.Context) (string, error), client *http.Client) (*Client, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || token == nil {
		return nil, errors.New("memory: an API origin and token provider are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{strings.TrimRight(base, "/"), token, &copy}, nil
}

func (c *Client) call(ctx context.Context, method, path, key string, mutation bool, payload, out any) error {
	if mutation && strings.TrimSpace(key) == "" {
		return errors.New("memory: mutation requires an idempotency key")
	}
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	token, err := c.token(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(token) == "" {
		return errors.New("memory: empty task token")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+"/v1/memory/"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if mutation {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 4<<20 {
		return &Error{resp.StatusCode, "invalid_response", "response exceeds 4 MiB"}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct {
			Error         struct{ Code, Message string }
			Code, Message string
		}
		if json.Unmarshal(data, &body) != nil {
			return &Error{resp.StatusCode, "invalid_response", "API returned an invalid error response"}
		}
		if body.Error.Code != "" {
			return &Error{resp.StatusCode, body.Error.Code, body.Error.Message}
		}
		if body.Code != "" {
			return &Error{resp.StatusCode, body.Code, body.Message}
		}
		return &Error{resp.StatusCode, "invalid_response", "API returned an invalid error response"}
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, out) != nil {
		return &Error{resp.StatusCode, "invalid_response", "API returned invalid JSON"}
	}
	return nil
}
func query(scope Scope, project string) string {
	return url.Values{"scope": {string(scope)}, "project_id": {project}}.Encode()
}

// Remember queues a durable write. A queued response is not extraction success.
func (c *Client) Remember(ctx context.Context, key string, request RememberRequest) (out Operation, err error) {
	err = c.call(ctx, "POST", "remember", key, true, request, &out)
	return
}

// Search recalls authorized personal/project facts.
func (c *Client) Search(ctx context.Context, request SearchRequest) (out SearchResult, err error) {
	err = c.call(ctx, "POST", "search", "", false, request, &out)
	return
}

// Context selects project facts and optionally personal preferences.
func (c *Client) Context(ctx context.Context, request ContextRequest) (out SearchResult, err error) {
	err = c.call(ctx, "POST", "context", "", false, request, &out)
	return
}

// List returns a bounded page from a logical scope.
func (c *Client) List(ctx context.Context, scope Scope, project string, limit, offset int) (out Page, err error) {
	err = c.call(ctx, "GET", "items?"+query(scope, project)+"&limit="+strconv.Itoa(limit)+"&offset="+strconv.Itoa(offset), "", false, nil, &out)
	return
}

// Correct queues a correction to a fact reference returned by the server.
func (c *Client) Correct(ctx context.Context, key, ref string, request CorrectRequest) (out Operation, err error) {
	err = c.call(ctx, "PATCH", "items/"+url.PathEscape(ref), key, true, request, &out)
	return
}

// Invalidate queues removal of a fact from future retrieval.
func (c *Client) Invalidate(ctx context.Context, key, ref string, request CorrectRequest) (out Operation, err error) {
	err = c.call(ctx, "POST", "items/"+url.PathEscape(ref)+"/invalidate", key, true, request, &out)
	return
}

// Operation reads a durable write receipt within the verified owner's scope.
func (c *Client) Operation(ctx context.Context, id string, scope Scope, project string) (out Operation, err error) {
	err = c.call(ctx, "GET", "operations/"+url.PathEscape(id)+"?"+query(scope, project), "", false, nil, &out)
	return
}

// Settings reads current flags without extending project activity.
func (c *Client) Settings(ctx context.Context, project string) (out Settings, err error) {
	err = c.call(ctx, "GET", "settings?"+url.Values{"project_id": {project}}.Encode(), "", false, nil, &out)
	return
}

// Status reads queue and bank lifecycle status, without native bank IDs.
func (c *Client) Status(ctx context.Context, scope Scope, project string) (out Status, err error) {
	err = c.call(ctx, "GET", "status?"+query(scope, project), "", false, nil, &out)
	return
}

// ManagementClient is for a sanctioned ingress/control application. Constructing
// it grants no permission: the server requires authority plus review/delete caps.
type ManagementClient struct{ *Client }

// Management exposes management operations on the same verified transport.
func (c *Client) Management() *ManagementClient { return &ManagementClient{c} }

// UpdateSettings requires ingress/control authority and memory:review.
func (c *ManagementClient) UpdateSettings(ctx context.Context, key string, request SettingsRequest) (out Settings, err error) {
	err = c.call(ctx, "PATCH", "settings", key, true, request, &out)
	return
}

// Clear requires ingress/control authority and memory:delete.
func (c *ManagementClient) Clear(ctx context.Context, key string, request ClearRequest) (out ClearResult, err error) {
	err = c.call(ctx, "POST", "clear", key, true, request, &out)
	return
}

// Retry requires ingress/control authority and memory:review.
func (c *ManagementClient) Retry(ctx context.Context, key, id string, request ClearRequest) (out Operation, err error) {
	err = c.call(ctx, "POST", "operations/"+url.PathEscape(id)+"/retry", key, true, request, &out)
	return
}
