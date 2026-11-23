// Package clienthttp provides single-attempt HTTP transport for public clients.
package clienthttp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const MaxBytes = 16 << 20

type TokenSource func(context.Context) (string, error)
type Config struct {
	BaseURL     string
	HTTPClient  *http.Client
	TokenSource TokenSource
}

// Error preserves platform code/reason and uncertainty. Unwrap supports contracts.Code.
type Error struct {
	Status                int
	Code, Message, Reason string
	OutcomeUnknown        bool
	Cause                 error
}

func (e *Error) Error() string { return "AHP API: " + e.Code + ": " + e.Message }
func (e *Error) Unwrap() []error {
	out := []error{&contracts.Error{Code: e.Code, Message: e.Message}}
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	return out
}
func (e *Error) ReasonCode() string { return e.Reason }

type Client struct {
	base  string
	http  *http.Client
	token TokenSource
}

func New(cfg Config) (*Client, error) {
	u, e := url.Parse(cfg.BaseURL)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || strings.ContainsAny(cfg.BaseURL, "%?#") || u.Opaque != "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "a bare HTTPS owner origin is required")
	}
	if cfg.HTTPClient == nil || cfg.HTTPClient.Transport == nil || cfg.TokenSource == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "caller-owned mTLS transport and current token source are required")
	}
	h := *cfg.HTTPClient
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if h.Timeout == 0 {
		h.Timeout = 10 * time.Minute
	}
	return &Client{strings.TrimRight(cfg.BaseURL, "/"), &h, cfg.TokenSource}, nil
}

// NewLocal connects to a host injector over the supplied local transport.
// Network clients must use New, which always requires a token and HTTPS.
func NewLocal(dial func(context.Context) (net.Conn, error)) *Client {
	return &Client{base: "http://ahp-injector.local", http: &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dial(ctx)
			},
			IdleConnTimeout: 30 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       10 * time.Minute,
	}}
}

func Required(s string) error {
	if strings.TrimSpace(s) == "" {
		return contracts.NewError(contracts.ErrInvalid, "stable operation key or identifier is required")
	}
	return nil
}
func (c *Client) request(ctx context.Context, path string, input any, accept string) (*http.Response, error) {
	return c.requestMethod(ctx, http.MethodPost, path, input, accept)
}

func (c *Client) requestMethod(ctx context.Context, method, path string, input any, accept string) (*http.Response, error) {
	if ctx == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "context is required")
	}
	var token string
	if c.token != nil {
		var err error
		token, err = c.token(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, contracts.NewError(contracts.ErrForbidden, "token source failed")
		}
		if strings.TrimSpace(token) == "" {
			return nil, contracts.NewError(contracts.ErrForbidden, "current task token is required")
		}
	}
	var body io.Reader
	if input != nil || method == http.MethodPost {
		b, err := json.Marshal(input)
		if err != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "invalid request JSON")
		}
		body = bytes.NewReader(b)
	}
	r, e := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if e != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid request")
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	r.Header.Set("Accept", accept)
	return c.http.Do(r)
}
func failure(ctx context.Context, mutation bool) error {
	code := "UNAVAILABLE"
	if mutation {
		code = "UNKNOWN"
	}
	return &Error{Code: code, Message: "transport interrupted; reconcile the same operation", OutcomeUnknown: mutation, Cause: ctx.Err()}
}
func status(status int, b []byte) error {
	var v struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	if json.Unmarshal(b, &v) != nil || v.Code == "" {
		v.Code = "HTTP_ERROR"
		v.Message = "request rejected"
	}
	if len(v.Message) > 512 {
		v.Message = v.Message[:512]
	}
	return &Error{Status: status, Code: v.Code, Message: v.Message, Reason: v.Reason}
}
func (c *Client) Call(ctx context.Context, path string, input, out any, mutation bool) error {
	return c.CallMethod(ctx, http.MethodPost, path, input, out, mutation)
}

func (c *Client) CallMethod(ctx context.Context, method, path string, input, out any, mutation bool) error {
	r, e := c.requestMethod(ctx, method, path, input, "application/json")
	if e != nil {
		var ce *contracts.Error
		if errors.As(e, &ce) {
			return e
		}
		return failure(ctx, mutation)
	}
	defer r.Body.Close()
	b, e := io.ReadAll(io.LimitReader(r.Body, MaxBytes+1))
	if e != nil {
		return failure(ctx, mutation)
	}
	if len(b) > MaxBytes {
		return &Error{Status: r.StatusCode, Code: "INVALID_RESPONSE", Message: "response exceeds 16 MiB", OutcomeUnknown: mutation}
	}
	if r.StatusCode < 200 || r.StatusCode >= 300 {
		err := status(r.StatusCode, b)
		if api, ok := err.(*Error); ok && mutation && r.StatusCode >= 500 {
			api.OutcomeUnknown = true
		}
		return err
	}
	if out != nil && json.Unmarshal(b, out) != nil {
		return &Error{Status: r.StatusCode, Code: "INVALID_RESPONSE", Message: "invalid JSON", OutcomeUnknown: mutation}
	}
	return nil
}

// Lines owns the body until callback returns false or the stream ends. No reconnect.
func (c *Client) Lines(ctx context.Context, path string, input any, sse bool, yield func([]byte) error) error {
	accept := "application/x-ndjson"
	if sse {
		accept = "text/event-stream"
	}
	r, e := c.request(ctx, path, input, accept)
	if e != nil {
		var ce *contracts.Error
		if errors.As(e, &ce) {
			return e
		}
		return failure(ctx, true)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(r.Body, MaxBytes))
		err := status(r.StatusCode, b)
		if api, ok := err.(*Error); ok && r.StatusCode >= 500 {
			api.OutcomeUnknown = true
		}
		return err
	}
	if strings.Split(r.Header.Get("Content-Type"), ";")[0] != accept {
		return &Error{Code: "INVALID_RESPONSE", Message: "unexpected stream content type", OutcomeUnknown: true}
	}
	s := bufio.NewScanner(r.Body)
	s.Buffer(make([]byte, 4096), MaxBytes)
	for s.Scan() {
		if e := yield(s.Bytes()); e != nil {
			return e
		}
	}
	if s.Err() != nil {
		return failure(ctx, true)
	}
	return nil
}
