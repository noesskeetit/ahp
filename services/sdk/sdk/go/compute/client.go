// Package compute is the public HTTP client SDK for the AHP Compute API.
//
// Wire contract: every call is POST {base}/v1/compute/{Operation} with a
// strict JSON body. Isolation scope (tenant/deployment/subject) is derived
// server-side from the caller identity (mTLS certificate plus a bound task
// token), so no request body ever carries scope fields. mTLS is supplied by
// the http.Client the caller passes to New; this package never configures TLS
// itself.
//
// Security properties enforced here:
//   - the base URL must be a bare HTTPS origin: no path, query, fragment or
//     userinfo;
//   - the caller's http.Client is cloned and redirects are refused
//     (http.ErrUseLastResponse), so a bearer token can never be forwarded to
//     a redirect destination;
//   - responses are bounded to 16 MiB and error values never embed the raw
//     response body or a bearer token;
//   - no automatic retries: every call performs at most one HTTP request.
package compute

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

// maxResponseBytes bounds every response body this SDK reads: 16 MiB.
const maxResponseBytes = 16 << 20

// Error preserves platform status, reason and uncertain mutation outcomes.
type Error = clienthttp.Error

// apiPrefix is the fixed mount point of the Compute API facade.
const apiPrefix = "/v1/compute/"

// TokenSource yields the task token for one call. It is consulted afresh on
// every task-scoped call; an error or an empty token fails the call closed
// without any HTTP request being made.
type TokenSource func(ctx context.Context) (string, error)

// Config configures a Client.
type Config struct {
	// BaseURL must be a bare HTTPS origin: no path segments beyond "/", no
	// query, no fragment, no userinfo, no percent escapes.
	BaseURL string
	// HTTPClient is mandatory. It carries the caller-supplied mTLS material;
	// New clones it and refuses redirects on the clone.
	HTTPClient *http.Client
	// TokenSource is optional. A client built without it fails every task
	// scoped call with FORBIDDEN before any request leaves the process.
	TokenSource TokenSource
}

// Client is a stateless, concurrency-safe Compute API client. It performs at
// most one HTTP request per call and never retries.
type Client struct {
	base   string
	http   *http.Client
	token  TokenSource
	prefix string
}

// New validates cfg and builds a Client.
func New(cfg Config) (*Client, error) {
	base, err := normalizeBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.HTTPClient == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"compute sdk: Config.HTTPClient is required (it carries the mTLS material)")
	}
	hc := *cfg.HTTPClient
	if hc.Transport == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"compute sdk: Config.HTTPClient must carry a transport with the client TLS material")
	}
	if _, ok := hc.Transport.(*http.Transport); !ok {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"compute sdk: Config.HTTPClient.Transport must be an *http.Transport configured for mTLS")
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if hc.Timeout == 0 {
		hc.Timeout = 60 * time.Second
	}
	return &Client{base: base, http: &hc, token: cfg.TokenSource, prefix: apiPrefix}, nil
}

// normalizeBaseURL accepts only a bare https origin.
func normalizeBaseURL(raw string) (string, error) {
	invalid := func(reason string) error {
		return contracts.NewError(contracts.ErrInvalid, "compute sdk: BaseURL "+reason)
	}
	if raw == "" {
		return "", invalid("must not be empty")
	}
	if strings.ContainsAny(raw, "%?#") {
		return "", invalid("must not contain percent escapes, query or fragment")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", invalid("is not a valid URL")
	}
	if u.Scheme != "https" {
		return "", invalid("must use the https scheme")
	}
	if u.User != nil {
		return "", invalid("must not contain userinfo")
	}
	if u.Host == "" {
		return "", invalid("must contain a host")
	}
	if u.RawPath != "" || u.Opaque != "" {
		return "", invalid("must not contain an encoded or opaque path")
	}
	if u.Path != "" && u.Path != "/" {
		return "", invalid("must not contain extra path segments")
	}
	host := u.Hostname()
	if net.ParseIP(host) == nil && !validDNSHost(host) {
		return "", invalid("host is not a valid DNS name or IP")
	}
	return "https://" + u.Host, nil
}

// validDNSHost reports whether host is a plain ASCII DNS name.
func validDNSHost(host string) bool {
	if len(host) == 0 || len(host) > 253 || !utf8.ValidString(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-':
			default:
				return false
			}
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Transport core
// ---------------------------------------------------------------------------

// callTask posts one task-scoped operation: it attaches a freshly obtained
// bearer token and fails closed when no token source is configured.
func (c *Client) callTask(ctx context.Context, op string, in any, out any) error {
	if ctx == nil {
		return contracts.NewError(contracts.ErrInvalid, "compute sdk: context must not be nil")
	}
	if c.token == nil {
		return contracts.NewError(contracts.ErrForbidden,
			"compute sdk: this client has no token source; the call is forbidden")
	}
	tok, err := c.token(ctx)
	if err != nil {
		return tokenSourceError(err)
	}
	if strings.TrimSpace(tok) == "" {
		return contracts.NewError(contracts.ErrForbidden,
			"compute sdk: token source returned an empty token")
	}
	return c.do(ctx, op, in, out, tok)
}

func (c *Client) do(ctx context.Context, op string, in any, out any, bearer string) error {
	body, err := json.Marshal(in)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid,
			"compute sdk: request payload cannot be encoded")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+c.prefix+op, bytes.NewReader(body))
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "compute sdk: cannot build request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.ContentLength = int64(len(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return requestError(ctx, op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if readErr != nil {
		return requestError(ctx, op, readErr)
	}
	if len(raw) > maxResponseBytes {
		return &Error{Status: resp.StatusCode, Code: "INVALID_RESPONSE", Message: "response exceeds 16 MiB", OutcomeUnknown: mutationOperation(op)}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		e := statusError(resp.StatusCode, raw)
		if api, ok := e.(*Error); ok && resp.StatusCode >= 500 {
			api.OutcomeUnknown = mutationOperation(op)
		}
		return e
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &Error{Status: resp.StatusCode, Code: "INVALID_RESPONSE", Message: "response is not valid JSON", OutcomeUnknown: mutationOperation(op)}
	}
	return nil
}

// statusError turns a non-2xx response into a bounded typed API error. Only a small,
// well-formed error document is parsed; anything else yields a generic error
// that never echoes the body.
func statusError(status int, raw []byte) error {
	code, message, ok := parseErrorDocument(raw)
	if !ok {
		return &Error{Status: status, Code: contracts.ErrInvalid, Message: fmt.Sprintf("compute sdk: unexpected HTTP status %d", status)}
	}
	var reason struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(raw, &reason)
	return &Error{Status: status, Code: code, Message: sanitizeMessage(message), Reason: reason.Reason}

}

// parseErrorDocument accepts only a strict {"code":...,"message":...} object
// with a short identifier-like code.
func parseErrorDocument(raw []byte) (code, message string, ok bool) {
	var doc struct {
		Code    *string `json:"code"`
		Message *string `json:"message"`
		Reason  *string `json:"reason"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil || doc.Code == nil {
		return "", "", false
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return "", "", false
	}
	c := *doc.Code
	if c == "" || len(c) > 64 {
		return "", "", false
	}
	for i := 0; i < len(c); i++ {
		ch := c[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '_':
		default:
			return "", "", false
		}
	}
	m := ""
	if doc.Message != nil {
		m = *doc.Message
	}
	return c, m, true
}

// sanitizeMessage keeps the message printable and short.
func sanitizeMessage(m string) string {
	if len(m) > 512 {
		m = m[:512]
	}
	var b strings.Builder
	b.Grow(len(m))
	for _, r := range m {
		switch {
		case r == '\n', r == '\t':
			b.WriteRune(' ')
		case r < 0x20 || r == utf8.RuneError:
			continue
		default:
			b.WriteRune(r)
		}
	}
	return strings.TrimSpace(b.String())
}

// ctxError maps a canceled/expired context to a stable, safe error.
func ctxError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(context.DeadlineExceeded, contracts.NewError(contracts.ErrInvalid, "compute sdk: request context deadline exceeded"))
	}
	return errors.Join(context.Canceled, contracts.NewError(contracts.ErrInvalid, "compute sdk: request context canceled"))
}

// transportError reports a transport failure without echoing the URL, which
// could carry caller-supplied material.
func transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxError(ctxErr)
	}
	var certErr x509.CertificateInvalidError
	if errors.As(err, &certErr) {
		return contracts.NewError(contracts.ErrForbidden, "compute sdk: certificate validation failed")
	}
	return contracts.NewError(contracts.ErrInvalid, "compute sdk: request could not be delivered")
}

// tokenSourceError never echoes the source error text, which could carry a
// credential endpoint or secret.
func tokenSourceError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ctxError(err)
	}
	return contracts.NewError(contracts.ErrForbidden, "compute sdk: token source failed")
}

// requestError preserves uncertainty for writes; cancellation does not prove no effect.
func requestError(ctx context.Context, op string, cause error) error {
	mutation := mutationOperation(op)
	code := "UNAVAILABLE"
	if mutation {
		code = "UNKNOWN"
	}
	return &Error{Code: code, Message: "transport interrupted; reconcile the same operation", OutcomeUnknown: mutation, Cause: ctx.Err()}
}

func mutationOperation(op string) bool {
	switch op {
	case "GetComputer", "GetCommand", "WorkspaceHead", "CurrentEnvironment", "CurrentFence", "StateHead", "StateRevisions", "CostReport", "ActivityLeases", "ListEnvironment", "GetActiveVersion":
		return false
	}
	return true
}
