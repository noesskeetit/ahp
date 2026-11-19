package state

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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// maxResponseBytes bounds every response body this SDK reads: 16 MiB.
const maxResponseBytes = 16 << 20

// apiPrefix is the fixed mount point of the State API facade.
const apiPrefix = "/v1/state/"

// TokenSource yields the task token for one call. It is consulted afresh on
// every task-scoped call; an error or an empty token fails the call closed
// without any HTTP request being made.
type TokenSource func(ctx context.Context) (string, error)

// Config configures a Client.
type Config struct {
	// Artifacts optionally selects a separate owner origin. By default large
	// checkpoint payloads use the State origin and its current task authority.
	Artifacts *artifacts.Client
	// BaseURL must be a bare HTTPS origin: no path segments beyond "/", no
	// query, no fragment, no userinfo, no percent escapes.
	BaseURL string
	// HTTPClient is mandatory. It carries the caller-supplied mTLS material;
	// New clones it and refuses redirects on the clone.
	HTTPClient *http.Client
	// TokenSource is optional. A client built without it can still call
	// ClaimRun (certificate-only); every task-scoped call then fails with
	// FORBIDDEN before any request leaves the process.
	TokenSource TokenSource
}

// Client is a stateless, concurrency-safe State API client. It performs at
// most one State HTTP request per call and never retries. Checkpoint payloads
// may additionally use authenticated Artifacts owner calls for spill/hydration.
type Client struct {
	artifacts *artifacts.Client
	base      string
	http      *http.Client
	token     TokenSource
	prefix    string
}

// New validates cfg and builds a Client.
func New(cfg Config) (*Client, error) {
	base, err := normalizeBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	if cfg.HTTPClient == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"state sdk: Config.HTTPClient is required (it carries the mTLS material)")
	}
	// Clone so later mutation of the caller's client cannot weaken us, and
	// so a redirect can never carry our Authorization header onward.
	hc := *cfg.HTTPClient
	if hc.Transport == nil {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"state sdk: Config.HTTPClient must carry a transport with the client TLS material")
	}
	if _, ok := hc.Transport.(*http.Transport); !ok {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"state sdk: Config.HTTPClient.Transport must be an *http.Transport configured for mTLS")
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if hc.Timeout == 0 {
		hc.Timeout = 60 * time.Second // ceiling; callers may tighten via context.
	}
	return &Client{base: base, http: &hc, token: cfg.TokenSource, prefix: apiPrefix, artifacts: cfg.Artifacts}, nil
}

// WithTokenSource returns an independent client with the same mTLS transport.
// It never mutates a client shared by concurrent runs.
func (c *Client) WithTokenSource(source TokenSource) *Client {
	copy := *c
	copy.token = source
	return &copy
}

// normalizeBaseURL accepts only a bare https origin.
func normalizeBaseURL(raw string) (string, error) {
	invalid := func(reason string) error {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: BaseURL "+reason)
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
	if p := u.Port(); p != "" {
		port, err := strconv.Atoi(p)
		if err != nil || port < 1 || port > 65535 {
			return "", invalid("port is out of range")
		}
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

// call performs exactly one POST {base}/v1/state/{op} and decodes the JSON
// response into out (out may be nil to discard the body).
func (c *Client) call(ctx context.Context, op string, in any, out any) error {
	return c.do(ctx, op, in, out, false)
}

// callTask is call for the task-scoped operations: it attaches a freshly
// obtained bearer token and fails closed when no token source is configured.
func (c *Client) callTask(ctx context.Context, op string, in any, out any) error {
	return c.do(ctx, op, in, out, true)
}

func (c *Client) do(ctx context.Context, op string, in any, out any, withToken bool) error {
	if ctx == nil {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return ctxError(err)
	}
	var bearer string
	if withToken {
		if c.token == nil {
			return contracts.NewError(contracts.ErrForbidden,
				"state sdk: this client has no token source; the call is forbidden")
		}
		tok, err := c.token(ctx)
		if err != nil {
			return tokenSourceError(err)
		}
		if strings.TrimSpace(tok) == "" {
			return contracts.NewError(contracts.ErrForbidden,
				"state sdk: token source returned an empty token")
		}
		bearer = tok
	}
	body, err := json.Marshal(in)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid,
			"state sdk: request payload cannot be encoded")
	}
	if (op == "PutCheckpoint" || op == "PutWrites" || op == "PutBatch") && len(body) > 1<<20 {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: checkpoint request exceeds 1MiB after payload spill")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+c.prefix+op, bytes.NewReader(body))
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: cannot build request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.ContentLength = int64(len(body))
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return transportError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if readErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxError(ctxErr)
		}
		return contracts.NewError(contracts.ErrInvalid,
			"state sdk: response body could not be read")
	}
	if len(raw) > maxResponseBytes {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: response exceeds 16 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return statusError(resp.StatusCode, raw)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return contracts.NewError(contracts.ErrInvalid, "state sdk: response is not valid JSON")
	}
	return nil
}

// statusError turns a non-2xx response into a *contracts.Error. Only a small,
// well-formed error document is parsed; anything else yields a generic error
// that never echoes the body.
func statusError(status int, raw []byte) error {
	code, message, reason, ok := parseErrorDocument(raw)
	if !ok {
		return contracts.NewError(contracts.ErrInvalid,
			fmt.Sprintf("state sdk: unexpected HTTP status %d", status))
	}
	switch code {
	case contracts.ErrNotFound, contracts.ErrConflict, contracts.ErrStaleEpoch,
		contracts.ErrForbidden, contracts.ErrInvalid, contracts.ErrKeyReuse,
		contracts.ErrLeaseLost:
		message = sanitizeMessage(message)
	default:
		message = "state api rejected the request"
	}
	if reason != "" {
		return contracts.NewReasoned(code, reason, message)
	}
	return &contracts.Error{Code: code, Message: message}
}

// parseErrorDocument accepts the owner's strict code/message/optional-reason
// envelope. Reason is a bounded identifier, not arbitrary server diagnostics.
func parseErrorDocument(raw []byte) (code, message, reason string, ok bool) {
	var doc struct {
		Code    *string `json:"code"`
		Message *string `json:"message"`
		Reason  *string `json:"reason"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil || doc.Code == nil {
		return "", "", "", false
	}
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return "", "", "", false
	}
	c := *doc.Code
	if c == "" || len(c) > 64 {
		return "", "", "", false
	}
	for i := 0; i < len(c); i++ {
		ch := c[i]
		switch {
		case ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '_':
		default:
			return "", "", "", false
		}
	}
	m := ""
	if doc.Message != nil {
		m = *doc.Message
	}
	r := ""
	if doc.Reason != nil {
		r = *doc.Reason
		if len(r) > 64 {
			return "", "", "", false
		}
		for i := range len(r) {
			ch := r[i]
			if !(ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_') {
				return "", "", "", false
			}
		}
	}
	return c, m, r, true
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
		return contracts.NewError(contracts.ErrInvalid, "state sdk: request context deadline exceeded")
	}
	return contracts.NewError(contracts.ErrInvalid, "state sdk: request context canceled")
}

// transportError reports a transport failure without echoing the URL, which
// could carry caller-supplied material.
func transportError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxError(ctxErr)
	}
	var certErr x509.CertificateInvalidError
	if errors.As(err, &certErr) {
		return contracts.NewError(contracts.ErrForbidden, "state sdk: certificate validation failed")
	}
	return contracts.NewError(contracts.ErrInvalid, "state sdk: request could not be delivered")
}

// tokenSourceError never echoes the source error text, which could carry a
// credential endpoint or secret.
func tokenSourceError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ctxError(err)
	}
	return contracts.NewError(contracts.ErrForbidden, "state sdk: token source failed")
}
