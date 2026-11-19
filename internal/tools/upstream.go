package tools

import (
	"errors"
	"io"
	"net/http"
)

// DefaultMaxUpstreamBytes is the ceiling on bytes the gateway will accept from
// ONE upstream MCP response. Before this bound existed the only size check
// lived in the effect ledger (1 MiB) and ran AFTER the whole response had been
// buffered, so a misbehaving or compromised connector could make the gateway
// allocate arbitrarily much before being refused. The ceiling sits above the
// inline ledger bound so ordinary results still inline (and large ones can
// still spill to Artifacts), while making memory use finite and known.
const DefaultMaxUpstreamBytes = 8 << 20

// ErrUpstreamTooLarge is returned when an upstream response exceeds the bound.
// It is a transport-level refusal: nothing is journalled as a result and
// nothing is truncated.
var ErrUpstreamTooLarge = errors.New("tools: upstream response exceeds the ingress bound")

// UpstreamLimiter caps the response body of every request made through it.
// The request direction is untouched (request bodies are bounded at the HTTP
// face by maxBody); this is the ingress direction from the provider.
type UpstreamLimiter struct {
	Base http.RoundTripper
	Max  int64
}

// RoundTrip implements http.RoundTripper.
func (l UpstreamLimiter) RoundTrip(r *http.Request) (*http.Response, error) {
	base := l.Base
	if base == nil {
		base = http.DefaultTransport
	}
	max := l.Max
	if max <= 0 {
		max = DefaultMaxUpstreamBytes
	}
	resp, err := base.RoundTrip(r)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &boundedBody{ReadCloser: resp.Body, max: max}
	return resp, nil
}

// LimitUpstream returns a client that enforces the bound on every response.
// The original client is not modified, so a caller can keep its own transport
// (proxy, TLS, egress policy) and only add the bound.
func LimitUpstream(client *http.Client, max int64) *http.Client {
	if client == nil {
		return nil
	}
	out := *client
	out.Transport = LimitUpstreamRoundTripper(client.Transport, max)
	return &out
}

// LimitUpstreamRoundTripper wraps an existing RoundTripper, or installs one
// when the client had none.
func LimitUpstreamRoundTripper(base http.RoundTripper, max int64) http.RoundTripper {
	if max <= 0 {
		max = DefaultMaxUpstreamBytes
	}
	return UpstreamLimiter{Base: base, Max: max}
}

// boundedBody fails the read as soon as the stream crosses the bound. The
// stream is then closed, so an oversized response cannot keep flowing.
type boundedBody struct {
	io.ReadCloser
	read int64
	max  int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if b.read >= b.max {
		_ = b.Close()
		return 0, ErrUpstreamTooLarge
	}
	n, err := b.ReadCloser.Read(p)
	b.read += int64(n)
	if b.read > b.max {
		_ = b.Close()
		return n, ErrUpstreamTooLarge
	}
	return n, err
}
