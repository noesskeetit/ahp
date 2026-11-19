package space

import (
	"context"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// requestIDHeader is the correlation id the Space edge shares with its caller. It exists because the
// earliest refusal this edge can make happens BEFORE anything durable: a run grant refused by Identity
// leaves no run row, so the structured record in logRunGrantRefusal is the only place its cause is ever
// written, and a reader needs one token to tie that record to the call that was refused. Root does
// exactly this reading (backend.log lines matched to a client-observed failure), and a span cannot
// carry the join - the OTel middleware is optional composition, so trace ids are absent on paths that
// still fail.
//
// The name is the conventional one so a proxy in front of the runner can pass its own id through and be
// heard; the value's safety is still decided here, not by whoever sent it.
const requestIDHeader = "X-Request-Id"

// maxAdoptedRequestIDRunes bounds an ADOPTED id. An oversized id is replaced rather than clipped: a
// clipped id belongs to nobody, and a reader following it would land on the wrong request.
const maxAdoptedRequestIDRunes = 64

type requestIDKey struct{}

// WithRequestID returns ctx carrying a correlation id for the duration of one request. An empty id is
// ignored rather than stored, so a later lookup can tell "not stated" from "stated as nothing".
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFromContext returns the correlation id of the request ctx belongs to, or "" when none was
// stated. Context values are read-only projections of one request: nothing here is serialized, persisted
// or forwarded to a dependency, so the id cannot leak into a downstream call as a credential-shaped
// string would.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// resolveRequestID states the correlation id for one inbound request: the caller's own when it is safe
// to carry, otherwise a fresh one. "Safe" is a trust decision, not a format preference - the adopted
// value is written into a structured log line and echoed back in a response header, so it must be short
// and made only of characters that cannot forge a log field, break a line, or inject a header.
func resolveRequestID(inbound string) string {
	if adoptableRequestID(inbound) {
		return inbound
	}
	return "req_" + contracts.NewID()
}

// adoptableRequestID is the allowlist behind resolveRequestID: bounded length, and letters, digits and
// the four separators the platform already uses inside ids. Anything else - empty, oversized, spaces,
// quotes, CR/LF, non-ASCII - is caller text and stays caller text.
func adoptableRequestID(v string) bool {
	if v == "" || utf8.RuneCountInString(v) > maxAdoptedRequestIDRunes {
		return false
	}
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '.', r == '_', r == ':', r == '-':
		default:
			return false
		}
	}
	return true
}
