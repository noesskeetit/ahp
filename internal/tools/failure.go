package tools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"strings"
	"syscall"

	"go.opentelemetry.io/otel/trace"
)

// Failure classes surfaced to human-facing surfaces. They are stable identifiers
// (a UI can localise them); the Message is a bounded, sanitized sentence.
const (
	FailureUnreachable   = "tool_server_unreachable"
	FailureTimeout       = "tool_call_timed_out"
	FailureCredentials   = "credential_unavailable"
	FailureLedger        = "platform_ledger_error"
	FailureToolError     = "tool_reported_error"
	FailureMisconfigured = "tool_misconfigured"
	FailureUnknown       = "tool_call_failed"
)

// FailureKey is the ledger-payload key under which the sanitized summary is
// journalled. Only this key is ever shown to a caller.
const FailureKey = "failure"

// Failure is the human-readable, secret-free reason a tool call did not
// produce a result. The raw internal cause stays server-side (logs); the plane
// has no other way to tell a human WHAT to do next, which is the whole point of
// a tool gateway that people operate.
type Failure struct {
	Class   string `json:"class"`
	Message string `json:"message"`
	// TraceID is the trace of the request that FAILED, captured when the
	// failure is journalled. A later read (Get) must not present its own trace
	// as the trace of the failure.
	TraceID string `json:"trace_id,omitempty"`
}

var (
	urlLike   = regexp.MustCompile(`[A-Za-z][A-Za-z0-9+.\-]*://[^\s"'(),]+`)
	fileLike  = regexp.MustCompile(`/(?:[A-Za-z0-9._\-]+/)+[A-Za-z0-9._\-]+`)
	secretish = regexp.MustCompile(`(?i)\b(bearer|token|secret|password|passwd|api[-_]?key|authorization)\b[=:\s]+\S+`)
	spaceRun  = regexp.MustCompile(`\s+`)
)

// maxFailureMessage bounds what a human surface has to render.
const maxFailureMessage = 240

// ClassifyFailure turns an internal cause into a class plus a message that is
// safe to show: endpoint URLs, filesystem paths (which name secret/Vault
// locations) and credential-looking assignments are stripped, and the text is
// bounded. It never returns an empty message for a non-nil error.
func ClassifyFailure(ctx context.Context, err error) Failure {
	if err == nil {
		return Failure{}
	}
	raw := err.Error()
	class := FailureUnknown
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, syscall.ETIMEDOUT) || os.IsTimeout(err) ||
		strings.Contains(raw, "timeout") || strings.Contains(raw, "timed out"):
		class = FailureTimeout
	case strings.Contains(raw, "resolve credential"):
		class = FailureCredentials
	case strings.Contains(raw, "connection vanished"), strings.Contains(raw, "unknown tool"):
		class = FailureMisconfigured
	case strings.Contains(raw, "connect:"), strings.Contains(raw, "dial tcp"),
		strings.Contains(raw, "connection refused"), strings.Contains(raw, "no such host"),
		strings.Contains(raw, "EOF"), strings.Contains(raw, "refused"):
		class = FailureUnreachable
	case strings.Contains(raw, "SQLSTATE"), strings.Contains(raw, "ledger"),
		strings.Contains(raw, "pgx:"), strings.Contains(raw, "complete:"):
		class = FailureLedger
	}
	return Failure{Class: class, Message: sanitize(raw), TraceID: traceID(ctx)}
}

// ToolFailure builds the failure for a tool that answered with an error result
// (the call round-tripped, the tool itself refused or failed). excerpt is the
// tool's own words, which the user needs -- bounded and sanitized like any
// other message.
func ToolFailure(ctx context.Context, excerpt string) Failure {
	msg := sanitize(excerpt)
	if msg == "" {
		msg = "the tool reported an error and gave no explanation"
	}
	return Failure{Class: FailureToolError, Message: msg, TraceID: traceID(ctx)}
}

// traceID returns the current request's trace id when one is being recorded.
func traceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// sanitize removes machine-only and sensitive detail from an error string.
func sanitize(s string) string {
	s = secretish.ReplaceAllString(s, "$1=[redacted]")
	s = urlLike.ReplaceAllString(s, "<endpoint>")
	s = fileLike.ReplaceAllString(s, "<path>")
	s = strings.TrimSpace(s)
	if r := []rune(s); len(r) > maxFailureMessage {
		s = string(r[:maxFailureMessage-1]) + "…"
	}
	return spaceRun.ReplaceAllString(s, " ")
}

// FailureFromPayload reads the journalled summary out of an operation result
// payload. Absent or malformed summaries yield ok=false: the caller then shows
// the status alone rather than inventing a reason.
func FailureFromPayload(payload []byte) (Failure, bool) {
	if len(payload) == 0 {
		return Failure{}, false
	}
	var probe struct {
		Failure *Failure `json:"failure"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil || probe.Failure == nil {
		return Failure{}, false
	}
	if probe.Failure.Class == "" && probe.Failure.Message == "" {
		return Failure{}, false
	}
	if probe.Failure.Message == "" {
		probe.Failure.Message = "the tool call failed without an explanation"
	}
	return *probe.Failure, true
}
