package otel

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// HTTPMiddleware creates a tracing middleware for net/http handlers.
func HTTPMiddleware(serviceName string) func(http.Handler) http.Handler {
	tracer := otel.Tracer("ahp/http")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(),
				propagation.HeaderCarrier(r.Header))
			spanName := r.Method + " " + cleanPath(r.URL.Path)
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					attribute.String("http.method", r.Method),
					attribute.String("http.target", r.URL.Path),
					attribute.String("service", serviceName),
					attribute.String("ahp.component", boundedComponent(serviceName, r.URL.Path)),
					attribute.String("ahp.operation", boundedOperation(r.URL.Path)),
				),
			)
			defer span.End()

			rw := &statusRecorder{ResponseWriter: w, status: 200}
			start := time.Now()
			next.ServeHTTP(rw, r.WithContext(ctx))
			elapsed := time.Since(start)

			span.SetAttributes(
				attribute.Int("http.status_code", rw.status),
				attribute.Int64("http.duration_ms", elapsed.Milliseconds()),
			)
			if rw.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(rw.status))
			}
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush passes the connection's Flusher through to the handler.
//
// Embedding http.ResponseWriter advertises Header/Write/WriteHeader only, so a
// handler that asks for a Flusher gets "no" from behind this recorder. That is
// not cosmetic: a live turn stream (internal/space/sse.go asserts
// w.(http.Flusher)) and the single-origin reverse proxy (whose flush goes
// through http.NewResponseController) both need it. Without it every streamed
// answer served through this middleware sits in the server's buffer until the
// handler returns, which reaches the browser as an origin that never answers.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.NewResponseController reach the capabilities this type does
// not name (Hijacker, ReadFrom, write deadlines) on the writer underneath, so
// recording a status code removes nothing from the handler's view of the
// connection.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Hijack passes protocol upgrades through for the same reason as Flush: a
// handler that asserts w.(http.Hijacker) directly, rather than going through
// http.NewResponseController, must not be told the connection cannot be
// hijacked just because something in front of it records a status code.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("otel: the wrapped ResponseWriter is not a http.Hijacker")
	}
	return h.Hijack()
}

// cleanPath normalises path for span names without high-cardinality values.
func cleanPath(p string) string {
	parts := strings.SplitN(p, "/", 5)
	if len(parts) <= 3 {
		return p
	}
	return strings.Join(parts[:3], "/") + "/*"
}

// Only fixed protocol operations reach the collector allowlist; arbitrary path
// segments, resource IDs, query strings and request content do not become attrs.
func boundedComponent(service, path string) string {
	// artifacts is here because the artifact write is part of the turn the operator
	// has to follow: without it the leg that produced a user-visible file shows up
	// as the role that happens to serve it (data/all) instead of the plane it is.
	for _, name := range []string{"state", "model", "tools", "space", "identity", "gateway", "artifacts"} {
		if strings.HasPrefix(path, "/v1/"+name+"/") {
			return name
		}
	}
	return service
}
func boundedOperation(path string) string {
	last := path[strings.LastIndex(path, "/")+1:]
	switch last {
	case "CreateThread", "CreateRun", "ClaimRun", "AppendStream", "ReadStream", "GetRun", "ListRuns", "PutCheckpoint", "GetCheckpoint", "Heartbeat", "Release", "SubmitModel", "StreamModel", "GetModel", "CancelModel", "AcceptTurn", "DispatchTurn", "CancelTurn", "GetTurn", "CallTool", "ExecuteTool":
		return last
	}
	return "request"
}
